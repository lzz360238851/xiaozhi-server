import asyncio
import logging
import json
import sys
import time
from typing import Dict, Optional
from dataclasses import dataclass

import websockets
import opuslib_next
from websockets.legacy.protocol import WebSocketCommonProtocol

# ==================== 日志 ====================
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# ==================== 常量 ====================
radio_url_map = {
    "中国之声": "http://ngcdn001.cnr.cn/live/zgzs/index.m3u8",
    "经济之声": "http://ngcdn001.cnr.cn/live/jjzs/index.m3u8",
    "音乐之声": "http://ngcdn001.cnr.cn/live/yyzs/index.m3u8",
    "都市之声": "http://ngcdn001.cnr.cn/live/dszs/index.m3u8",
    "中华之声": "http://ngcdn001.cnr.cn/live/zhzs/index.m3u8",
    "神州之声": "http://ngcdn001.cnr.cn/live/szzs/index.m3u8"
    # "环球资讯广播": "http://ngcdn001.cnr.cn/live/hqzx/index.m3u8",
    # "HIT FM": "http://stream.hitfm.cn/hitfm887/playlist.m3u8"
}
SAMPLE_RATE = 16000
CHANNELS = 1
FRAME_DURATION_MS = 60
FRAME_SIZE_SAMPLES = SAMPLE_RATE * FRAME_DURATION_MS // 1000
FRAME_SIZE_BYTES = FRAME_SIZE_SAMPLES * 2


# ==================== Opus 编码器 ====================
class OpusEncoder:
    def __init__(self):
        self._encoder = opuslib_next.Encoder(SAMPLE_RATE, CHANNELS, opuslib_next.APPLICATION_AUDIO)
        self._encoder.bitrate = 32000  # 提高比特率以改善音质
        self._encoder.complexity = 8   # 平衡质量和性能

    def encode_frame(self, pcm_data: bytes) -> bytes:
        try:
            if len(pcm_data) != FRAME_SIZE_BYTES:
                pcm_data = (pcm_data + b'\x00' * FRAME_SIZE_BYTES)[:FRAME_SIZE_BYTES]
            return self._encoder.encode(pcm_data, FRAME_SIZE_SAMPLES)
        except Exception as e:
            logger.error(f"Opus编码失败: {e}")
            return b''


# ==================== 客户端会话管理 ====================
@dataclass
class ClientSession:
    websocket: WebSocketCommonProtocol
    task: Optional[asyncio.Task] = None
    stop_event: asyncio.Event = None
    encoder: Optional['OpusEncoder'] = None

    def __post_init__(self):
        # 使用当前正在运行的事件循环来创建Event
        self.stop_event = asyncio.Event()
        self.stop_event.set()
        # 每个 session 拥有独立的编码器实例，避免并发竞争
        self.encoder = OpusEncoder()

    def is_playing(self) -> bool:
        return not self.stop_event.is_set()

    def start_playing(self): self.stop_event.clear()

    def stop_playing(self): self.stop_event.set()


class ClientManager:
    def __init__(self):
        self._clients: Dict[WebSocketCommonProtocol, ClientSession] = {}
        self._lock = asyncio.Lock()

    async def add(self, websocket):
        async with self._lock:
            if websocket not in self._clients:
                # 确保ClientSession在其websocket所属的事件循环中创建
                self._clients[websocket] = ClientSession(websocket)
                logger.info(f"客户端加入: {websocket.remote_address}")

    async def remove(self, websocket):
        async with self._lock:
            session = self._clients.pop(websocket, None)
            if session and session.task:
                session.stop_playing()
                session.task.cancel()
                try:
                    await session.task
                except asyncio.CancelledError:
                    pass
            logger.info(f"客户端移除: {websocket.remote_address}")

    async def get_session(self, websocket) -> Optional[ClientSession]:
        async with self._lock:
            return self._clients.get(websocket)

    async def stop_all(self):
        async with self._lock:
            for ws, session in list(self._clients.items()):
                if session and session.is_playing():
                    session.stop_playing()
                    if session.task:
                        session.task.cancel()
                        try:
                            await session.task
                        except asyncio.CancelledError:
                            pass


client_manager = ClientManager()


# ==================== 音频提取 ====================
async def extract_audio_from_stream(stream_url: str):
    command = [
        'ffmpeg', '-i', stream_url,
        '-f', 's16le', '-ar', str(SAMPLE_RATE), '-ac', str(CHANNELS),
        '-nostdin', '-v', 'warning', 'pipe:1'
    ]
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    try:
        while True:
            data = await process.stdout.read(FRAME_SIZE_BYTES)
            if not data:
                break
            yield data
    except asyncio.CancelledError:
        raise
    finally:
        # 安全关闭子进程和管道
        try:
            if process.stdout and not process.stdout.at_eof():
                process.stdout.feed_eof()
            if process.stderr and not process.stderr.at_eof():
                process.stderr.feed_eof()
        except Exception:
            pass

        try:
            if process and process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 3.0)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
        except Exception:
            pass

        # 明确释放引用，减少 __del__ 触发时的访问
        process.stdout = None
        process.stderr = None
        process = None


# ==================== 重连封装 ====================
RECONNECT_INITIAL_DELAY = 1.0
RECONNECT_MAX_DELAY = 10.0


async def iter_stream_with_reconnect(stream_url: str, stop_event: asyncio.Event):
    delay = RECONNECT_INITIAL_DELAY
    while not stop_event.is_set():
        try:
            logger.info("尝试连接电台流")
            async for frame in extract_audio_from_stream(stream_url):
                # 一旦有数据正常返回，重置退避间隔
                delay = RECONNECT_INITIAL_DELAY
                if stop_event.is_set():
                    break
                yield frame

            if stop_event.is_set():
                break

            logger.warning("电台流结束或中断，准备重连")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"电台流读取异常: {e}. 将在 {delay:.1f}s 后重连")

        # 退避等待
        await asyncio.sleep(delay)
        delay = min(RECONNECT_MAX_DELAY, delay * 2)


# ==================== 音频流任务 ====================
async def stream_audio_task(session: ClientSession,radio_name):
    if radio_name is None or radio_name.strip() == "":
        radio_name="中国之声"
    if radio_name not in radio_url_map:
        radio_name="中国之声"
    url=radio_url_map[radio_name]
    try:
        encoder = session.encoder
        start_time = time.time()
        frame_count = 0

        async for pcm_frame in iter_stream_with_reconnect(url, session.stop_event):
            if session.stop_event.is_set():
                break

            opus_frame = encoder.encode_frame(pcm_frame)
            if opus_frame:
                try:
                    await session.websocket.send(opus_frame)
                except websockets.exceptions.ConnectionClosed:
                    session.stop_playing()
                    break

            frame_count += 1
            expected_time = start_time + frame_count * (FRAME_DURATION_MS / 1000.0)
            now = time.time()
            sleep_time = max(0, expected_time - now)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("音频流任务被取消")
        raise
    except Exception as e:
        logger.error(f"发送音频失败: {e}")
        session.stop_playing()
    finally:
        logger.info("音频流任务结束")


# ==================== 外部可调用函数 ====================
async def start_radio_stream(websocket: WebSocketCommonProtocol,radio_name):

    session = await client_manager.get_session(websocket)
    if not session:
        await client_manager.add(websocket)
        session = await client_manager.get_session(websocket)

    if session.is_playing():
        return

    # 启动前确保没有其他会话在播放（单连接单电台）
    await client_manager.stop_all()

    session.start_playing()

    # ❗ 关键修改: 直接使用 asyncio.create_task 在当前事件循环中创建任务
    #    不再使用 done_callback，将任务管理逻辑集中在 stop 函数中
    session.task = asyncio.create_task(stream_audio_task(session,radio_name))

    try:
        await websocket.send(json.dumps({
            "type": "system",
            "message": "已开始播放电台",
            "action": "start"
        }))
    except websockets.exceptions.ConnectionClosed:
        session.stop_playing()


async def stop_radio_stream(websocket: WebSocketCommonProtocol):
    session = await client_manager.get_session(websocket)
    if not session or not session.is_playing():
        return

    session.stop_playing()
    if session.task:
        # ❗ 关键修改: 显式地取消任务，并等待其完成
        session.task.cancel()
        try:
            await session.task
        except asyncio.CancelledError:
            pass
        session.task = None

    try:
        await websocket.send(json.dumps({
            "type": "system",
            "message": "已停止播放",
            "action": "stop"
        }))
    except websockets.exceptions.ConnectionClosed:
        pass