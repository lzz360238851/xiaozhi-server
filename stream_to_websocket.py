#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import json
import sys
from typing import Dict, Optional
from dataclasses import dataclass

# ✅ 正确导入 WebSocketCommonProtocol（适用于 websockets >= 12）

import websockets  # 用于捕获 ConnectionClosed 等异常

import opuslib_next
from pydub.utils import which
from websockets.legacy.protocol import WebSocketCommonProtocol

# ==================== 配置日志 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==================== 全局常量 ====================
STREAM_URL = "http://ngcdn001.cnr.cn/live/zgzs/index.m3u8"
HOST = "127.0.0.1"
PORT = 8765

# 音频参数
SAMPLE_RATE = 16000
CHANNELS = 1
FRAME_DURATION_MS = 60
FRAME_SIZE_SAMPLES = SAMPLE_RATE * FRAME_DURATION_MS // 1000
FRAME_SIZE_BYTES = FRAME_SIZE_SAMPLES * 2  # 16-bit = 2 bytes

# ==================== Opus 编码器（单例）====================
class OpusEncoder:
    _instance: Optional['OpusEncoder'] = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            raise RuntimeError("OpusEncoder must be initialized with create()")
        return cls._instance

    @classmethod
    async def create(cls):
        async with cls._lock:
            if cls._instance is None:
                encoder = super().__new__(cls)
                encoder._encoder = opuslib_next.Encoder(
                    SAMPLE_RATE, CHANNELS, opuslib_next.APPLICATION_AUDIO
                )
                encoder._encoder.bitrate = 32000  # 提高比特率以改善音质
                encoder._encoder.complexity = 8   # 平衡质量和性能
                cls._instance = encoder
        return cls._instance

    def encode_frame(self, pcm_data: bytes) -> bytes:
        try:
            if len(pcm_data) != FRAME_SIZE_BYTES:
                # 填充或截断
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

    def __post_init__(self):
        self.stop_event = asyncio.Event()
        self.stop_event.set()  # 初始为停止状态

    def is_playing(self) -> bool:
        return not self.stop_event.is_set()

    def start_playing(self):
        self.stop_event.clear()

    def stop_playing(self):
        self.stop_event.set()


# 全局客户端管理（线程安全）
class ClientManager:
    def __init__(self):
        self._clients: Dict[WebSocketCommonProtocol, ClientSession] = {}
        self._lock = asyncio.Lock()

    async def add(self, websocket: WebSocketCommonProtocol):
        async with self._lock:
            if websocket not in self._clients:
                self._clients[websocket] = ClientSession(websocket)
                logger.info(f"客户端加入: {websocket.remote_address}")

    async def remove(self, websocket: WebSocketCommonProtocol):
        async with self._lock:
            session = self._clients.pop(websocket, None)
            if session and session.task:
                session.stop_playing()
                try:
                    await session.task
                except asyncio.CancelledError:
                    pass
            logger.info(f"客户端移除: {websocket.remote_address}")

    async def get_session(self, websocket: WebSocketCommonProtocol) -> Optional[ClientSession]:
        async with self._lock:
            return self._clients.get(websocket)

    async def all_sessions(self):
        async with self._lock:
            return list(self._clients.values())


client_manager = ClientManager()


# ==================== 音频流提取 ====================
async def extract_audio_from_stream(stream_url: str):
    """从流中提取PCM音频，异步生成器"""
    command = [
        'ffmpeg',
        '-i', stream_url,
        '-f', 's16le',
        '-ar', str(SAMPLE_RATE),
        '-ac', str(CHANNELS),
        '-nostdin',
        '-v', 'warning',
        'pipe:1'
    ]

    logger.info(f"启动FFmpeg从 {stream_url}")
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
        logger.info("音频提取任务被取消")
        raise
    except Exception as e:
        logger.error(f"提取音频出错: {e}")
    finally:
        logger.info("正在终止FFmpeg进程...")
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("FFmpeg未响应终止，强制杀死")
            process.kill()
            await process.wait()
        except Exception as e:
            logger.error(f"终止FFmpeg失败: {e}")


# ==================== 音频流任务 ====================
import time  # 在文件头部添加

# 在 stream_audio_task 函数中修改时间控制逻辑：
async def stream_audio_task(session: ClientSession):
    """为单个客户端流式传输音频"""
    try:
        encoder = await OpusEncoder.create()
        start_time = time.time()  # ✅ 改用 time.time()
        frame_count = 0

        async for pcm_frame in extract_audio_from_stream(STREAM_URL):
            if session.stop_event.is_set():
                break

            opus_frame = encoder.encode_frame(pcm_frame)
            if opus_frame:
                try:
                    await session.websocket.send(opus_frame)
                except websockets.exceptions.ConnectionClosed:
                    session.stop_playing()
                    break

            # ✅ 使用 time.time() 控制播放速率
            frame_count += 1
            expected_time = start_time + frame_count * (FRAME_DURATION_MS / 1000.0)
            now = time.time()
            sleep_time = expected_time - now

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                # 可选：记录是否出现负延迟（说明处理太慢）
                logger.debug(f"音频帧延迟: {abs(sleep_time)*1000:.2f}ms")

    except asyncio.CancelledError:
        logger.info("音频流任务被取消")
        raise
    except Exception as e:
        logger.error(f"向客户端发送音频失败: {e}")
        session.stop_playing()
    finally:
        logger.info(f"音频流任务结束")


# ==================== WebSocket 处理 ====================
async def handle_client(websocket: WebSocketCommonProtocol):
    """处理单个客户端连接"""
    await client_manager.add(websocket)
    session = await client_manager.get_session(websocket)

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                cmd = data.get("text", "").strip().lower()

                if cmd in ("播放电台", "play radio"):
                    if not session.is_playing():
                        session.start_playing()
                        task = asyncio.create_task(stream_audio_task(session))
                        session.task = task

                        def done_callback(t):
                            try:
                                t.result()
                            except asyncio.CancelledError:
                                pass
                            except Exception as e:
                                logger.error(f"音频任务异常: {e}")
                        task.add_done_callback(done_callback)

                        await websocket.send(json.dumps({
                            "type": "system",
                            "message": "已开始播放电台",
                            "action": "start"
                        }))
                        logger.info(f"客户端开始播放: {websocket.remote_address}")

                elif cmd in ("停止", "stop"):
                    if session.is_playing():
                        session.stop_playing()
                        if session.task:
                            session.task.cancel()
                            try:
                                await session.task
                            except asyncio.CancelledError:
                                pass
                            session.task = None
                        await websocket.send(json.dumps({
                            "type": "system",
                            "message": "已停止播放",
                            "action": "stop"
                        }))
                        logger.info(f"客户端停止播放: {websocket.remote_address}")

                else:
                    reply = {"type": "chat", "response": f"你说: {cmd}"}
                    await websocket.send(json.dumps(reply))

            except json.JSONDecodeError:
                await websocket.send(json.dumps({"error": "Invalid JSON"}))

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"客户端断开连接: {websocket.remote_address}")
    except Exception as e:
        logger.error(f"处理客户端出错: {e}")
    finally:
        await client_manager.remove(websocket)


# ==================== 主函数 ====================
async def main():
    """主入口"""
    logger.info("启动流媒体WebSocket服务...")

    if not which("ffmpeg"):
        logger.error("FFmpeg未安装或不在PATH中，请安装FFmpeg")
        return

    try:
        # 初始化Opus编码器
        await OpusEncoder.create()
        logger.info("Opus编码器初始化完成")

        # 启动WebSocket服务器
        server = await websockets.serve(
            handle_client,
            HOST,
            PORT,
            ping_interval=20,
            ping_timeout=30
        )
        logger.info(f"WebSocket服务器启动: ws://{HOST}:{PORT}")

        await server.wait_closed()

    except KeyboardInterrupt:
        logger.info("服务被用户中断")
    except Exception as e:
        logger.error(f"服务启动失败: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())