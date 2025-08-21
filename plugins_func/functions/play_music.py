import logging
import shutil
import subprocess
import sys
import tempfile
import threading
from asyncio import Queue
from pickle import GLOBAL

import select

from config.logger import setup_logging
import os
import re
import time
import random
import asyncio
import difflib
import traceback
import hashlib
from pathlib import Path
from core.utils import p3
from core.handle.sendAudioHandle import send_stt_message, send_tts_message
from plugins_func.register import register_function, ToolType, ActionResponse, Action
from core.providers.tts.dto.dto import TTSMessageDTO, SentenceType, ContentType
from core.utils.dialogue import Message
import requests
try:
    from pydub import AudioSegment
except Exception:
    AudioSegment = None
from core.handle.sendLyricsHandle import start_lyrics_sync
from core.handle.saveLyricsHandle import save_lyrics

TAG = __name__

MUSIC_CACHE = {}

# 全局播放控制变量
PLAY_CONTROL = {
    "current_song": None,  # 当前播放的歌曲标识
    "interrupt_flag": False,  # 中断标志
    "lyrics_task": None,  # 当前歌词任务
    "is_playing": False  # 播放状态
}

play_music_function_desc = {
    "type": "function",
    "function": {
        "name": "play_music",
        "description": (
            "**用于处理用户的歌曲播放请求，是播放单曲的唯一方式**。\n"
            "触发规则：当用户说“播放XX”“放XX”“听XX”“点歌XX”“来一首歌”“我要听歌”等歌曲相关指令时，必须调用此函数。\n"
            "⚠️ 注意：如果用户指令中出现电台相关关键词（如“电台”“广播”“中国之声”等），即使包含“播放”也必须调用电台函数而不是本函数。\n"
            "提取规则：\n"
            "1. 直接取出用户在播放指令后的歌名（如“播放稻香”→“稻香”）。\n"
            "2. 若未提供歌名（如“播放音乐”），则默认播放“素颜”。\n"
            "3. 忽略无关修饰语（如“帮我播放一下稻香谢谢”→“稻香”）。\n"
            "4. 支持的触发动词包括：播放、放、听、点歌、来一首、想听、我要听、帮我点一首等。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "song_name": {
                    "type": "string",
                    "description": (
                        "提取的歌曲名称：\n"
                        "- 如果用户未指定歌名，默认“素颜”\n"
                        "- 若包含多余词汇，需提取核心歌名"
                    ),
                    "examples": [
                        {"用户": "播放素颜", "参数": "素颜"},
                        {"用户": "放稻香", "参数": "稻香"},
                        {"用户": "听稻香", "参数": "稻香"},
                        {"用户": "帮我播放稻香", "参数": "稻香"},
                        {"用户": "播放音乐", "参数": "素颜"},
                        {"用户": "来一首歌", "参数": "素颜"},
                        {"用户": "点歌稻香", "参数": "稻香"},
                        {"用户": "我要听歌", "参数": "素颜"},
                        {"用户": "帮我点一首稻香", "参数": "稻香"},
                        {"用户": "放一首七里香", "参数": "七里香"},
                        {"用户": "我想听晴天", "参数": "晴天"},
                        {"用户": "放一首歌", "参数": "素颜"}
                    ]
                }
            },
            "required": ["song_name"]
        }
    }
}



@register_function("play_music", play_music_function_desc, ToolType.SYSTEM_CTL)
def play_music(conn, song_name: str):
    if song_name is None:
        song_name="素颜"
    conn.logger.bind(tag=TAG).info("进入播放音乐功能函数")
    try:
        # 如果电台正在播放，先停止电台，避免与音乐/回复叠音
        try:
            from core.utils.radio_streamer import stop_radio_stream
            asyncio.run_coroutine_threadsafe(stop_radio_stream(conn.websocket), conn.loop)
        except Exception:
            pass

        # 触发中断（如果有正在播放的歌曲）
        global PLAY_CONTROL
        if PLAY_CONTROL["current_song"] is not None and PLAY_CONTROL["is_playing"]:
            PLAY_CONTROL["interrupt_flag"] = True
            conn.logger.bind(tag=TAG).info(f"触发中断，准备播放新歌曲: {song_name}")
            
            # 注意：audio_generation 的递增由 handleAbortMessage 统一处理，避免重复递增
            
            # 向客户端发送一次 stop，防止客户端保留的缓冲继续播放旧音频
            try:
                asyncio.run_coroutine_threadsafe(
                    send_tts_message(conn, "stop", None), conn.loop
                ).result(timeout=1)
            except Exception:
                pass
                
            # 立即清空播放相关队列，确保即时打断
            try:
                while not conn.tts.tts_text_queue.empty():
                    conn.tts.tts_text_queue.get_nowait()
            except Exception:
                pass
            try:
                while not conn.audio_play_queue.empty():
                    conn.audio_play_queue.get_nowait()
            except Exception:
                pass
            # 短暂等待旧播放协程感知并退出
            time.sleep(0.15)
        else:
            # 注意：audio_generation 的递增由 handleAbortMessage 统一处理，避免重复递增
            pass

        # 向客户端发送一次 stop，防止客户端保留的缓冲继续播放旧音频
        try:
            asyncio.run_coroutine_threadsafe(
                send_tts_message(conn, "stop", None), conn.loop
            ).result(timeout=1)
        except Exception:
            pass

        # 不要在此处切换 client_abort，避免与全局打断竞争导致旧音频恢复

        music_intent = (
            f"播放音乐 {song_name}" if song_name != "random" else "随机播放音乐"
        )

        # 检查事件循环状态
        if not conn.loop.is_running():
            conn.logger.bind(tag=TAG).error("事件循环未运行，无法提交任务")
            return ActionResponse(
                action=Action.RESPONSE, result="系统繁忙", response="请稍后再试"
            )

        # 提交异步任务
        future = asyncio.run_coroutine_threadsafe(
            handle_music_command(conn, music_intent), conn.loop
        )

        # 获取歌曲文件的绝对路径
        specific_file = "music"

        # # 启动歌词线程
        # lyric_future = asyncio.create_task(start_lyrics_sync(conn, specific_file))
        # conn.logger.bind(tag=TAG).info("开始处理歌词")

        # 非阻塞回调处理
        def handle_done(f):
            try:
                f.result()  # 可在此处理成功逻辑
                conn.logger.bind(tag=TAG).info("播放完成")
            except Exception as e:
                conn.logger.bind(tag=TAG).error(f"播放失败: {e}")

        # 非阻塞歌词回调处理
        # def lyrics_handle_done(f):
        #     try:
        #         f.result()  # 可在此处理成功逻辑
        #         conn.logger.bind(tag=TAG).info("歌词推送完成")
        #     except Exception as e:
        #         conn.logger.bind(tag=TAG).error(f"歌词推送失败: {e}")

        future.add_done_callback(handle_done)
        # lyric_future.add_done_callback(lyrics_handle_done)

        # return ActionResponse(
        #     action=Action.NONE, result="指令已接收", response=""
        # )
    except Exception as e:
        conn.logger.bind(tag=TAG).error(f"处理音乐意图错误: {e}")
        return ActionResponse(
            action=Action.RESPONSE, result=str(e), response="播放音乐时出错了"
        )


def _extract_song_name(text):
    """从用户输入中提取歌名"""
    for keyword in ["播放音乐"]:
        if keyword in text:
            parts = text.split(keyword)
            if len(parts) > 1:
                return parts[1].strip()
    return None


def _find_best_match(potential_song, music_files):
    """查找最匹配的歌曲（增强版）"""
    best_match = None
    highest_score = 0
    potential_song = re.sub(r'[^\w\s]', '', potential_song).lower()

    for music_file in music_files:
        song_name = os.path.splitext(music_file)[0]
        clean_name = re.sub(r'[^\w\s]', '', song_name).lower()

        # 使用组合相似度算法
        seq_ratio = difflib.SequenceMatcher(None, potential_song, clean_name).ratio()
        partial_ratio = difflib.SequenceMatcher(None, potential_song, clean_name).quick_ratio()
        score = (seq_ratio * 0.6 + partial_ratio * 0.4)  # 组合权重

        # 增加绝对匹配检测
        if potential_song in clean_name or clean_name in potential_song:
            score = max(score, 0.85)

        if score > highest_score and score > 0.6:  # 提升阈值到0.6
            highest_score = score
            best_match = music_file
            logging.getLogger(TAG).debug(f"新最佳匹配: {song_name} 得分: {score:.2f}")

    return best_match if highest_score >= 0.6 else None


def get_music_files(music_dir, music_ext):
    music_dir = Path(music_dir)
    music_files = []
    music_file_names = []
    for file in music_dir.rglob("*"):
        # 判断是否是文件
        if file.is_file():
            # 获取文件扩展名
            ext = file.suffix.lower()
            # 判断扩展名是否在列表中
            if ext in music_ext:
                # 添加相对路径
                music_files.append(str(file.relative_to(music_dir)))
                music_file_names.append(
                    os.path.splitext(str(file.relative_to(music_dir)))[0]
                )
    return music_files, music_file_names


def initialize_music_handler(conn):
    global MUSIC_CACHE
    if MUSIC_CACHE == {}:
        if "play_music" in conn.config["plugins"]:
            MUSIC_CACHE["music_config"] = conn.config["plugins"]["play_music"]
            MUSIC_CACHE["music_dir"] = os.path.abspath(
                MUSIC_CACHE["music_config"].get("music_dir", "./music")  # 默认路径修改
            )
            MUSIC_CACHE["music_ext"] = MUSIC_CACHE["music_config"].get(
                "music_ext", (".mp3", ".wav", ".p3")
            )
            MUSIC_CACHE["refresh_time"] = MUSIC_CACHE["music_config"].get(
                "refresh_time", 60
            )
        else:
            MUSIC_CACHE["music_dir"] = os.path.abspath("./music")
            MUSIC_CACHE["music_ext"] = (".mp3", ".wav", ".p3")
            MUSIC_CACHE["refresh_time"] = 60
        # 获取音乐文件列表
        MUSIC_CACHE["music_files"], MUSIC_CACHE["music_file_names"] = get_music_files(
            MUSIC_CACHE["music_dir"], MUSIC_CACHE["music_ext"]
        )
        MUSIC_CACHE["scan_time"] = time.time()
        MUSIC_CACHE["music_cache_dir"] = os.path.abspath("./music/cache")
        os.makedirs(MUSIC_CACHE["music_cache_dir"], exist_ok=True)
        MUSIC_CACHE["download_api"] = "http://datukuai.top:1450/djs/API/QQ_Music/api.php"
    return MUSIC_CACHE


def _detect_audio_type(file_path):
    """通过文件头检测音频类型（增强版）"""
    max_head_size = 4096  # 读取4KB内容进行检测
    with open(file_path, 'rb') as f:
        head = f.read(max_head_size)

        # MP3检测（ID3v1/v2标签）
        if head.startswith(b'ID3'):
            return 'mp3'

        # M4A检测（QuickTime文件格式）
        if head.startswith(b'ftyp'):
            return 'm4a'

        # WAV检测
        if head.startswith(b'RIFF'):
            return 'wav'

        # AAC检测（ADTS头部）
        if head.startswith(b'\x00\x00\x00\x1f\x61\x74\x64\x53'):
            return 'aac'

        # 其他流媒体格式检测
        # 继续检查常见的流媒体头部特征
        # FFV1视频流（虽然不是音频，但某些情况可能出现）
        if head.startswith(b'FFV1'):
            return 'unknown'  # 视为未知流媒体

        # 如果仍未检测到，继续扫描剩余内容
        # 查找MP3的魔数（可能在文件中间）
        mp3_signature = b'\x49\x44\x33'  # "ID3"
        pos = 0
        while pos < len(head) - 3:
            if head[pos:pos + 3] == mp3_signature:
                return 'mp3'
            pos += 1

        # 检查MPEG-4音频流
        mpeg4_signature = b'\x00\x00\x01'  # ISO BMFF标识符
        if head.find(mpeg4_signature) != -1:
            return 'm4a'

        return None


def _validate_download(temp_path, expected_size):
    """验证下载文件完整性"""
    if not os.path.exists(temp_path):
        return False
    downloaded_size = os.path.getsize(temp_path)
    if downloaded_size < expected_size * 0.9:  # 允许一定误差
        return False
    return True


async def play_online_music(conn, specific_file=None, song_name=None):
    """播放在线音乐文件（支持中断）"""
    try:
        conn.logger.bind(tag=TAG).info("play_online_music 函数开始执行")
        
        # 再发一次 stop，双保险清理前端播放器缓冲
        try:
            await send_tts_message(conn, "stop", None)
        except Exception:
            pass
        
        # 统一递增音频代次，确保新播放与中断处理同步（移到音频入队前）
        if hasattr(conn, "audio_generation"):
            # 递增音频代次，确保与中断处理同步
            conn.audio_generation += 1
            conn.logger.bind(tag=TAG).info(f"新音乐播放开始，audio_generation递增至: {conn.audio_generation}")

        # 生成唯一歌曲标识（使用文件路径哈希）
        song_id = hashlib.md5(specific_file.encode()).hexdigest()

        # 更新当前播放状态
        PLAY_CONTROL["current_song"] = song_id
        PLAY_CONTROL["interrupt_flag"] = False
        PLAY_CONTROL["is_playing"] = True

        # 重置client_abort状态，确保新音频能够发送
        conn.client_abort = False
        conn.logger.bind(tag=TAG).info(f"音乐文件绝对路径{specific_file}")

        # 随机选择播放提示语
        text = _get_random_play_prompt(song_name)
        status = f"正在播放歌曲: {song_name}"
        await send_stt_message(conn, text)
        conn.logger.bind(tag=TAG).info(status)
        conn.tts_last_text_index = 0
        conn.tts_first_text_index = 0

        # 修复音乐播放启动机制：先发送FIRST信号启动播放器，再发送音乐文件
        # 发送FIRST信号以启动客户端播放器
        tts_start_msg = TTSMessageDTO(
            sentence_id=conn.sentence_id,
            sentence_type=SentenceType.FIRST,
            content_type=ContentType.ACTION,
        )
        conn.tts.tts_text_queue.put(tts_start_msg)

        # 发送音频文件到播放队列
        tts_msg = TTSMessageDTO(
            sentence_id=conn.sentence_id,
            sentence_type=SentenceType.MIDDLE,
            content_type=ContentType.FILE,
            content_file=specific_file,
        )
        conn.tts.tts_text_queue.put(tts_msg)

        # 启动歌词线程（添加中断检查）
        # lyrics_task = asyncio.create_task(start_lyrics_sync(conn, specific_file, song_id))
        # PLAY_CONTROL["lyrics_task"] = lyrics_task
        # conn.logger.bind(tag=TAG).info("开始处理歌词")

        # 检查是否被新点歌替换或被显式中断（每0.05秒检查一次）
        while (
            PLAY_CONTROL["current_song"] == song_id
            and not PLAY_CONTROL["interrupt_flag"]
            and conn.tts.tts_text_queue.qsize() > 0
        ):
            await asyncio.sleep(0.05)

        # 如果被替换（新歌覆盖 current_song），直接退出，不要清空全局队列（避免清掉新歌的队列）
        if PLAY_CONTROL["current_song"] != song_id:
            conn.logger.bind(tag=TAG).info(f"歌曲《{song_name}》被新点歌替换")
            PLAY_CONTROL["is_playing"] = False
            return

        # 如果被显式中断，清空队列
        if PLAY_CONTROL["interrupt_flag"]:
            conn.logger.bind(tag=TAG).info(f"歌曲《{song_name}》被中断")
            # 清空TTS队列
            while not conn.tts.tts_text_queue.empty():
                try:
                    conn.tts.tts_text_queue.get_nowait()
                except:
                    pass
            # 清空音频播放队列
            while not conn.audio_play_queue.empty():
                try:
                    conn.audio_play_queue.get_nowait()
                except:
                    pass
            # 取消歌词任务
            if PLAY_CONTROL["lyrics_task"] and not PLAY_CONTROL["lyrics_task"].done():
                PLAY_CONTROL["lyrics_task"].cancel()
            # 重置播放状态
            PLAY_CONTROL["is_playing"] = False
            PLAY_CONTROL["current_song"] = None
            return

        # 正常播放结束
        conn.tts.tts_text_queue.put(
            TTSMessageDTO(
                sentence_id=conn.sentence_id,
                sentence_type=SentenceType.LAST,
                content_type=ContentType.ACTION,
            )
        )

        # 重置播放状态
        PLAY_CONTROL["is_playing"] = False
        PLAY_CONTROL["current_song"] = None

    except Exception as e:
        conn.logger.bind(tag=TAG).error(f"播放在线音乐失败: {str(e)}")
        conn.logger.bind(tag=TAG).error(f"详细错误: {traceback.format_exc()}")
        # 重置播放状态
        PLAY_CONTROL["is_playing"] = False
        PLAY_CONTROL["current_song"] = None


def _cleanup_files(conn, file_paths):
    """清理指定的文件"""
    for path in file_paths:
        if os.path.exists(path):
            try:
                os.remove(path)
                conn.logger.bind(tag=TAG).info(f"清理文件: {path}")
            except Exception as e:
                conn.logger.bind(tag=TAG).error(f"清理文件失败: {path} - {str(e)}")


def convert_to_mp3(conn, input_path):
    """将音频文件转换为MP3格式（增强版）"""
    try:
        if input_path.endswith('.mp3'):
            return input_path

        output_path = os.path.join(MUSIC_CACHE["music_cache_dir"], f"{os.path.basename(input_path)}.mp3")

        # 优先使用 pydub，如不可用或失败则回退到 ffmpeg
        if AudioSegment is not None and (input_path.endswith('.m4a') or input_path.endswith('.aac')):
            try:
                fmt = 'm4a' if input_path.endswith('.m4a') else 'aac'
                audio = AudioSegment.from_file(input_path, format=fmt, parameters=["-nostdin"]) if fmt == 'm4a' else AudioSegment.from_file(input_path, format=fmt)
                audio.export(output_path, format='mp3', bitrate='192k')
                return output_path
            except Exception:
                pass

        # 使用 ffmpeg 回退方案
        ffmpeg_cmd = [
            'ffmpeg', '-nostdin', '-y', '-loglevel', 'error',
            '-i', input_path,
            '-f', 'mp3', '-b:a', '192k',
            output_path
        ]
        subprocess.run(ffmpeg_cmd, check=True)
        return output_path
    except Exception as e:
        _cleanup_files(conn, [input_path])
        raise e

async def _stream_download_and_convert(conn, music_url, processed_song_name):
    """快速播放优先版本：将阻塞的下载/转码移到后台线程，128KB 即开播"""
    mp3_cache_path = os.path.join(MUSIC_CACHE["music_cache_dir"], f"{processed_song_name}.mp3")
    os.makedirs(MUSIC_CACHE["music_cache_dir"], exist_ok=True)

    ext = os.path.splitext(music_url.split("?")[0])[1].lower()
    is_mp3 = ext == ".mp3"

    conn.logger.bind(tag=TAG).info(f"开始下载: {music_url} (源格式: {ext or '未知'})")

    # 使用事件在达到可播放阈值时立即开播，避免阻塞事件循环
    start_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_start_once():
        if not start_event.is_set():
            loop.call_soon_threadsafe(start_event.set)

    def _download_mp3_blocking():
        with requests.get(music_url, stream=True, timeout=10, headers={'User-Agent': 'Mozilla/5.0'}) as r:
            r.raise_for_status()
            with open(mp3_cache_path, 'wb') as f_cache:
                for chunk in r.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    f_cache.write(chunk)
                    # 128KB 缓冲即允许播放
                    if f_cache.tell() >= 128 * 1024:
                        _signal_start_once()

    def _transcode_to_mp3_blocking():
        start_time = time.time()

        try:
            # === 1. 发起流式下载 ===
            with requests.get(music_url, stream=True, timeout=30, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                r.raise_for_status()

                # === 2. 启动 ffmpeg 子进程，输入为 pipe，输出为本地文件 ===
                ffmpeg_cmd = [
                    "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
                    "-i", "pipe:0",  # 从 stdin 接收原始音频流
                    "-f", "mp3",
                    "-b:a", "128k",
                    "-ac", "1",
                    "-ar", "22050",
                    "-acodec", "libmp3lame",
                    "-compression_level", "10",
                    mp3_cache_path  # 直接输出到目标文件
                ]

                conn.logger.bind(tag=TAG).info(f"执行流式ffmpeg命令: {' '.join(ffmpeg_cmd)}")

                process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE
                )

                # === 3. 边下载边喂给 ffmpeg stdin，同时监控输出文件 ===
                downloaded = 0
                playback_triggered = False
                
                for chunk in r.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    process.stdin.write(chunk)
                    downloaded += len(chunk)

                    # 检查输出文件大小，达到128KB时立即触发播放
                    if not playback_triggered and os.path.exists(mp3_cache_path):
                        output_size = os.path.getsize(mp3_cache_path)
                        if output_size >= 128 * 1024:  # 128KB
                            conn.logger.bind(tag=TAG).info(f"转码输出达到128KB，立即开始播放")
                            _signal_start_once()
                            playback_triggered = True

                    # 可选：记录进度
                    # if downloaded % (1024 * 100) == 0:
                    #     conn.logger.bind(tag=TAG).info(f"已传输: {downloaded} bytes 到 ffmpeg")

                # === 4. 关闭 stdin，等待转码完成 ===
                process.stdin.close()
                stderr_output = process.stderr.read()
                process.stderr.close()
                return_code = process.wait()

                if return_code != 0:
                    error_msg = stderr_output.decode() if isinstance(stderr_output, bytes) else str(stderr_output)
                    raise RuntimeError(f"ffmpeg 转码失败: {error_msg}")

            # === 5. 记录耗时，如果还没触发播放则触发 ===
            total_time = time.time() - start_time
            output_size = os.path.getsize(mp3_cache_path) if os.path.exists(mp3_cache_path) else 0

            conn.logger.bind(tag=TAG).info(f"下载+转码完成，总耗时: {total_time:.2f}秒")
            conn.logger.bind(tag=TAG).info(f"输出文件大小: {output_size} bytes")

            if output_size == 0:
                raise Exception("转码后文件为空")

            # 如果由于某种原因还没触发播放，现在触发
            if not playback_triggered:
                _signal_start_once()

        except Exception as e:
            # 清理可能的残余文件
            if os.path.exists(mp3_cache_path):
                try:
                    os.remove(mp3_cache_path)
                except:
                    pass
            conn.logger.bind(tag=TAG).error(f"流式转码失败: {e}")
            raise e
    # 在后台线程执行阻塞型网络/转码
    if is_mp3:
        asyncio.create_task(asyncio.to_thread(_download_mp3_blocking))
    else:
        asyncio.create_task(asyncio.to_thread(_transcode_to_mp3_blocking))

    # 达到可播放阈值后立即开播（不会阻塞下载/转码）
    await start_event.wait()
    conn.logger.bind(tag=TAG).info("开播")
    asyncio.create_task(play_online_music(
        conn,
        specific_file=mp3_cache_path,
        song_name=processed_song_name
    ))

    # 不等待后台下载/转码完成，这样不会阻塞事件循环
    return mp3_cache_path


async def handle_online_song_command(conn, song_name):
    """处理在线点歌指令（快速播放优先版，歌词完全后台）"""
    try:
        # 强制立即打断并清空客户端播放缓存，确保切歌无延迟
        conn.client_abort = True
        # 注意：不在此处递增 audio_generation，由 play_online_music 统一处理
        conn.clear_queues()
        await send_tts_message(conn, "stop", None)
        # 确保stop消息被发送并处理
        await asyncio.sleep(0.1)
        
        # 重置client_abort状态，确保新音频能够发送
        conn.client_abort = False

        processed_song_name = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9_]', '', song_name.strip()) or "unknown"

        # 先查缓存
        music_cache_files = [f for f in os.listdir(MUSIC_CACHE["music_cache_dir"]) if f.endswith('.mp3')]
        if music_cache_files:
            for f in music_cache_files:
                if processed_song_name in f:
                    mp3_path = os.path.join(MUSIC_CACHE["music_cache_dir"], f)
                    # await send_stt_message(conn, f"正在播放在线歌曲: {processed_song_name}")
                    await play_online_music(conn, specific_file=mp3_path, song_name=processed_song_name)
                    return True

        # 请求 API（只获取音乐地址，不在这里下歌词）
        start_time=time.time()
        response = requests.get(MUSIC_CACHE["download_api"], params={'msg': song_name, 'n': 1}, timeout=10)
        print(f"音乐api请求{round(time.time()-start_time,3)}秒")
        response.raise_for_status()
        data = response.json()

        if data.get('code') != 1:
            await send_stt_message(conn, "播放失败，请换首歌试试")
            return False

        # 歌曲名处理
        singer = data['data'].get('singer', '') or ''
        song = data['data'].get('song', '') or ''
        clean_singer = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9_]', '', singer.strip())
        clean_song = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9_]', '', song.strip())
        processed_song_name = f"{clean_singer} - {clean_song}".strip() or "unknown"

        music_url = data['data']['music']

        # 先启动音乐秒播
        mp3_cache_path = await _stream_download_and_convert(conn, music_url, processed_song_name)

        # 再后台启动歌词下载（不阻塞播放）
        # music_mid = data['data'].get('mid') or ""
        # if music_mid:
        #     asyncio.create_task(save_lyrics(
        #         conn, song_id=music_mid, id_type='mid',
        #         lyrics_dir=MUSIC_CACHE["music_cache_dir"],
        #         file_name=processed_song_name
        #     ))

    except Exception as e:
        conn.logger.bind(tag=TAG).error(f"在线点歌失败: {str(e)}")
        await send_stt_message(conn, f"在线点歌失败，请稍后再试。错误详情: {str(e)}")
        return False

async def handle_music_command(conn, text):
    initialize_music_handler(conn)
    global MUSIC_CACHE

    """处理音乐播放指令"""
    clean_text = re.sub(r"[^\w\s]", "", text).strip()
    conn.logger.bind(tag=TAG).debug(f"检查是否是音乐命令: {clean_text}")

    song_name = _extract_song_name(clean_text)
    await handle_online_song_command(conn, song_name)
    return True

    # 尝试匹配具体歌名
    if os.path.exists(MUSIC_CACHE["music_dir"]):
        if time.time() - MUSIC_CACHE["scan_time"] > MUSIC_CACHE["refresh_time"]:
            # 刷新音乐文件列表
            MUSIC_CACHE["music_files"], MUSIC_CACHE["music_file_names"] = (
                get_music_files(MUSIC_CACHE["music_dir"], MUSIC_CACHE["music_ext"])
            )
            MUSIC_CACHE["scan_time"] = time.time()

        potential_song = _extract_song_name(clean_text)
        if potential_song:
            best_match = _find_best_match(potential_song, MUSIC_CACHE["music_files"])
            if best_match:
                conn.logger.bind(tag=TAG).info(f"找到最匹配的歌曲: {best_match}")
                await play_local_music(conn, specific_file=best_match)
                return True
    # 检查是否是通用播放音乐命令
    await play_local_music(conn)
    return True


def _get_random_play_prompt(song_name):
    """生成随机播放引导语"""
    # 移除文件扩展名
    clean_name = os.path.splitext(song_name)[0]
    prompts = [
        f"正在为您播放，{clean_name}",
        f"请欣赏歌曲，{clean_name}",
        f"即将为您播放，{clean_name}",
        f"为您带来，{clean_name}",
        f"让我们聆听，{clean_name}",
        f"接下来请欣赏，{clean_name}",
        f"为您献上，{clean_name}",
    ]
    # 直接使用random.choice，不设置seed
    return random.choice(prompts)


async def play_local_music(conn, specific_file=None):
    global MUSIC_CACHE
    """播放本地音乐文件（支持中断）"""
    try:
        # 生成唯一歌曲标识
        song_id = hashlib.md5(specific_file.encode() if specific_file else str(time.time()).encode()).hexdigest()

        # 更新当前播放状态
        global PLAY_CONTROL
        PLAY_CONTROL["current_song"] = song_id
        PLAY_CONTROL["interrupt_flag"] = False
        PLAY_CONTROL["is_playing"] = True

        if not os.path.exists(MUSIC_CACHE["music_dir"]):
            conn.logger.bind(tag=TAG).error(
                f"音乐目录不存在: " + MUSIC_CACHE["music_dir"]
            )
            return

        # 确保路径正确性
        if specific_file:
            selected_music = specific_file
            music_path = os.path.join(MUSIC_CACHE["music_dir"], specific_file)
        else:
            if not MUSIC_CACHE["music_files"]:
                conn.logger.bind(tag=TAG).error("未找到MP3音乐文件")
                return
            selected_music = random.choice(MUSIC_CACHE["music_files"])
            music_path = os.path.join(MUSIC_CACHE["music_dir"], selected_music)

        if not os.path.exists(music_path):
            conn.logger.bind(tag=TAG).error(f"选定的音乐文件不存在: {music_path}")
            return

        conn.llm_finish_task = True

        # 检查中断标志
        if PLAY_CONTROL["interrupt_flag"]:
            conn.logger.bind(tag=TAG).info("本地音乐播放被中断")
            PLAY_CONTROL["is_playing"] = False
            PLAY_CONTROL["current_song"] = None
            return

        if music_path.endswith(".p3"):
            opus_packets, _ = p3.decode_opus_from_file(music_path)
        else:
            opus_packets, _ = conn.tts.audio_to_opus_data(music_path)

        # 检查中断标志
        if PLAY_CONTROL["interrupt_flag"]:
            conn.logger.bind(tag=TAG).info("本地音乐播放被中断")
            PLAY_CONTROL["is_playing"] = False
            PLAY_CONTROL["current_song"] = None
            return

        conn.audio_play_queue.put((opus_packets, None, conn.tts_last_text_index))

        # 重置播放状态
        PLAY_CONTROL["is_playing"] = False
        PLAY_CONTROL["current_song"] = None

    except Exception as e:
        conn.logger.bind(tag=TAG).error(f"播放音乐失败: {str(e)}")
        conn.logger.bind(tag=TAG).error(f"详细错误: {traceback.format_exc()}")
        # 重置播放状态
        PLAY_CONTROL["is_playing"] = False
        PLAY_CONTROL["current_song"] = None


def interrupt_current_music(conn):
    """中断当前正在播放的音乐"""
    global PLAY_CONTROL
    # 无条件执行中断，确保任意播放都能被停止
    PLAY_CONTROL["interrupt_flag"] = True
    conn.logger.bind(tag=TAG).info("触发音乐中断")

    # 先向客户端发出中断信号，防止旧音频包继续播放
    try:
        conn.client_abort = True
        # 注意：audio_generation 的递增由 handleAbortMessage 统一处理，避免重复递增
        # 通知客户端立即停止，清空客户端端的缓冲
        try:
            asyncio.run_coroutine_threadsafe(
                send_tts_message(conn, "stop", None), conn.loop
            ).result(timeout=0.2)
        except Exception:
            pass
        # 给客户端一些时间处理 stop 和中断
        time.sleep(0.05)
    except Exception:
        pass

    # 清空TTS队列
    try:
        while not conn.tts.tts_text_queue.empty():
            conn.tts.tts_text_queue.get_nowait()
    except Exception:
        pass

    # 清空音频播放队列
    try:
        while not conn.audio_play_queue.empty():
            conn.audio_play_queue.get_nowait()
    except Exception:
        pass

    # 取消歌词任务
    try:
        if PLAY_CONTROL["lyrics_task"] and not PLAY_CONTROL["lyrics_task"].done():
            PLAY_CONTROL["lyrics_task"].cancel()
    except Exception:
        pass

    # 重置播放状态
    PLAY_CONTROL["is_playing"] = False
    PLAY_CONTROL["current_song"] = None
    PLAY_CONTROL["interrupt_flag"] = False

    # 允许后续新播放
    try:
        conn.client_abort = False
    except Exception:
        pass

    conn.logger.bind(tag=TAG).info("音乐中断完成")
    return True


def get_current_play_status():
    """获取当前播放状态"""
    global PLAY_CONTROL
    return {
        "is_playing": PLAY_CONTROL["is_playing"],
        "current_song": PLAY_CONTROL["current_song"],
        "interrupt_flag": PLAY_CONTROL["interrupt_flag"]
    }
