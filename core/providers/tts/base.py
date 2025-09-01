import os
import re
import queue
import uuid
import asyncio
import threading
from core.utils import p3
from datetime import datetime
from core.utils import textUtils
from abc import ABC, abstractmethod
from config.logger import setup_logging
from core.utils.util import audio_to_data, audio_bytes_to_data
from core.utils.tts import MarkdownCleaner
from core.utils.output_counter import add_device_output
from core.handle.reportHandle import enqueue_tts_report
from core.handle.sendAudioHandle import sendAudioMessage
from core.providers.tts.dto.dto import (
    TTSMessageDTO,
    SentenceType,
    ContentType,
    InterfaceType,
)

import traceback

TAG = __name__
logger = setup_logging()


class TTSProviderBase(ABC):
    def __init__(self, config, delete_audio_file):
        self.interface_type = InterfaceType.NON_STREAM
        self.conn = None
        self.tts_timeout = 10
        self.delete_audio_file = delete_audio_file
        self.audio_file_type = "wav"
        self.output_file = config.get("output_dir", "tmp/")
        self.tts_text_queue = queue.Queue()
        self.tts_audio_queue = queue.Queue()
        self.tts_audio_first_sentence = True
        self.before_stop_play_files = []

        self.tts_text_buff = []
        self.punctuations = (
            "。",
            # ".",
            "？",
            "?",
            "！",
            "!",
            "；",
            ";",
            "：",
        )
        self.first_sentence_punctuations = (
            "，",
            "～",
            "~",
            "、",
            ",",
            "。",
            # ".",
            "？",
            "?",
            "！",
            "!",
            "；",
            ";",
            "：",
        )
        self.tts_stop_request = False
        self.processed_chars = 0
        self.is_first_sentence = True

    def generate_filename(self, extension=".wav"):
        return os.path.join(
            self.output_file,
            f"tts-{datetime.now().date()}@{uuid.uuid4().hex}{extension}",
        )

    def to_tts(self, text):
        text = MarkdownCleaner.clean_markdown(text)
        max_repeat_time = 5
        if self.delete_audio_file:
            # 需要删除文件的直接转为音频数据
            while max_repeat_time > 0:
                try:
                    audio_bytes = asyncio.run(self.text_to_speak(text, None))
                    if audio_bytes:
                        audio_datas, _ = audio_bytes_to_data(
                            audio_bytes, file_type=self.audio_file_type, is_opus=True
                        )
                        return audio_datas
                    else:
                        max_repeat_time -= 1
                except Exception as e:
                    logger.bind(tag=TAG).warning(
                        f"语音生成失败{5 - max_repeat_time + 1}次: {text}，错误: {e}"
                    )
                    max_repeat_time -= 1
            if max_repeat_time > 0:
                logger.bind(tag=TAG).info(
                    f"语音生成成功: {text}，重试{5 - max_repeat_time}次"
                )
            else:
                logger.bind(tag=TAG).error(
                    f"语音生成失败: {text}，请检查网络或服务是否正常"
                )
            return None
        else:
            tmp_file = self.generate_filename()
            try:
                while not os.path.exists(tmp_file) and max_repeat_time > 0:
                    try:
                        asyncio.run(self.text_to_speak(text, tmp_file))
                    except Exception as e:
                        logger.bind(tag=TAG).warning(
                            f"语音生成失败{5 - max_repeat_time + 1}次: {text}，错误: {e}"
                        )
                        # 未执行成功，删除文件
                        if os.path.exists(tmp_file):
                            os.remove(tmp_file)
                        max_repeat_time -= 1

                if max_repeat_time > 0:
                    logger.bind(tag=TAG).info(
                        f"语音生成成功: {text}:{tmp_file}，重试{5 - max_repeat_time}次"
                    )
                else:
                    logger.bind(tag=TAG).error(
                        f"语音生成失败: {text}，请检查网络或服务是否正常"
                    )

                return tmp_file
            except Exception as e:
                logger.bind(tag=TAG).error(f"Failed to generate TTS file: {e}")
                return None

    @abstractmethod
    async def text_to_speak(self, text, output_file):
        pass

    def audio_to_pcm_data(self, audio_file_path):
        """音频文件转换为PCM编码"""
        return audio_to_data(audio_file_path, is_opus=False)

    def audio_to_opus_data(self, audio_file_path):
        """音频文件转换为Opus编码"""
        return audio_to_data(audio_file_path, is_opus=True)

    def tts_one_sentence(
        self,
        conn,
        content_type,
        content_detail=None,
        content_file=None,
        sentence_id=None,
    ):
        """发送一句话"""
        if not sentence_id:
            if conn.sentence_id:
                sentence_id = conn.sentence_id
            else:
                sentence_id = str(uuid.uuid4()).replace("-", "")
                conn.sentence_id = sentence_id
        self.tts_text_queue.put(
            TTSMessageDTO(
                sentence_id=sentence_id,
                sentence_type=SentenceType.FIRST,
                content_type=ContentType.ACTION,
            )
        )
        # 对于单句的文本，进行分段处理
        segments = re.split(r'([。！？!?；;\n])', content_detail)
        for seg in segments:
            self.tts_text_queue.put(
                TTSMessageDTO(
                    sentence_id=sentence_id,
                    sentence_type=SentenceType.MIDDLE,
                    content_type=content_type,
                    content_detail=seg,
                    content_file=content_file,
                )
            )
        self.tts_text_queue.put(
            TTSMessageDTO(
                sentence_id=sentence_id,
                sentence_type=SentenceType.LAST,
                content_type=ContentType.ACTION,
            )
        )

    async def open_audio_channels(self, conn):
        self.conn = conn
        self.tts_timeout = conn.config.get("tts_timeout", 10)
        # tts 消化线程
        self.tts_priority_thread = threading.Thread(
            target=self.tts_text_priority_thread, daemon=True
        )
        self.tts_priority_thread.start()

        # 音频播放 消化线程
        self.audio_play_priority_thread = threading.Thread(
            target=self._audio_play_priority_thread, daemon=True
        )
        self.audio_play_priority_thread.start()

    # 这里默认是非流式的处理方式
    # 流式处理方式请在子类中重写
    def tts_text_priority_thread(self):
        while not self.conn.stop_event.is_set():
            try:
                message = self.tts_text_queue.get(timeout=1)
                if self.conn.client_abort:
                    logger.bind(tag=TAG).info("收到打断信息，终止TTS文本处理线程")
                    continue
                if message.sentence_type == SentenceType.FIRST:
                    # 初始化参数
                    self.tts_stop_request = False
                    self.processed_chars = 0
                    self.tts_text_buff = []
                    self.is_first_sentence = True
                    self.tts_audio_first_sentence = True
                    # 将 FIRST 事件入队，触发句子开始信号
                    self.tts_audio_queue.put((SentenceType.FIRST, [], message.content_detail))
                elif ContentType.TEXT == message.content_type:
                    self.tts_text_buff.append(message.content_detail)
                    segment_text = self._get_segment_text()
                    if segment_text:
                        if self.delete_audio_file:
                            audio_datas = self.to_tts(segment_text)
                            if audio_datas:
                                self.tts_audio_queue.put(
                                    (message.sentence_type, audio_datas, segment_text)
                                )
                        else:
                            tts_file = self.to_tts(segment_text)
                            if tts_file:
                                audio_datas = self._process_audio_file(tts_file)
                                self.tts_audio_queue.put(
                                    (message.sentence_type, audio_datas, segment_text)
                                )
                elif ContentType.FILE == message.content_type:
                    self._process_remaining_text()
                    tts_file = message.content_file
                    logger.bind(tag=TAG).info(f"处理音乐文件: {tts_file}, 文件存在: {os.path.exists(tts_file) if tts_file else False}")
                    if tts_file and os.path.exists(tts_file):
                        # 使用流式处理音频文件
                        self._process_audio_file_streaming(tts_file, message.sentence_type, message.content_detail)
                    else:
                        logger.bind(tag=TAG).error(f"音乐文件不存在或路径为空: {tts_file}")

                if message.sentence_type == SentenceType.LAST:
                    self._process_remaining_text()
                    self.tts_audio_queue.put(
                        (message.sentence_type, [], message.content_detail)
                    )

            except queue.Empty:
                continue
            except Exception as e:
                logger.bind(tag=TAG).error(
                    f"处理TTS文本失败: {str(e)}, 类型: {type(e).__name__}, 堆栈: {traceback.format_exc()}"
                )
                continue

    def _audio_play_priority_thread(self):
        while not self.conn.stop_event.is_set():
            text = None
            try:
                try:
                    sentence_type, audio_datas, text = self.tts_audio_queue.get(
                        timeout=1
                    )
                except queue.Empty:
                    if self.conn.stop_event.is_set():
                        break
                    continue
                future = asyncio.run_coroutine_threadsafe(
                    sendAudioMessage(self.conn, sentence_type, audio_datas, text),
                    self.conn.loop,
                )
                future.result()
                if self.conn.max_output_size > 0 and text:
                    add_device_output(self.conn.headers.get("device-id"), len(text))
                enqueue_tts_report(self.conn, text, audio_datas)
            except Exception as e:
                logger.bind(tag=TAG).error(
                    f"audio_play_priority priority_thread: {text} {e}"
                )

    async def start_session(self, session_id):
        pass

    async def finish_session(self, session_id):
        pass

    async def close(self):
        """资源清理方法"""
        if hasattr(self, "ws") and self.ws:
            await self.ws.close()

    def _get_segment_text(self):
        # 合并当前全部文本并处理未分割部分
        full_text = "".join(self.tts_text_buff)
        current_text = full_text[self.processed_chars :]  # 从未处理的位置开始
        last_punct_pos = -1

        # 根据是否是第一句话选择不同的标点符号集合
        punctuations_to_use = (
            self.first_sentence_punctuations
            if self.is_first_sentence
            else self.punctuations
        )

        for punct in punctuations_to_use:
            pos = current_text.rfind(punct)
            if (pos != -1 and last_punct_pos == -1) or (
                pos != -1 and pos < last_punct_pos
            ):
                last_punct_pos = pos

        if last_punct_pos != -1:
            segment_text_raw = current_text[: last_punct_pos + 1]
            segment_text = textUtils.get_string_no_punctuation_or_emoji(
                segment_text_raw
            )
            self.processed_chars += len(segment_text_raw)  # 更新已处理字符位置

            # 如果是第一句话，在找到第一个逗号后，将标志设置为False
            if self.is_first_sentence:
                self.is_first_sentence = False

            return segment_text
        elif self.tts_stop_request and current_text:
            segment_text = current_text
            self.is_first_sentence = True  # 重置标志
            return segment_text
        else:
            return None

    def _process_audio_file(self, tts_file):
        """处理音频文件并转换为指定格式

        Args:
            tts_file: 音频文件路径
            content_detail: 内容详情

        Returns:
            tuple: (sentence_type, audio_datas, content_detail)
        """
        audio_datas = []
        if tts_file.endswith(".p3"):
            audio_datas, _ = p3.decode_opus_from_file(tts_file)
        elif self.conn.audio_format == "pcm":
            audio_datas, _ = self.audio_to_pcm_data(tts_file)
        else:
            audio_datas, _ = self.audio_to_opus_data(tts_file)

        if (
            self.delete_audio_file
            and tts_file is not None
            and os.path.exists(tts_file)
            and tts_file.startswith(self.output_file)
        ):
            os.remove(tts_file)
        return audio_datas

    def _process_audio_file_streaming(self, tts_file, sentence_type, content_detail):
        """两部分发送音频文件：第一部分128KB快速启动，第二部分剩余音频
        
        Args:
            tts_file: 音频文件路径
            sentence_type: 句子类型
            content_detail: 内容详情
        """
        import threading
        import subprocess
        import numpy as np
        import opuslib_next

        def streaming_process():
            logger.bind(tag=TAG).info("进入streaming_process")
            
            # 发送音频流开始标记
            self.tts_audio_queue.put((SentenceType.FIRST, [], content_detail))
            logger.bind(tag=TAG).info("发送音频流开始标记")

            proc = None
            try:
                # 使用 ffmpeg 以流式方式解码为单声道/16kHz/16bit PCM，避免 pydub 预解码带来的冷启动延迟
                cmd = [
                    "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
                    "-i", tts_file, "-ac", "1", "-ar", "16000", "-f", "s16le", "pipe:1"
                ]
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)

                # 初始化 Opus 编码器（或直接透传 PCM）
                encoder = opuslib_next.Encoder(16000, 1, opuslib_next.APPLICATION_AUDIO)

                # 60ms 一帧（与发送端对齐）
                frame_duration = 60
                frame_size = int(16000 * frame_duration / 1000)  # 960 samples/frame
                bytes_per_frame = frame_size * 2  # s16le => 2 bytes/sample

                # 两段发送策略：第一段固定时长的小包，第二段一次性发送剩余帧
                first_part_seconds = 8  # 可根据需要调整（过小可能导致第二段未准备好而产生间隙）
                first_part_target_frames = max(1, int(first_part_seconds * 1000 / frame_duration))

                first_part = []
                remaining_datas = []
                total_frames = 0

                while True:
                    if self.conn.client_abort:
                        logger.bind(tag=TAG).info("收到打断信息，终止流式解码")
                        break

                    data = proc.stdout.read(bytes_per_frame)
                    if not data:
                        break

                    if len(data) < bytes_per_frame:
                        data += b"\x00" * (bytes_per_frame - len(data))

                    if self.conn.audio_format == "pcm":
                        frame_data = data
                    else:
                        np_frame = np.frombuffer(data, dtype=np.int16)
                        frame_data = encoder.encode(np_frame.tobytes(), frame_size)

                    total_frames += 1

                    # 先累积第一段
                    if len(first_part) < first_part_target_frames:
                        first_part.append(frame_data)
                        # 第一段达到目标帧数后立即发送
                        if len(first_part) == first_part_target_frames:
                            logger.bind(tag=TAG).info(f"发送首段音频数据，帧数={len(first_part)}（约{first_part_seconds}秒）")
                            self.tts_audio_queue.put((SentenceType.MIDDLE, first_part, content_detail))
                        continue

                    # 其余帧全部累积到剩余部分
                    remaining_datas.append(frame_data)

                # 发送剩余部分（一次性）
                if remaining_datas and not self.conn.client_abort:
                    logger.bind(tag=TAG).info(f"发送剩余音频数据，帧数={len(remaining_datas)}")
                    self.tts_audio_queue.put((SentenceType.MIDDLE, remaining_datas, content_detail))
                    # 发送结束标记
                    self.tts_audio_queue.put((SentenceType.LAST, [], content_detail))

                logger.bind(tag=TAG).info(f"流式音乐发送完成，总帧数={total_frames}")

            except Exception as e:
                logger.bind(tag=TAG).error(f"流式音频处理失败: {str(e)}")
            finally:
                try:
                    if proc:
                        proc.stdout.close()
                        proc.kill()
                except Exception:
                    pass
        
        # 在单独线程中执行流式处理
        streaming_thread = threading.Thread(target=streaming_process, daemon=True)
        logger.bind(tag=TAG).info(
            "音频播放线程准备"
        )
        streaming_thread.start()

    def _process_before_stop_play_files(self):
        for tts_file, text in self.before_stop_play_files:
            if tts_file and os.path.exists(tts_file):
                audio_datas = self._process_audio_file(tts_file)
                self.tts_audio_queue.put((SentenceType.MIDDLE, audio_datas, text))
        self.before_stop_play_files.clear()
        self.tts_audio_queue.put((SentenceType.LAST, [], None))

    def _process_remaining_text(self):
        """处理剩余的文本并生成语音

        Returns:
            bool: 是否成功处理了文本
        """
        full_text = "".join(self.tts_text_buff)
        remaining_text = full_text[self.processed_chars :]
        if remaining_text:
            segment_text = textUtils.get_string_no_punctuation_or_emoji(remaining_text)
            if segment_text:
                if self.delete_audio_file:
                    audio_datas = self.to_tts(segment_text)
                    if audio_datas:
                        self.tts_audio_queue.put(
                            (SentenceType.MIDDLE, audio_datas, segment_text)
                        )
                else:
                    tts_file = self.to_tts(segment_text)
                    audio_datas = self._process_audio_file(tts_file)
                    self.tts_audio_queue.put(
                        (SentenceType.MIDDLE, audio_datas, segment_text)
                    )
                self.processed_chars += len(full_text)
                return True
        return False
