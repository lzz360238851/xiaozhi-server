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
            ".",
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
            ".",
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
        from pydub import AudioSegment
        import numpy as np
        import opuslib_next
        
        def streaming_process():
            try:
                # 获取文件后缀名
                file_type = os.path.splitext(tts_file)[1]
                if file_type:
                    file_type = file_type.lstrip(".")
                
                # 读取音频文件
                audio = AudioSegment.from_file(
                    tts_file, format=file_type, parameters=["-nostdin"]
                )
                
                # 转换为单声道/16kHz采样率/16位小端编码
                audio = audio.set_channels(1).set_frame_rate(16000).set_sample_width(2)
                
                # 获取原始PCM数据
                raw_data = audio.raw_data
                
                # 初始化Opus编码器
                encoder = opuslib_next.Encoder(16000, 1, opuslib_next.APPLICATION_AUDIO)
                
                # 编码参数
                frame_duration = 60  # 60ms per frame
                frame_size = int(16000 * frame_duration / 1000)  # 960 samples/frame
                
                # 计算128KB对应的PCM数据量
                # 128KB Opus数据大约对应 8-10秒的音频（取8秒保守估计）
                first_part_duration_sec = 8
                first_part_pcm_size = first_part_duration_sec * 16000 * 2  # 8秒 * 16kHz * 2字节
                first_part_pcm_size = min(first_part_pcm_size, len(raw_data))  # 不超过总长度
                
                # 第一部分：前128KB对应的音频数据
                first_part_raw = raw_data[:first_part_pcm_size]
                first_part_datas = []
                
                for i in range(0, len(first_part_raw), frame_size * 2):
                    frame_chunk = first_part_raw[i:i + frame_size * 2]
                    
                    # 如果最后一帧不足，补零
                    if len(frame_chunk) < frame_size * 2:
                        frame_chunk += b"\x00" * (frame_size * 2 - len(frame_chunk))
                    
                    if self.conn.audio_format == "pcm":
                        frame_data = frame_chunk if isinstance(frame_chunk, bytes) else bytes(frame_chunk)
                    else:
                        # 转换为numpy数组处理
                        np_frame = np.frombuffer(frame_chunk, dtype=np.int16)
                        # 编码Opus数据
                        frame_data = encoder.encode(np_frame.tobytes(), frame_size)
                    
                    first_part_datas.append(frame_data)
                
                # 立即发送第一部分（快速启动播放）
                if first_part_datas:
                    logger.bind(tag=TAG).info(f"发送第一部分音频数据，包含 {len(first_part_datas)} 帧 (约{first_part_duration_sec}秒)")
                    self.tts_audio_queue.put(
                        (sentence_type, first_part_datas, content_detail)
                    )
                
                # 检查是否有剩余音频需要发送
                if first_part_pcm_size < len(raw_data):
                    # 第二部分：剩余的音频数据
                    remaining_raw = raw_data[first_part_pcm_size:]
                    remaining_datas = []
                    
                    for i in range(0, len(remaining_raw), frame_size * 2):
                        if self.conn.client_abort:
                            logger.bind(tag=TAG).info("收到打断信息，终止剩余音频处理")
                            break
                            
                        frame_chunk = remaining_raw[i:i + frame_size * 2]
                        
                        # 如果最后一帧不足，补零
                        if len(frame_chunk) < frame_size * 2:
                            frame_chunk += b"\x00" * (frame_size * 2 - len(frame_chunk))
                        
                        if self.conn.audio_format == "pcm":
                            frame_data = frame_chunk if isinstance(frame_chunk, bytes) else bytes(frame_chunk)
                        else:
                            # 转换为numpy数组处理
                            np_frame = np.frombuffer(frame_chunk, dtype=np.int16)
                            # 编码Opus数据
                            frame_data = encoder.encode(np_frame.tobytes(), frame_size)
                        
                        remaining_datas.append(frame_data)
                    
                    # 发送第二部分（剩余音频）
                    if remaining_datas and not self.conn.client_abort:
                        logger.bind(tag=TAG).info(f"发送第二部分音频数据，包含 {len(remaining_datas)} 帧")
                        self.tts_audio_queue.put(
                            (sentence_type, remaining_datas, content_detail)
                        )
                
                total_frames = len(first_part_datas) + (len(remaining_datas) if 'remaining_datas' in locals() else 0)
                logger.bind(tag=TAG).info(f"音乐文件两部分发送完成，总共 {total_frames} 帧")
                
            except Exception as e:
                logger.bind(tag=TAG).error(f"流式音频处理失败: {str(e)}")
        
        # 在单独线程中执行流式处理
        streaming_thread = threading.Thread(target=streaming_process, daemon=True)
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
