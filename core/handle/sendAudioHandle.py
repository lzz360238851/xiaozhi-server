import json
import asyncio
import time
from core.providers.tts.dto.dto import SentenceType
from core.utils.util import get_string_no_punctuation_or_emoji, analyze_emotion
from loguru import logger

TAG = __name__

emoji_map = {
    "neutral": "😶",
    "happy": "🙂",
    "laughing": "😆",
    "funny": "😂",
    "sad": "😔",
    "angry": "😠",
    "crying": "😭",
    "loving": "😍",
    "embarrassed": "😳",
    "surprised": "😲",
    "shocked": "😱",
    "thinking": "🤔",
    "winking": "😉",
    "cool": "😎",
    "relaxed": "😌",
    "delicious": "🤤",
    "kissy": "😘",
    "confident": "😏",
    "sleepy": "😴",
    "silly": "😜",
    "confused": "🙄",
}


async def sendAudioMessage(conn, sentenceType, audios, text):
    # 发送句子开始消息
    conn.logger.bind(tag=TAG).info(f"发送音频消息: {sentenceType}, {text}")
    snapshot_generation = getattr(conn, "audio_generation", 0)
    conn.logger.bind(tag=TAG).info(f"发送音频消息-代次快照: {snapshot_generation}, 数据包数: {len(audios) if audios else 0}")
    if text is not None:
        emotion = analyze_emotion(text)
        emoji = emoji_map.get(emotion, "🙂")  # 默认使用笑脸
        await conn.websocket.send(
            json.dumps(
                {
                    "type": "llm",
                    "text": emoji,
                    "emotion": emotion,
                    "session_id": conn.session_id,
                }
            )
        )
    pre_buffer = False
    if conn.tts.tts_audio_first_sentence and text is not None:
        conn.logger.bind(tag=TAG).info(f"发送第一段语音: {text}")
        conn.tts.tts_audio_first_sentence = False
        pre_buffer = True

    await send_tts_message(conn, "sentence_start", text)

    frames_sent = await sendAudio(conn, audios, pre_buffer, snapshot_generation=snapshot_generation)

    await send_tts_message(conn, "sentence_end", text)

    conn.logger.bind(tag=TAG).info(f"音频消息发送完成: 发送帧数={frames_sent}, 代次=({snapshot_generation}->{getattr(conn, 'audio_generation', 0)})")

    # 发送结束消息（如果是最后一个文本）
    if conn.llm_finish_task and sentenceType == SentenceType.LAST:
        await send_tts_message(conn, "stop", None)
        conn.client_is_speaking = False
        if conn.close_after_chat:
            await conn.close()


# 播放音频
async def sendAudio(conn, audios, pre_buffer=True, snapshot_generation=None):
    if audios is None or len(audios) == 0:
        return 0
        
    # 流控参数优化
    frame_duration = 60  # 帧时长（毫秒），匹配 Opus 编码
    start_time = time.perf_counter()
    play_position = 0
    last_reset_time = time.perf_counter()  # 记录最后的重置时间

    # 增强缓冲机制：确保至少缓冲3个音频包，而仅依赖于 pre_buffer 标志
    min_buffer_frames = 3
    
    # 如果音频包数量很少，全部作为缓冲
    buffer_frames = min(min_buffer_frames, len(audios))
    
    # 对于短音频（小于等于3帧），直接缓冲所有帧
    if len(audios) <= min_buffer_frames:
        buffer_frames = len(audios)
        remaining_audios = []
    else:
        # 对于长音频，先缓冲指定数量的帧
        buffer_frames = min_buffer_frames
        remaining_audios = audios[buffer_frames:]
    
    frames_sent = 0
    bytes_sent = 0
    # 发送初始缓冲帧
    conn.logger.bind(tag=TAG).info(f"发送音频缓冲帧：{buffer_frames}帧，剩余：{len(remaining_audios)}帧 (snapshot={snapshot_generation}, current={getattr(conn, 'audio_generation', 0)})")
    for i in range(buffer_frames):
        # 检查音频代次，确保不发送过期音频
        if snapshot_generation is not None and snapshot_generation != getattr(conn, "audio_generation", 0):
            conn.logger.bind(tag=TAG).info(f"音频代次不匹配，停止发送缓冲帧 (snapshot: {snapshot_generation}, current: {getattr(conn, 'audio_generation', 0)}), 已发送帧数: {frames_sent}, 字节: {bytes_sent}")
            return frames_sent
        
        if conn.client_abort:
            conn.logger.bind(tag=TAG).info("客户端中断，停止发送缓冲帧")
            return frames_sent
            
        packet = audios[i]
        await conn.websocket.send(packet)
        frames_sent += 1
        try:
            bytes_sent += len(packet)
        except Exception:
            pass
        play_position += frame_duration

    # 播放剩余音频帧
    for opus_packet in remaining_audios:
        # 优先检查音频代次，确保旧音频包被正确丢弃
        if snapshot_generation is not None and snapshot_generation != getattr(conn, "audio_generation", 0):
            conn.logger.bind(tag=TAG).info(f"音频代次不匹配，停止播放 (snapshot: {snapshot_generation}, current: {getattr(conn, 'audio_generation', 0)}), 已发送帧数: {frames_sent}, 字节: {bytes_sent}")
            break
            
        if conn.client_abort:
            conn.logger.bind(tag=TAG).info("客户端中断，停止播放")
            break

        # 每分钟重置一次计时器
        if time.perf_counter() - last_reset_time > 60:
            await conn.reset_timeout()
            last_reset_time = time.perf_counter()

        # 计算预期发送时间
        expected_time = start_time + (play_position / 1000)
        current_time = time.perf_counter()
        delay = expected_time - current_time
        if delay > 0:
            await asyncio.sleep(delay)

        await conn.websocket.send(opus_packet)
        frames_sent += 1
        try:
            bytes_sent += len(opus_packet)
        except Exception:
            pass
        play_position += frame_duration

    conn.logger.bind(tag=TAG).info(f"音频发送结束：总帧数={frames_sent}, 总字节={bytes_sent}, 代次=({snapshot_generation}->{getattr(conn, 'audio_generation', 0)})")
    return frames_sent


async def send_tts_message(conn, state, text=None):
    """发送 TTS 状态消息"""
    message = {"type": "tts", "state": state, "session_id": conn.session_id}
    if text is not None:
        message["text"] = text

    # TTS播放结束
    if state == "stop":
        # 播放提示音
        tts_notify = conn.config.get("enable_stop_tts_notify", False)
        if tts_notify:
            stop_tts_notify_voice = conn.config.get(
                "stop_tts_notify_voice", "config/assets/tts_notify.mp3"
            )
            audios, _ = conn.tts.audio_to_opus_data(stop_tts_notify_voice)
            # 获取当前音频代次快照，确保提示音也遵循代次检查
            snapshot_generation = getattr(conn, "audio_generation", 0)
            await sendAudio(conn, audios, pre_buffer=False, snapshot_generation=snapshot_generation)
        # 清除服务端讲话状态
        conn.clearSpeakStatus()

    # 发送消息到客户端
    await conn.websocket.send(json.dumps(message))


async def send_stt_message(conn, text):
    end_prompt_str = conn.config.get("end_prompt", {}).get("prompt")
    if end_prompt_str and end_prompt_str == text:
        await send_tts_message(conn, "start")
        return

    """发送 STT 状态消息"""
    stt_text = get_string_no_punctuation_or_emoji(text)
    await conn.websocket.send(
        json.dumps({"type": "stt", "text": stt_text, "session_id": conn.session_id})
    )
    conn.client_is_speaking = True
    await send_tts_message(conn, "start")
