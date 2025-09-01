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
    
    # 在发送音频数据前，先发送音频代次控制消息
    # 修改为测试系统能识别的audio消息类型，避免"未知消息类型"错误
    # 注释掉generation_control消息发送，避免客户端播放问题
    # if snapshot_generation is not None:
    #     audio_generation_message = {
    #         "type": "audio",
    #         "action": "generation_control",
    #         "generation": snapshot_generation,
    #         "session_id": conn.session_id
    #     }
    #     await conn.websocket.send(json.dumps(audio_generation_message))
    #     conn.logger.bind(tag=TAG).info(f"发送音频代次控制消息: generation={snapshot_generation}")
        
    # 流控参数优化 - 使用更精确的时间控制
    frame_duration_ms = 60  # 帧时长（毫秒），匹配 Opus 编码
    frame_duration_s = frame_duration_ms / 1000.0  # 转换为秒，提高精度
    start_time = time.perf_counter()
    frame_count = 0  # 使用帧计数而非累积时间，避免误差累积
    last_reset_time = time.perf_counter()  # 记录最后的重置时间

    # 增强缓冲机制：确保至少缓冲5个音频包，提高播放流畅性
    min_buffer_frames = 5  # 增加缓冲帧数
    
    # 如果音频包数量很少，全部作为缓冲
    buffer_frames = min(min_buffer_frames, len(audios))
    
    # 对于短音频（小于等于5帧），直接缓冲所有帧
    if len(audios) <= min_buffer_frames:
        buffer_frames = len(audios)
        remaining_audios = []
    else:
        # 对于长音频，先缓冲指定数量的帧
        buffer_frames = min_buffer_frames
        remaining_audios = audios[buffer_frames:]
    
    frames_sent = 0
    bytes_sent = 0
    # 发送初始缓冲帧 - 添加适当延迟确保客户端能正确接收
    conn.logger.bind(tag=TAG).info(f"开始发送初始缓冲帧: {buffer_frames}帧，剩余：{len(remaining_audios)}帧 (snapshot={snapshot_generation}, current={getattr(conn, 'audio_generation', 0)})")
    for i in range(buffer_frames):
        # 每帧都检查中断状态，确保快速响应
        if snapshot_generation is not None and snapshot_generation != getattr(conn, "audio_generation", 0):
            conn.logger.bind(tag=TAG).info(f"音频代次不匹配，停止发送缓冲帧 (snapshot: {snapshot_generation}, current: {getattr(conn, 'audio_generation', 0)}), 已发送帧数: {frames_sent}, 字节: {bytes_sent}")
            return frames_sent
        
        if conn.client_abort:
            conn.logger.bind(tag=TAG).info("客户端中断，停止发送缓冲帧")
            return frames_sent
            
        packet = audios[i]
        await conn.websocket.send(packet)
        frames_sent += 1
        frame_count += 1
        try:
            bytes_sent += len(packet)
        except Exception:
            pass
        
        conn.logger.bind(tag=TAG).info(f"发送初始缓冲帧 {i+1}/{buffer_frames}")
        
        # 在缓冲帧之间添加延迟，确保客户端能正确接收
        if i < buffer_frames - 1:  # 最后一帧不需要延迟
            await asyncio.sleep(0.1)  # 增加到100ms延迟
            conn.logger.bind(tag=TAG).info(f"缓冲帧间延迟100ms完成")

    # 播放剩余音频帧 - 使用精确的时间控制
    for i, opus_packet in enumerate(remaining_audios):
        # 每帧都检查中断状态，确保立即响应用户输入
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

        # 使用帧计数计算精确的预期发送时间，避免累积误差
        expected_time = start_time + (frame_count * frame_duration_s)
        current_time = time.perf_counter()
        delay = expected_time - current_time
        
        # 添加最小延迟保护，确保不会发送过快
        min_delay = 0.001  # 1ms最小延迟
        if delay > min_delay:
            await asyncio.sleep(delay)
        elif delay < -frame_duration_s:  # 如果延迟过大，重置时间基准
            start_time = current_time
            frame_count = 0

        await conn.websocket.send(opus_packet)
        frames_sent += 1
        frame_count += 1
        try:
            bytes_sent += len(opus_packet)
        except Exception:
            pass

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
