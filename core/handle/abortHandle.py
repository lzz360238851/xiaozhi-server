import json
import queue
from config.logger import setup_logging
from core.handle.sendLyricsHandle import stop_lyrics_sync

TAG = __name__


async def handleAbortMessage(conn):
    conn.logger.bind(tag=TAG).info("Abort message received")
    # 设置成打断状态，会自动打断llm、tts任务
    conn.client_abort = True
    # 增加音频代次，客户端可据此丢弃旧缓冲（若前端支持）
    if hasattr(conn, "audio_generation"):
        conn.audio_generation += 1
    conn.clear_queues()
    
    # 打断歌词推送线程
    await stop_lyrics_sync(conn)
    
    # 中断当前音乐播放
    try:
        from plugins_func.functions.play_music import interrupt_current_music
        interrupt_current_music(conn)
    except Exception as e:
        conn.logger.bind(tag=TAG).error(f"中断音乐播放失败: {str(e)}")

    # 中断电台播放
    try:
        from core.utils.radio_streamer import stop_radio_stream
        await stop_radio_stream(conn.websocket)
    except Exception as e:
        conn.logger.bind(tag=TAG).error(f"中断电台播放失败: {str(e)}")

    
    # 打断客户端说话状态
    await conn.websocket.send(
        json.dumps({"type": "tts", "state": "stop", "session_id": conn.session_id})
    )
    conn.clearSpeakStatus()
    conn.logger.bind(tag=TAG).info("Abort message received-end")
