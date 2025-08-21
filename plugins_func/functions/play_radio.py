import asyncio
import logging
import sys
import threading
from typing import Optional

# 假设这些是你自己的模块，请确保路径正确
from config.logger import setup_logging
from plugins_func.register import register_function, ToolType, ActionResponse, Action
from core.utils.radio_streamer import start_radio_stream, stop_radio_stream, client_manager
from websockets.legacy.protocol import WebSocketCommonProtocol

TAG = __name__
# 如果你的 setup_logging 函数已配置，这里可以移除 basicConfig
logger = setup_logging()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# ❗ 移除后台线程和后台事件循环。这正是导致问题的原因。
#    所有 WebSocket 相关的异步操作都必须在同一个事件循环中。
# _background_loop = None
# _loop_thread = None
# def get_or_create_event_loop(): ...

PLAY_RADIO_FUNCTION_DESC = {
    "type": "function",
    "function": {
        "name": "play_radio",
        "description": (
            "**用于处理用户的电台播放/停止请求，是播放电台的唯一方式**。\n"
            "⚠️ 优先级：当用户指令同时命中电台关键词与歌曲关键词时，必须优先调用本函数。\n"
            "触发规则：当用户的语句中出现以下任一关键词（或相似表达）时，必须调用本函数，不得调用播放歌曲函数：\n"
            "【电台】【广播】【新闻广播】【中国之声】【新闻之声】【音乐之声】【经济之声】【都市之声】【中华之声】【神州之声】【电台直播】【收听电台】等。\n"
            "支持指令：\n"
            "  - 播放/打开/切换到 指定电台（如：播放音乐之声、切换到经济之声）\n"
            "  - 停止/关闭 当前播放的电台（如：停止广播、关闭中国之声）\n"
            "默认行为：未指定电台名时，默认播放“中国之声”。\n"
            "所有表示“播放、打开、切换到、切换成”等动词必须映射为 'play'；所有表示“停止、关闭”等动词必须映射为 'stop'；不得输出其它值。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "enum": ["play", "stop"],
                    "description": (
                        "操作指令：\n"
                        "- play：播放或切换到指定电台\n"
                        "- stop：停止当前播放的电台\n"
                        "可为空，函数内部会默认为 'play'"
                    ),
                    "examples": [
                        {"用户": "播放电台", "参数": "play"},
                        {"用户": "打开中国之声", "参数": "play"},
                        {"用户": "切换到经济之声", "参数": "play"},
                        {"用户": "停止广播", "参数": "stop"},
                        {"用户": "关闭电台直播", "参数": "stop"}
                    ]
                },
                "radio_name": {
                    "type": "string",
                    "description": (
                        "需播放的电台名称，支持：\n"
                        "中国之声、经济之声、音乐之声、都市之声、中华之声、神州之声。\n"
                        "可为空，函数内部会默认为 '中国之声'。\n"
                        "若提供的名称不在支持范围内，也会自动替换为 '中国之声'。"
                    ),
                    "examples": [
                        {"用户": "播放音乐之声", "参数": "音乐之声"},
                        {"用户": "切换到经济之声", "参数": "经济之声"},
                        {"用户": "打开中华之声", "参数": "中华之声"}
                    ]
                }
            }
            # 不加 "required" 表示两个参数都可选
        }
    }
}


# 同步注册函数
@register_function("play_radio", PLAY_RADIO_FUNCTION_DESC, ToolType.SYSTEM_CTL)
def get_news_from_chinanews(conn, command: str, radio_name: str):
    if command is None or command.strip() == "":
        command="play"
    if command not in ("play", "stop"):
        command = "play"

    conn.logger.bind(tag=TAG).info("进入播放电台函数")
    websocket: Optional[WebSocketCommonProtocol] = getattr(conn, "websocket", None)
    if not websocket:
        return ActionResponse(Action.REQLLM, "无法播放电台", None)

    # ❗ 关键修改：直接获取 websocket 所属的事件循环
    #    websocket 对象的 .loop 属性就是创建它的事件循环
    #    这样可以确保 start_radio_stream 总是被调度到正确的循环中
    main_loop = websocket.loop

    if command == "play":
        # ❗ 关键修改：使用 run_coroutine_threadsafe 在主循环中调度协程
        #    run_coroutine_threadsafe 是用于从非异步线程安全地调度协程的方法
        asyncio.run_coroutine_threadsafe(start_radio_stream(websocket,radio_name), main_loop)
        # ✅ 返回静默响应，不触发 TTS
        return ActionResponse(Action.NONE, "", None)

    elif command == "stop":
        asyncio.run_coroutine_threadsafe(stop_radio_stream(websocket), main_loop)
        return ActionResponse(Action.NONE, "", None)

    else:
        return ActionResponse(Action.REQLLM, "不支持的指令", None)