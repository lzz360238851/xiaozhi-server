import sys
import uuid
import signal
import asyncio
import threading
from aioconsole import ainput
from config.settings import load_config
from config.logger import setup_logging
from core.http_server import SimpleHttpServer
from core.utils.util import get_local_ip, check_ffmpeg_installed
from core.websocket_server import WebSocketServer

TAG = __name__
logger = setup_logging()


# =========================
# 全局后台事件循环（用于插件异步任务）
# =========================
_background_loop = None
_loop_thread = None


def get_or_create_event_loop():
    """获取或创建独立线程运行的事件循环，用于插件异步调用"""
    global _background_loop, _loop_thread
    if _background_loop is None or _background_loop.is_closed():
        _background_loop = asyncio.new_event_loop()
        _loop_thread = threading.Thread(target=_background_loop.run_forever, daemon=True)
        _loop_thread.start()
        logger.bind(tag=TAG).info("后台事件循环线程已启动")
    return _background_loop


async def wait_for_exit() -> None:
    """
    阻塞直到收到 Ctrl+C / SIGTERM
    - Unix: 使用 add_signal_handler
    - Windows: 依赖 KeyboardInterrupt
    """
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    if sys.platform != "win32":  # Unix / macOS
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
    else:
        try:
            await asyncio.Future()  # 永远等待，直到被中断
        except KeyboardInterrupt:
            pass


async def monitor_stdin():
    """监控标准输入，消费回车键（防止阻塞）"""
    while True:
        try:
            await ainput()  # 异步读取输入
        except Exception:
            break  # 安全退出


async def main():
    # 检查 FFmpeg 是否安装
    check_ffmpeg_installed()

    # 加载配置
    config = load_config()

    # 设置 auth_key（JWT 认证用）
    auth_key = config.get("manager-api", {}).get("secret", "")
    if not auth_key or len(auth_key) == 0 or "你" in auth_key:
        auth_key = str(uuid.uuid4().hex)
    config["server"]["auth_key"] = auth_key

    # 启动后台事件循环（关键！用于插件异步任务）
    get_or_create_event_loop()

    # 创建任务
    stdin_task = asyncio.create_task(monitor_stdin())
    ws_server = WebSocketServer(config)
    ws_task = asyncio.create_task(ws_server.start())

    ota_server = SimpleHttpServer(config)
    ota_task = asyncio.create_task(ota_server.start())

    # 获取端口信息
    port = int(config["server"].get("http_port", 8003))
    websocket_port = int(config["server"].get("port", 8000))

    read_config_from_api = config.get("read_config_from_api", False)
    if not read_config_from_api:
        logger.bind(tag=TAG).info(
            "OTA接口是\t\thttp://{}:{}/xiaozhi/ota/",
            get_local_ip(),
            port,
        )
    logger.bind(tag=TAG).info(
        "视觉分析接口是\thttp://{}:{}/mcp/vision/explain",
        get_local_ip(),
        port,
    )
    logger.bind(tag=TAG).info(
        "Websocket地址是\tws://{}:{}/xiaozhi/v1/",
        get_local_ip(),
        websocket_port,
    )
    logger.bind(tag=TAG).info("=======上面的地址是websocket协议地址，请勿用浏览器访问=======")
    logger.bind(tag=TAG).info("如想测试websocket请用谷歌浏览器打开test目录下的test_page.html")
    logger.bind(tag=TAG).info("=============================================================\n")

    try:
        await wait_for_exit()  # 等待退出信号
    except asyncio.CancelledError:
        logger.bind(tag=TAG).info("收到取消信号，开始清理...")
    finally:
        # 取消所有任务
        stdin_task.cancel()
        ws_task.cancel()
        ota_task.cancel()

        # 等待任务清理完成（带超时）
        await asyncio.wait(
            [stdin_task, ws_task, ota_task],
            timeout=3.0,
            return_when=asyncio.ALL_COMPLETED,
        )

        # 停止后台事件循环
        global _background_loop
        if _background_loop and not _background_loop.is_closed():
            _background_loop.call_soon_threadsafe(_background_loop.stop)
            logger.bind(tag=TAG).info("后台事件循环已停止")

        print("服务器已关闭，程序退出。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("手动中断，程序终止。")