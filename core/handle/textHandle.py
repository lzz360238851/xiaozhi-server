import json
from core.handle.abortHandle import handleAbortMessage
from core.handle.helloHandle import handleHelloMessage
from core.handle.mcpHandle import handle_mcp_message
from core.utils.util import remove_punctuation_and_length, filter_sensitive_info
from core.handle.receiveAudioHandle import startToChat, handleAudioMessage
from core.handle.sendAudioHandle import send_stt_message, send_tts_message
from core.handle.iotHandle import handleIotDescriptors, handleIotStatus
from core.handle.reportHandle import enqueue_asr_report
import asyncio
from core.providers.tts.dto.dto import ContentType
from core.utils.dialogue import Message
from plugins_func.register import Action

TAG = __name__


async def handleTextMessage(conn, message):
    """处理文本消息"""
    try:
        msg_json = json.loads(message)
        if isinstance(msg_json, int):
            conn.logger.bind(tag=TAG).info(f"收到文本消息：{message}")
            await conn.websocket.send(message)
            return
            
        # 对于所有类型的消息（除了hello），都先检查是否需要立即中断当前播放
        # 这确保用户的任何输入都能立即停止音频播放
        if msg_json["type"] != "hello" and msg_json["type"] != "abort":
            # 检查是否有音频正在播放，如果有则立即中断
            if (hasattr(conn, 'client_is_speaking') and conn.client_is_speaking) or \
               (hasattr(conn, 'tts') and conn.tts and not conn.tts.tts_audio_queue.empty()):
                conn.logger.bind(tag=TAG).info(f"检测到用户输入，立即中断当前音频播放")
                await handleAbortMessage(conn)
        
        if msg_json["type"] == "hello":
            conn.logger.bind(tag=TAG).info(f"收到hello消息：{message}")
            await handleHelloMessage(conn, msg_json)
        elif msg_json["type"] == "abort":
            conn.logger.bind(tag=TAG).info(f"收到abort消息：{message}")
            await handleAbortMessage(conn)
        elif msg_json["type"] == "listen":
            conn.logger.bind(tag=TAG).info(f"收到listen消息：{message}")
            if "mode" in msg_json:
                conn.client_listen_mode = msg_json["mode"]
                conn.logger.bind(tag=TAG).debug(
                    f"客户端拾音模式：{conn.client_listen_mode}"
                )
            if msg_json["state"] == "start":
                # 一旦开始拾音，立即打断当前播放
                await handleAbortMessage(conn)
                # 同时停止电台播放
                try:
                    from core.utils.radio_streamer import stop_radio_stream
                    await stop_radio_stream(conn.websocket)
                except Exception:
                    pass
                conn.client_have_voice = True
                conn.client_voice_stop = False
            elif msg_json["state"] == "stop":
                conn.client_have_voice = True
                conn.client_voice_stop = True
                if len(conn.asr_audio) > 0:
                    await handleAudioMessage(conn, b"")
            elif msg_json["state"] == "detect":
                # 前端检测到用户在说话(或准备说话)，立即触发打断
                await handleAbortMessage(conn)
                # 同时停止电台播放
                try:
                    from core.utils.radio_streamer import stop_radio_stream
                    await stop_radio_stream(conn.websocket)
                except Exception:
                    pass
                conn.client_have_voice = False
                conn.asr_audio.clear()
                if "text" in msg_json:
                    original_text = msg_json["text"]  # 保留原始文本
                    filtered_len, filtered_text = remove_punctuation_and_length(
                        original_text
                    )

                    # 识别是否是唤醒词
                    is_wakeup_words = filtered_text in conn.config.get("wakeup_words")
                    # 是否开启唤醒词回复
                    enable_greeting = conn.config.get("enable_greeting", True)

                    if is_wakeup_words and not enable_greeting:
                        # 如果是唤醒词，且关闭了唤醒词回复，就不用回答
                        await send_stt_message(conn, original_text)
                        await send_tts_message(conn, "stop", None)
                        conn.client_is_speaking = False
                    elif is_wakeup_words:
                        conn.just_woken_up = True
                        # 注意：audio_generation 的递增由 handleAbortMessage 统一处理，避免重复递增
                        # 上报纯文字数据（复用ASR上报功能，但不提供音频数据）
                        enqueue_asr_report(conn, "嘿，你好呀", [])
                        await startToChat(conn, "嘿，你好呀")
                    else:
                        # 注意：audio_generation 的递增由 handleAbortMessage 统一处理，避免重复递增
                        # 上报纯文字数据（复用ASR上报功能，但不提供音频数据）
                        enqueue_asr_report(conn, original_text, [])
                        # 否则需要LLM对文字内容进行答复
                        await startToChat(conn, original_text)
        elif msg_json["type"] == "iot":
            conn.logger.bind(tag=TAG).info(f"收到iot消息：{message}")
            if "descriptors" in msg_json:
                asyncio.create_task(handleIotDescriptors(conn, msg_json["descriptors"]))
            if "states" in msg_json:
                asyncio.create_task(handleIotStatus(conn, msg_json["states"]))
        elif msg_json["type"] == "mcp":
            conn.logger.bind(tag=TAG).info(f"收到mcp消息：{message}")
            if "payload" in msg_json:
                asyncio.create_task(
                    handle_mcp_message(conn, conn.mcp_client, msg_json["payload"])
                )
        elif msg_json["type"] == "location":
            conn.logger.bind(tag=TAG).info(f"收到位置消息：{filter_sensitive_info(msg_json)}")
            # 兼容多种字段命名与嵌套
            data = msg_json.get("content", msg_json)
            lat = data.get("lat") or data.get("latitude")
            lon = data.get("lon") or data.get("lng") or data.get("longitude")
            # 解析为浮点数
            try:
                if isinstance(lat, str):
                    lat = float(lat.strip())
                if isinstance(lon, str):
                    lon = float(lon.strip())
                lat = float(lat)
                lon = float(lon)
            except Exception:
                await conn.websocket.send(
                    json.dumps(
                        {
                            "type": "location",
                            "status": "error",
                            "message": "经纬度格式错误或缺失，应包含 lat、lon",
                        }
                    )
                )
                return
            # 调用导航更新函数
            try:
                func_item = conn.func_handler.get_function("navigation_update")
                if not func_item:
                    conn.logger.bind(tag=TAG).warning("navigation_update 未注册或不可用")
                    return
                # 可能包含网络IO，放入线程池执行避免阻塞事件循环
                result = await asyncio.to_thread(func_item.func, conn, lat=lat, lon=lon)
                if result and result.action == Action.RESPONSE:
                    text = result.response
                    if text:
                        conn.tts.tts_one_sentence(conn, ContentType.TEXT, content_detail=text)
                        conn.dialogue.put(Message(role="assistant", content=text))
                elif result and result.action == Action.REQLLM:
                    # 理论上导航更新不需要REQLLM，这里做兜底处理
                    text = result.result
                    if text:
                        conn.dialogue.put(Message(role="tool", content=text))
            except Exception as e:
                conn.logger.bind(tag=TAG).error(f"处理位置消息失败: {e}")
                await conn.websocket.send(
                    json.dumps(
                        {
                            "type": "location",
                            "status": "error",
                            "message": f"处理位置消息失败: {str(e)}",
                        }
                    )
                )
        elif msg_json["type"] == "server":
            # 记录日志时过滤敏感信息
            conn.logger.bind(tag=TAG).info(
                f"收到服务器消息：{filter_sensitive_info(msg_json)}"
            )
            # 如果配置是从API读取的，则需要验证secret
            if not conn.read_config_from_api:
                return
            # 获取post请求的secret
            post_secret = msg_json.get("content", {}).get("secret", "")
            secret = conn.config["manager-api"].get("secret", "")
            # 如果secret不匹配，则返回
            if post_secret != secret:
                await conn.websocket.send(
                    json.dumps(
                        {
                            "type": "server",
                            "status": "error",
                            "message": "服务器密钥验证失败",
                        }
                    )
                )
                return
            # 动态更新配置
            if msg_json["action"] == "update_config":
                try:
                    # 更新WebSocketServer的配置
                    if not conn.server:
                        await conn.websocket.send(
                            json.dumps(
                                {
                                    "type": "server",
                                    "status": "error",
                                    "message": "无法获取服务器实例",
                                    "content": {"action": "update_config"},
                                }
                            )
                        )
                        return

                    if not await conn.server.update_config():
                        await conn.websocket.send(
                            json.dumps(
                                {
                                    "type": "server",
                                    "status": "error",
                                    "message": "更新服务器配置失败",
                                    "content": {"action": "update_config"},
                                }
                            )
                        )
                        return

                    # 发送成功响应
                    await conn.websocket.send(
                        json.dumps(
                            {
                                "type": "server",
                                "status": "success",
                                "message": "配置更新成功",
                                "content": {"action": "update_config"},
                            }
                        )
                    )
                except Exception as e:
                    conn.logger.bind(tag=TAG).error(f"更新配置失败: {str(e)}")
                    await conn.websocket.send(
                        json.dumps(
                            {
                                "type": "server",
                                "status": "error",
                                "message": f"更新配置失败: {str(e)}",
                                "content": {"action": "update_config"},
                            }
                        )
                    )
            # 重启服务器
            elif msg_json["action"] == "restart":
                await conn.handle_restart(msg_json)
        else:
            conn.logger.bind(tag=TAG).error(f"收到未知类型消息：{message}")
    except json.JSONDecodeError:
        await conn.websocket.send(message)
