import json
import time
import math
from core.handle.abortHandle import handleAbortMessage
from core.handle.helloHandle import handleHelloMessage
from core.handle.mcpHandle import handle_mcp_message
from core.utils.util import remove_punctuation_and_length, filter_sensitive_info
from core.handle.receiveAudioHandle import startToChat, handleAudioMessage
from core.handle.sendAudioHandle import send_stt_message, send_tts_message
from core.handle.iotHandle import handleIotDescriptors, handleIotStatus
from core.handle.reportHandle import enqueue_asr_report
from core.utils.nmea_parser import parse_nmea_message
import asyncio
from core.providers.tts.dto.dto import ContentType
from core.utils.dialogue import Message
from plugins_func.register import Action

TAG = __name__

# 位置信息处理控制
class LocationProcessor:
    def __init__(self):
        self.last_process_time = {}
        self.last_location = {}
        self.min_interval = 10.0  # 最小处理间隔（秒）
        self.min_distance = 5.0  # 最小距离变化（米）
    
    def should_process_location(self, client_id: str, lat: float, lon: float) -> bool:
        """判断是否应该处理位置信息"""
        current_time = time.time()
        
        # 检查是否是首次位置信息
        last_pos = self.last_location.get(client_id)
        if last_pos is None:
            # 首次位置信息，必须处理（用于路线规划）
            self.last_process_time[client_id] = current_time
            self.last_location[client_id] = {'lat': lat, 'lon': lon}
            print(f"首次位置信息，进行路线规划：经纬度{self.last_location.get(client_id)}")
            return True
        
        # 非首次位置信息，进行时间间隔和距离变化判断
        # 检查时间间隔
        last_time = self.last_process_time.get(client_id, 0)
        if current_time - last_time < self.min_interval:
            return False
        
        # 检查距离变化
        distance = self._calculate_distance(lat, lon, last_pos['lat'], last_pos['lon'])
        print(f"距离变化：{distance:.2f}米")
        if distance < self.min_distance:
            return False
        
        # 更新记录
        self.last_process_time[client_id] = current_time
        self.last_location[client_id] = {'lat': lat, 'lon': lon}
        print(f"位置更新：经纬度{self.last_location.get(client_id)}")
        return True
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """计算两点间距离（米）"""
        R = 6371000.0  # 地球半径（米）
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

# 全局位置处理器实例
location_processor = LocationProcessor()


async def handleTextMessage(conn, message):
    """处理文本消息"""
    try:
        msg_json = json.loads(message)
        if isinstance(msg_json, int):
            conn.logger.bind(tag=TAG).info(f"收到文本消息：{message}")
            await conn.websocket.send(message)
            return
            
        # 对于所有类型的消息（除了hello和导航nmea），都先检查是否需要立即中断当前播放
        # 这确保用户的任何输入都能立即停止音频播放
        if msg_json["type"] != "hello" and msg_json["type"] != "abort" and msg_json["type"] !="nmea":
            # 检查是否有音频正在播放，如果有则立即中断
            if (hasattr(conn, 'client_is_speaking') and conn.client_is_speaking) or \
               (hasattr(conn, 'tts') and conn.tts and not conn.tts.tts_audio_queue.empty()):
                conn.logger.bind(tag=TAG).info(f"收到3非消息：{message}")
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

                    # 对于唤醒词和普通文本，都需要先打断当前播放
                    await handleAbortMessage(conn)
                    # 同时停止电台播放
                    try:
                        from core.utils.radio_streamer import stop_radio_stream
                        await stop_radio_stream(conn.websocket)
                    except Exception:
                        pass

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
        elif msg_json["type"] == "nmea":
            conn.logger.bind(tag=TAG).info(f"收到NMEA消息：{filter_sensitive_info(msg_json)}")
            
            # 检查是否正在播放语音，避免阻塞
            if conn.client_is_speaking:
                conn.logger.bind(tag=TAG).info("正在播放语音，跳过NMEA消息处理")
                return
            
            # 解析NMEA数据提取经纬度
            try:
                coords = parse_nmea_message(msg_json)
                if coords:
                    lat, lon = coords
                    conn.logger.bind(tag=TAG).info(f"从NMEA解析到坐标: 纬度={lat}, 经度={lon}")
                    
                    # 使用选择性处理机制
                    client_id = getattr(conn, 'client_id', 'default')
                    should_process = location_processor.should_process_location(client_id, lat, lon)
                    
                    if not should_process:
                        conn.logger.bind(tag=TAG).info(f"NMEA位置信息跳过处理：时间间隔或距离变化不足")
                        return
                    
                    # # 判断是否为首次位置信息
                    # is_first_location = client_id not in location_processor.last_location or \
                    #                   len(location_processor.last_location) == 1
                    #
                    # if is_first_location:
                    #     conn.logger.bind(tag=TAG).info(f"处理首次NMEA位置信息，进行路线规划：纬度={lat:.6f}, 经度={lon:.6f}")
                    # else:
                    #     conn.logger.bind(tag=TAG).info(f"处理NMEA位置更新：纬度={lat:.6f}, 经度={lon:.6f}")
                    
                    # 调用导航更新函数
                    func_item = conn.func_handler.get_function("navigate_to")
                    if func_item:
                        # 如果是“等待位置”的首次更新，且已记录目的地，则同时传入目的地以立即规划路线；
                        # 否则仅传入经纬度，让navigate_to内部按状态处理（避免重复启动导航）。
                        from plugins_func.functions.navigation import _get_navigation_state
                        nav_state = _get_navigation_state(conn)
                        if nav_state and nav_state.get("waiting_for_location") and nav_state.get("destination"):
                            dest = nav_state["destination"]
                            destination_str = f"{dest['lng']},{dest['lat']}"
                            result = await asyncio.to_thread(
                                func_item.func, conn,
                                destination=destination_str,
                                lat=lat, lon=lon
                            )
                        else:
                            result = await asyncio.to_thread(func_item.func, conn, lat=lat, lon=lon)
                        
                        if result and result.action == Action.RESPONSE:
                            text = result.response
                            if text:
                                conn.client_is_speaking = True
                                await send_tts_message(conn, "start")
                                conn.tts.tts_one_sentence(conn, ContentType.TEXT, content_detail=text, navigate_info=True)
                                conn.dialogue.put(Message(role="assistant", content=text))
                        elif result and result.action == Action.REQLLM:
                            # 理论上导航更新不需要REQLLM，这里做兜底处理
                            text = result.result
                            if text:
                                conn.dialogue.put(Message(role="tool", content=text))
                    else:
                        conn.logger.bind(tag=TAG).warning("navigate_to 未注册或不可用")
                else:
                    conn.logger.bind(tag=TAG).warning("无法从NMEA数据中解析出有效坐标")
            except Exception as e:
                conn.logger.bind(tag=TAG).error(f"处理NMEA消息失败: {e}")
        else:
            conn.logger.bind(tag=TAG).error(f"收到未知类型消息：{message}")
    except json.JSONDecodeError:
        await conn.websocket.send(message)
