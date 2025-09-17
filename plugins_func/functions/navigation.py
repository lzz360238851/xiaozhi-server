import time
import math
import json
import asyncio
import threading
from typing import Dict, Any, List, Optional, Tuple

import requests
from plugins_func.register import register_function, ToolType, ActionResponse, Action
from config.logger import setup_logging
from core.utils.nmea_parser import create_nmea_control_message

TAG = __name__
logger = setup_logging()

NAV_FUNCTION_DESC = {
    "type": "function",
    "function": {
        "name": "navigate_to",
        "description": "导航功能：开始导航到目的地或更新当前位置。当提供destination时开始导航；当只提供lat和lon时更新位置。",
        "parameters": {
            "type": "object",
            "properties": {
                "destination": {"type": "string", "description": "目的地名称或经纬度(如: 116.481499,39.990475)，可选"},
                "mode": {"type": "string", "description": "出行方式: driving/walking/bicycling，默认driving"},
                "origin": {"type": "string", "description": "起点经纬度: lng,lat，可选"},
                "lat": {"type": "number", "description": "当前纬度，用于位置更新"},
                "lon": {"type": "number", "description": "当前经度，用于位置更新"}
            },
            "required": []
        }
    }
}

STOP_FUNCTION_DESC = {
    "type": "function",
    "function": {
        "name": "navigation_stop",
        "description": "停止当前导航并清理会话状态。",
        "parameters": {"type": "object", "properties": {}, "required": []}
    }
}


def _get_nav_cfg(conn) -> Dict[str, Any]:
    cfg = conn.config.get("plugins", {}).get("navigate_to", {})
    return {
        "api_key": cfg.get("api_key"),
        "mode": cfg.get("mode", "driving"),
        "turn_distance": int(cfg.get("turn_distance", 80)),
        "arrival_distance": int(cfg.get("arrival_distance", 30)),
        "api_base": cfg.get("api_base", "https://restapi.amap.com")
    }


def _parse_lnglat(s: str) -> Optional[Tuple[float, float]]:
    try:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) != 2:
            return None
        lng = float(parts[0])
        lat = float(parts[1])
        return lng, lat
    except Exception:
        return None


def _haversine(lon1, lat1, lon2, lat2):
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _amap_geocode(api_base: str, key: str, address: str) -> Optional[Tuple[float, float, str]]:
    url = f"{api_base}/v3/geocode/geo"
    params = {"key": key, "address": address}
    r = requests.get(url, params=params, timeout=8)
    data = r.json()
    if data.get("status") == "1" and data.get("geocodes"):
        gc = data["geocodes"][0]
        loc = gc.get("location", "")
        name = gc.get("formatted_address", address)
        lnglat = _parse_lnglat(loc)
        if lnglat:
            return lnglat[0], lnglat[1], name
    return None


def _amap_direction(api_base: str, key: str, mode: str, origin: str, destination: str) -> Optional[Dict[str, Any]]:
    # mode: driving/walking/bicycling
    if mode == "walking":
        url = f"{api_base}/v3/direction/walking"
    elif mode == "bicycling":
        url = f"{api_base}/v4/direction/bicycling"
    else:
        url = f"{api_base}/v3/direction/driving"
    params = {"key": key, "origin": origin, "destination": destination}
    r = requests.get(url, params=params, timeout=10)
    data = r.json()
    # 兼容不同版本返回
    if data.get("status") != "1":
        return None
    return data


def _extract_steps_from_route(data: Dict[str, Any], mode: str) -> Tuple[List[Dict[str, Any]], int, int]:
    steps: List[Dict[str, Any]] = []
    total_distance = 0
    total_duration = 0

    try:
        if mode == "bicycling":
            # v4 bicycling: data["data"]["paths"][0]["steps"]
            path = data.get("data", {}).get("paths", [{}])[0]
            total_distance = int(float(path.get("distance", 0)))
            total_duration = int(float(path.get("duration", 0)))
            for st in path.get("steps", []):
                poly = st.get("polyline", "")
                instruction = st.get("instruction", "直行")
                end = poly.split(";")[-1]
                lnglat = _parse_lnglat(end) if end else None
                if not lnglat:
                    continue
                steps.append({
                    "instruction": instruction,
                    "end_lng": lnglat[0],
                    "end_lat": lnglat[1],
                    "distance": int(float(st.get("distance", 0)))
                })
        else:
            # driving/walking: data["route"]["paths"][0]["steps"]
            paths = data.get("route", {}).get("paths", [])
            if not paths:
                return steps, total_distance, total_duration
            path = paths[0]
            total_distance = int(float(path.get("distance", 0)))
            total_duration = int(float(path.get("duration", 0)))
            for st in path.get("steps", []):
                poly = st.get("polyline", "")
                instruction = st.get("instruction", "直行")
                end = poly.split(";")[-1]
                lnglat = _parse_lnglat(end) if end else None
                if not lnglat:
                    continue
                steps.append({
                    "instruction": instruction,
                    "end_lng": lnglat[0],
                    "end_lat": lnglat[1],
                    "distance": int(float(st.get("distance", 0)))
                })
    except Exception as e:
        logger.bind(tag=TAG).error(f"解析路线步骤失败: {e}")
    
    return steps, total_distance, total_duration


# 导航状态存储 - 每个会话独立
NAVIGATION_SESSIONS = {}


def _get_navigation_state(conn):
    """获取当前连接的导航状态"""
    client_id = getattr(conn, 'client_id', 'default')
    return NAVIGATION_SESSIONS.get(client_id)


def _set_navigation_state(conn, state):
    """设置当前连接的导航状态"""
    client_id = getattr(conn, 'client_id', 'default')
    NAVIGATION_SESSIONS[client_id] = state


def _clear_navigation_state(conn):
    """清除当前连接的导航状态"""
    client_id = getattr(conn, 'client_id', 'default')
    NAVIGATION_SESSIONS.pop(client_id, None)


def cleanup_navigation_on_connect(conn):
    """连接建立时清理可能的残留导航状态"""
    try:
        client_id = getattr(conn, 'client_id', 'default')
        if client_id in NAVIGATION_SESSIONS:
            logger.bind(tag=TAG).info(f"检测到客户端 {client_id} 存在残留导航状态，正在清理")
            # 发送停止命令确保客户端停止发送位置信息
            _send_nmea_control_command(conn, "stop")
            # 清理导航状态
            _clear_navigation_state(conn)
            logger.bind(tag=TAG).info(f"已清理客户端 {client_id} 的残留导航状态")
    except Exception as e:
        logger.bind(tag=TAG).error(f"清理残留导航状态失败: {e}")


def cleanup_navigation_on_disconnect(conn):
    """连接断开时清理导航状态并发送停止命令"""
    try:
        client_id = getattr(conn, 'client_id', 'default')
        nav_state = _get_navigation_state(conn)
        if nav_state:
            logger.bind(tag=TAG).info(f"客户端 {client_id} 断开连接，正在停止导航")
            # 发送停止命令
            _send_nmea_control_command(conn, "stop")
            # 清理导航状态
            _clear_navigation_state(conn)
            logger.bind(tag=TAG).info(f"已为断开的客户端 {client_id} 停止导航")
    except Exception as e:
        logger.bind(tag=TAG).error(f"断开连接时清理导航状态失败: {e}")


async def _send_nmea_control_command_async(conn, action: str, rate_hz: int = 1):
    """异步发送NMEA控制命令给设备端"""
    try:
        client_id = getattr(conn, 'client_id', 'default')
        nmea_control_msg = create_nmea_control_message(client_id, action, rate_hz)
        
        # 直接通过WebSocket发送
        if hasattr(conn, 'websocket') and conn.websocket:
            await conn.websocket.send(json.dumps(nmea_control_msg))
            logger.bind(tag=TAG).info(f"发送NMEA控制命令: {action}, 频率: {rate_hz}Hz")
        else:
            logger.bind(tag=TAG).error("无法发送NMEA控制命令：WebSocket连接不可用")
            
    except Exception as e:
        logger.bind(tag=TAG).error(f"发送NMEA控制命令失败: {e}")


def _send_nmea_control_command(conn, action: str, rate_hz: int = 1):
    """发送NMEA控制命令给设备端（使用后台事件循环）"""
    try:
        # 导入后台事件循环函数
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from app import get_or_create_event_loop
        
        # 获取后台事件循环
        background_loop = get_or_create_event_loop()
        
        # 在后台事件循环中执行异步函数
        asyncio.run_coroutine_threadsafe(
            _send_nmea_control_command_async(conn, action, rate_hz),
            background_loop
        )
        
        logger.bind(tag=TAG).info(f"已提交NMEA控制命令到后台事件循环: {action}")
        
    except Exception as e:
        logger.bind(tag=TAG).error(f"发送NMEA控制命令失败: {e}")


@register_function("navigate_to", NAV_FUNCTION_DESC, ToolType.SYSTEM_CTL)
def navigate_to(conn, destination: str = None, mode: str = "driving", origin: str = None, lat: float = None, lon: float = None):
    """导航功能：开始导航到目的地或更新当前位置"""
    logger.bind(tag=TAG).info(f"进入导航:")
    try:
        # 判断是导航还是位置更新
        if destination:
            # 开始导航逻辑
            cfg = _get_nav_cfg(conn)
            api_key = cfg.get("api_key")
            if not api_key:
                return ActionResponse(Action.RESPONSE, None, "导航功能需要配置高德地图API密钥")

            api_base = cfg["api_base"]
            mode = mode or cfg["mode"]

            # 解析目的地
            dest_lnglat = _parse_lnglat(destination)
            if not dest_lnglat:
                # 地址解析
                geo_result = _amap_geocode(api_base, api_key, destination)
                if not geo_result:
                    return ActionResponse(Action.RESPONSE, None, f"未找到目的地: {destination}")
                dest_lng, dest_lat, dest_name = geo_result
            else:
                dest_lng, dest_lat = dest_lnglat
                dest_name = destination

            # 解析起点
            origin_lng, origin_lat = None, None
            if origin:
                origin_lnglat = _parse_lnglat(origin)
                if origin_lnglat:
                    origin_lng, origin_lat = origin_lnglat
            elif lat is not None and lon is not None:
                # 使用提供的当前位置作为起点
                origin_lng, origin_lat = lon, lat

            logger.bind(tag=TAG).info(f"api请求结束")

            # 如果有起点，立即规划路线
            if origin_lng is not None and origin_lat is not None:
                origin_str = f"{origin_lng},{origin_lat}"
                dest_str = f"{dest_lng},{dest_lat}"

                route_data = _amap_direction(api_base, api_key, mode, origin_str, dest_str)
                if not route_data:
                    return ActionResponse(Action.RESPONSE, None, "路线规划失败，请稍后重试")

                steps, total_distance, total_duration = _extract_steps_from_route(route_data, mode)
                if not steps:
                    return ActionResponse(Action.RESPONSE, None, "未找到有效路线")

                # 保存导航状态
                nav_state = {
                    "destination": {"lng": dest_lng, "lat": dest_lat, "name": dest_name},
                    "origin": {"lng": origin_lng, "lat": origin_lat},
                    "mode": mode,
                    "steps": steps,
                    "current_step": 0,
                    "total_distance": total_distance,
                    "total_duration": total_duration,
                    "start_time": time.time()
                }
                _set_navigation_state(conn, nav_state)

                # 发送NMEA控制启动命令
                _send_nmea_control_command(conn, "start", 1)

                distance_km = total_distance / 1000
                duration_min = total_duration / 60
                return ActionResponse(Action.RESPONSE, None,
                                      f"开始导航到{dest_name}，全程约{distance_km:.1f}公里，预计{duration_min:.0f}分钟。{steps[0]['instruction']}")
            else:
                # 等待位置更新
                nav_state = {
                    "destination": {"lng": dest_lng, "lat": dest_lat, "name": dest_name},
                    "mode": mode,
                    "waiting_for_location": True,
                    "start_time": time.time()
                }
                _set_navigation_state(conn, nav_state)

                # 发送NMEA控制启动命令
                _send_nmea_control_command(conn, "start", 1)

                return ActionResponse(Action.RESPONSE, None,
                                      f"准备导航到{dest_name}，请提供当前位置或等待位置更新")
        
        elif lat is not None and lon is not None:
            # 位置更新逻辑
            return _handle_navigation_update(conn, lat, lon)
        
        else:
            return ActionResponse(Action.RESPONSE, None, "请提供目的地开始导航，或提供经纬度更新位置")

    except Exception as e:
        logger.bind(tag=TAG).error(f"导航操作失败: {e}")
        return ActionResponse(Action.RESPONSE, None, "导航服务暂时不可用，请稍后重试")


def _handle_navigation_update(conn, lat: float, lon: float):
    """处理导航位置更新的内部函数"""
    try:
        nav_state = _get_navigation_state(conn)
        if not nav_state:
            return ActionResponse(Action.RESPONSE, None, "当前没有活跃的导航，请重新发送导航命令")
        
        cfg = _get_nav_cfg(conn)
        
        # 如果正在等待位置，先规划路线
        if nav_state.get("waiting_for_location"):
            api_key = cfg.get("api_key")
            api_base = cfg["api_base"]
            dest = nav_state["destination"]
            mode = nav_state["mode"]
            
            origin_str = f"{lon},{lat}"
            dest_str = f"{dest['lng']},{dest['lat']}"
            
            route_data = _amap_direction(api_base, api_key, mode, origin_str, dest_str)
            if not route_data:
                return ActionResponse(Action.RESPONSE, None, "路线规划失败")
            
            steps, total_distance, total_duration = _extract_steps_from_route(route_data, mode)
            if not steps:
                return ActionResponse(Action.RESPONSE, None, "未找到有效路线")
            
            # 更新导航状态
            nav_state.update({
                "origin": {"lng": lon, "lat": lat},
                "steps": steps,
                "current_step": 0,
                "total_distance": total_distance,
                "total_duration": total_duration,
                "waiting_for_location": False
            })
            _set_navigation_state(conn, nav_state)
            
            distance_km = total_distance / 1000
            duration_min = total_duration / 60
            return ActionResponse(Action.RESPONSE, None, 
                f"路线规划完成，全程约{distance_km:.1f}公里，预计{duration_min:.0f}分钟。{steps[0]['instruction']}")
        
        # 检查是否到达目的地
        dest = nav_state["destination"]
        distance_to_dest = _haversine(lon, lat, dest["lng"], dest["lat"])
        arrival_distance = cfg["arrival_distance"]
        
        if distance_to_dest <= arrival_distance:
            # 发送NMEA控制停止命令
            _send_nmea_control_command(conn, "stop")
            
            _clear_navigation_state(conn)
            return ActionResponse(Action.RESPONSE, None, f"您已到达目的地：{dest['name']}")
        
        # 检查是否需要转向提示
        steps = nav_state["steps"]
        current_step = nav_state["current_step"]
        turn_distance = cfg["turn_distance"]
        
        guidance = []
        
        # 检查当前步骤和下一步骤
        for i in range(current_step, min(len(steps), current_step + 2)):
            step = steps[i]
            step_distance = _haversine(lon, lat, step["end_lng"], step["end_lat"])
            
            if step_distance <= turn_distance:
                if i == current_step:
                    # 完成当前步骤
                    nav_state["current_step"] = i + 1
                    _set_navigation_state(conn, nav_state)
                    if i + 1 < len(steps):
                        next_step = steps[i + 1]
                        guidance.append(f"即将{next_step['instruction']}")
                elif i == current_step + 1:
                    # 提前提示下一步
                    guidance.append(f"{int(step_distance)}米后{step['instruction']}")
        
        if guidance:
            return ActionResponse(Action.RESPONSE, None, "，".join(guidance))
        
        # 无特殊提示，返回简单状态
        remaining_steps = len(steps) - current_step
        return ActionResponse(Action.RESPONSE, None, f"继续直行，还有{remaining_steps}个转向")
    
    except Exception as e:
        logger.bind(tag=TAG).error(f"导航更新失败: {e}")
        return ActionResponse(Action.RESPONSE, None, "导航更新失败")


@register_function("navigation_stop", STOP_FUNCTION_DESC, ToolType.SYSTEM_CTL)
def navigation_stop(conn):
    """停止导航"""
    try:
        nav_state = _get_navigation_state(conn)
        if not nav_state:
            return ActionResponse(Action.RESPONSE, None, "当前没有进行中的导航")
        
        # 发送NMEA控制停止命令
        _send_nmea_control_command(conn, "stop")
        
        _clear_navigation_state(conn)
        return ActionResponse(Action.RESPONSE, None, "已停止导航")
    
    except Exception as e:
        logger.bind(tag=TAG).error(f"停止导航失败: {e}")
        return ActionResponse(Action.RESPONSE, None, "停止导航失败")