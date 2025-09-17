import re
from typing import Optional, Tuple, Dict, Any
from config.logger import setup_logging

TAG = __name__
logger = setup_logging()


def parse_nmea_coordinate(coord_str: str, direction: str) -> Optional[float]:
    """
    解析NMEA坐标格式为十进制度数
    
    Args:
        coord_str: NMEA坐标字符串，格式如 "3959.7648" (度分格式)
        direction: 方向字符 N/S/E/W
    
    Returns:
        十进制度数，如果解析失败返回None
    """
    try:
        if not coord_str or not direction:
            return None
            
        # 移除可能的空格
        coord_str = coord_str.strip()
        direction = direction.strip().upper()
        
        # 解析度分格式
        if len(coord_str) < 4:
            return None
            
        # 对于纬度：前2位是度，后面是分
        # 对于经度：前3位是度，后面是分
        if direction in ['N', 'S']:
            # 纬度
            degrees = int(coord_str[:2])
            minutes = float(coord_str[2:])
        else:
            # 经度
            degrees = int(coord_str[:3])
            minutes = float(coord_str[3:])
        
        # 转换为十进制度数
        decimal_degrees = degrees + minutes / 60.0
        
        # 根据方向调整符号
        if direction in ['S', 'W']:
            decimal_degrees = -decimal_degrees
            
        return decimal_degrees
        
    except (ValueError, IndexError) as e:
        logger.bind(tag=TAG).error(f"解析NMEA坐标失败: {coord_str}, {direction}, 错误: {e}")
        return None


def parse_gga_sentence(gga: str) -> Optional[Tuple[float, float]]:
    """
    解析GPGGA语句提取经纬度
    
    Args:
        gga: GPGGA语句，如 "$GPGGA,hhmmss.sss,llll.ll,a,yyyyy.yy,a,x,xx,x.x,x.x,M,x.x,M,x.x,xxxx*hh"
    
    Returns:
        (纬度, 经度) 元组，如果解析失败返回None
    """
    try:
        if not gga or not gga.startswith(('$GP', '$GB', '$GN')):
            return None
            
        # 移除校验和部分
        if '*' in gga:
            gga = gga.split('*')[0]
            
        # 分割字段
        fields = gga.split(',')
        
        if len(fields) < 6:
            return None
            
        # GGA格式：$GPGGA,时间,纬度,纬度方向,经度,经度方向,定位质量,卫星数,HDOP,海拔,M,大地水准面高,M,差分时间,差分站ID*校验和
        lat_str = fields[2]  # 纬度
        lat_dir = fields[3]  # 纬度方向
        lon_str = fields[4]  # 经度
        lon_dir = fields[5]  # 经度方向
        
        # 检查定位质量
        quality = fields[6] if len(fields) > 6 else '0'
        if quality == '0':  # 无效定位
            return None
            
        # 解析坐标
        lat = parse_nmea_coordinate(lat_str, lat_dir)
        lon = parse_nmea_coordinate(lon_str, lon_dir)
        
        if lat is not None and lon is not None:
            return lat, lon
            
        return None
        
    except Exception as e:
        logger.bind(tag=TAG).error(f"解析GGA语句失败: {gga}, 错误: {e}")
        return None


def parse_rmc_sentence(rmc: str) -> Optional[Tuple[float, float]]:
    """
    解析GPRMC语句提取经纬度
    
    Args:
        rmc: GPRMC语句，如 "$GPRMC,hhmmss.sss,A,llll.ll,a,yyyyy.yy,a,x.x,xxx.x,ddmmyy,x.x,a*hh"
    
    Returns:
        (纬度, 经度) 元组，如果解析失败返回None
    """
    try:
        if not rmc or not rmc.startswith(('$GP', '$GB', '$GN')):
            return None
            
        # 移除校验和部分
        if '*' in rmc:
            rmc = rmc.split('*')[0]
            
        # 分割字段
        fields = rmc.split(',')
        
        if len(fields) < 7:
            return None
            
        # RMC格式：$GPRMC,时间,状态,纬度,纬度方向,经度,经度方向,速度,航向,日期,磁偏角,磁偏角方向*校验和
        status = fields[2]   # 状态 A=有效，V=无效
        lat_str = fields[3]  # 纬度
        lat_dir = fields[4]  # 纬度方向
        lon_str = fields[5]  # 经度
        lon_dir = fields[6]  # 经度方向
        
        # 检查状态
        if status != 'A':  # 无效定位
            return None
            
        # 解析坐标
        lat = parse_nmea_coordinate(lat_str, lat_dir)
        lon = parse_nmea_coordinate(lon_str, lon_dir)
        
        if lat is not None and lon is not None:
            return (lat, lon)
            
        return None
        
    except Exception as e:
        logger.bind(tag=TAG).error(f"解析RMC语句失败: {rmc}, 错误: {e}")
        return None


def parse_nmea_message(nmea_data: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """
    解析NMEA消息，提取经纬度信息
    
    Args:
        nmea_data: NMEA消息数据，包含gga和rmc字段
    
    Returns:
        (纬度, 经度) 元组，如果解析失败返回None
    """
    try:
        # 优先使用GGA数据
        gga = nmea_data.get('gga')
        if gga:
            coords = parse_gga_sentence(gga)
            if coords:
                logger.bind(tag=TAG).debug(f"从GGA解析到坐标: {coords}")
                return coords
        
        # 如果GGA解析失败，尝试RMC
        rmc = nmea_data.get('rmc')
        if rmc:
            coords = parse_rmc_sentence(rmc)
            if coords:
                logger.bind(tag=TAG).debug(f"从RMC解析到坐标: {coords}")
                return coords
        
        logger.bind(tag=TAG).warning("无法从NMEA数据中解析出有效坐标")
        return None
        
    except Exception as e:
        logger.bind(tag=TAG).error(f"解析NMEA消息失败: {e}")
        return None


def create_nmea_control_message(client_id: str, action: str, rate_hz: int = 1) -> Dict[str, Any]:
    """
    创建NMEA控制消息
    
    Args:
        client_id: 客户端ID
        action: 动作，"start" 或 "stop"
        rate_hz: 频率，默认1Hz
    
    Returns:
        NMEA控制消息字典
    """
    return {
        "client_id": client_id,
        "type": "nmea_control",
        "action": action,
        "rate_hz": rate_hz
    }