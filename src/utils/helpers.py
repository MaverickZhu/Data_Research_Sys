"""
辅助函数模块
提供各种工具函数
"""

import re
import logging
from typing import Any, Optional, Union
from datetime import datetime
from bson import ObjectId

logger = logging.getLogger(__name__)


def normalize_string(text: str) -> str:
    """
    标准化字符串
    
    Args:
        text: 原始字符串
        
    Returns:
        str: 标准化后的字符串
    """
    if not text:
        return ""
    
    # 转换为字符串并去除首尾空格
    text = str(text).strip()
    
    # 移除多余的空格
    text = re.sub(r'\s+', ' ', text)
    
    # 移除特殊字符（保留中文、英文、数字、常用标点）
    text = re.sub(r'[^\u4e00-\u9fff\w\s\.\-\(\)（）]', '', text)
    
    return text


def normalize_phone(phone: str) -> str:
    """
    标准化电话号码
    
    Args:
        phone: 原始电话号码
        
    Returns:
        str: 标准化后的电话号码
    """
    if not phone:
        return ""
    
    # 移除所有非数字字符
    phone = re.sub(r'\D', '', str(phone))
    
    # 移除开头的0或+86
    phone = re.sub(r'^(0|86)', '', phone)
    
    return phone


def safe_float_convert(value: Any, default: float = 0.0) -> float:
    """
    安全的浮点数转换
    
    Args:
        value: 待转换的值
        default: 转换失败时的默认值
        
    Returns:
        float: 转换后的浮点数
    """
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (ValueError, TypeError):
        logger.debug(f"浮点数转换失败: {value}")
        return default


def safe_int_convert(value: Any, default: int = 0) -> int:
    """
    安全的整数转换
    
    Args:
        value: 待转换的值
        default: 转换失败时的默认值
        
    Returns:
        int: 转换后的整数
    """
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (ValueError, TypeError):
        logger.debug(f"整数转换失败: {value}")
        return default


def extract_chinese_text(text: str) -> str:
    """
    提取中文文本
    
    Args:
        text: 原始文本
        
    Returns:
        str: 提取的中文文本
    """
    if not text:
        return ""
    
    # 使用正则表达式提取中文字符
    chinese_chars = re.findall(r'[\u4e00-\u9fff]+', str(text))
    
    return ''.join(chinese_chars)


def calculate_percentage_diff(value1: float, value2: float) -> float:
    """
    计算两个数值的百分比差异
    
    Args:
        value1: 数值1
        value2: 数值2
        
    Returns:
        float: 百分比差异 (0-1之间)
    """
    if value1 == 0 and value2 == 0:
        return 0.0
    
    if value1 == 0 or value2 == 0:
        return 1.0
    
    return abs(value1 - value2) / max(value1, value2)


def is_valid_credit_code(credit_code: str) -> bool:
    """
    验证统一社会信用代码格式
    
    Args:
        credit_code: 信用代码
        
    Returns:
        bool: 是否有效
    """
    if not credit_code:
        return False
    
    # 统一社会信用代码应为18位字符
    credit_code = str(credit_code).strip().upper()
    
    if len(credit_code) != 18:
        return False
    
    # 简单格式验证（完整验证需要校验位算法）
    pattern = r'^[0-9A-HJ-NPQRTUWXY]{2}\d{6}[0-9A-HJ-NPQRTUWXY]{10}$'
    
    return bool(re.match(pattern, credit_code))


def is_valid_phone(phone: str) -> bool:
    """
    验证电话号码格式
    
    Args:
        phone: 电话号码
        
    Returns:
        bool: 是否有效
    """
    if not phone:
        return False
    
    # 标准化电话号码
    normalized = normalize_phone(phone)
    
    if not normalized:
        return False
    
    # 手机号码：11位，以1开头
    if len(normalized) == 11 and normalized.startswith('1'):
        return True
    
    # 固定电话：7-8位数字
    if 7 <= len(normalized) <= 8:
        return True
    
    # 带区号的固定电话：10-12位
    if 10 <= len(normalized) <= 12:
        return True
    
    return False


def format_timestamp(timestamp: Union[datetime, str, int, float], format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    格式化时间戳
    
    Args:
        timestamp: 时间戳（datetime对象、字符串或数字）
        format_str: 格式化字符串
        
    Returns:
        str: 格式化后的时间字符串
    """
    try:
        if isinstance(timestamp, datetime):
            return timestamp.strftime(format_str)
        elif isinstance(timestamp, str):
            # 尝试解析字符串时间
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.strftime(format_str)
        elif isinstance(timestamp, (int, float)):
            # 时间戳转换
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime(format_str)
        else:
            return str(timestamp)
    except Exception as e:
        logger.debug(f"时间格式化失败: {timestamp}, {str(e)}")
        return str(timestamp)


def truncate_string(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    截断字符串
    
    Args:
        text: 原始字符串
        max_length: 最大长度
        suffix: 后缀
        
    Returns:
        str: 截断后的字符串
    """
    if not text:
        return ""
    
    text = str(text)
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def deep_merge_dict(dict1: dict, dict2: dict) -> dict:
    """
    深度合并字典
    
    Args:
        dict1: 字典1（被合并）
        dict2: 字典2（合并源）
        
    Returns:
        dict: 合并后的字典
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dict(result[key], value)
        else:
            result[key] = value
    
    return result


def generate_match_id(source_id: str, target_id: str) -> str:
    """
    生成匹配ID
    
    Args:
        source_id: 源记录ID
        target_id: 目标记录ID
        
    Returns:
        str: 匹配ID
    """
    import hashlib
    
    # 创建唯一标识符
    combined = f"{source_id}_{target_id}_{datetime.now().strftime('%Y%m%d')}"
    
    # 生成MD5哈希
    hash_obj = hashlib.md5(combined.encode('utf-8'))
    
    return hash_obj.hexdigest()[:16]  # 取前16位


def batch_iterator(items: list, batch_size: int = 100):
    """
    批量迭代器
    
    Args:
        items: 数据列表
        batch_size: 批次大小
        
    Yields:
        list: 批次数据
    """
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def memory_usage() -> dict:
    """
    获取内存使用情况
    
    Returns:
        dict: 内存使用统计
    """
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        
        return {
            'rss': memory_info.rss / 1024 / 1024,  # MB
            'vms': memory_info.vms / 1024 / 1024,  # MB
            'percent': process.memory_percent()
        }
    except ImportError:
        return {'error': 'psutil not available'}
    except Exception as e:
        return {'error': str(e)}


def convert_objectid_to_str(data: Any) -> Any:
    """
    递归转换数据中的ObjectId为字符串，解决JSON序列化问题
    
    Args:
        data: 需要转换的数据（可以是dict、list、ObjectId或其他类型）
        
    Returns:
        Any: 转换后的数据
    """
    if isinstance(data, dict):
        return {key: convert_objectid_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data


def safe_json_response(data: Any) -> Any:
    """
    安全的JSON响应数据处理，确保所有MongoDB数据都能正确序列化
    
    Args:
        data: 原始数据
        
    Returns:
        Any: 可安全序列化的数据
    """
    try:
        return convert_objectid_to_str(data)
    except Exception as e:
        logger.error(f"数据序列化处理失败: {str(e)}")
        return {"error": "数据处理失败", "details": str(e)} 