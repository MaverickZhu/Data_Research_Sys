"""
日志管理模块
配置和管理系统日志
"""

import logging
import os
import sys
from datetime import datetime
from loguru import logger as loguru_logger
from typing import Optional


def setup_logger(name: Optional[str] = None, level: str = "INFO") -> logging.Logger:
    """
    设置日志记录器
    
    Args:
        name: 日志记录器名称
        level: 日志级别
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 创建logs目录
    log_dir = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 配置loguru
    loguru_logger.remove()  # 移除默认handler
    
    # 控制台输出
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    loguru_logger.add(
        sys.stdout,
        format=log_format,
        level=level,
        colorize=True
    )
    
    # 文件输出
    log_file = os.path.join(log_dir, f"app_{datetime.now().strftime('%Y%m%d')}.log")
    loguru_logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=level,
        rotation="100 MB",
        retention="7 days",
        compression="zip",
        encoding="utf-8"
    )
    
    # 错误日志单独记录
    error_log_file = os.path.join(log_dir, f"error_{datetime.now().strftime('%Y%m%d')}.log")
    loguru_logger.add(
        error_log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="ERROR",
        rotation="50 MB",
        retention="30 days",
        compression="zip",
        encoding="utf-8"
    )
    
    # 返回标准logging.Logger对象以兼容其他库
    if name:
        std_logger = logging.getLogger(name)
    else:
        std_logger = logging.getLogger(__name__)
        
    std_logger.setLevel(getattr(logging, level.upper()))
    
    # 如果还没有handler，添加一个
    if not std_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s'
        )
        handler.setFormatter(formatter)
        std_logger.addHandler(handler)
    
    return std_logger


class LogInterceptHandler(logging.Handler):
    """
    拦截标准logging并重定向到loguru
    """
    
    def emit(self, record):
        # 获取对应的loguru级别
        try:
            level = loguru_logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # 查找调用者
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        loguru_logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def configure_third_party_loggers():
    """配置第三方库的日志"""
    # 拦截标准logging
    logging.basicConfig(handlers=[LogInterceptHandler()], level=0, force=True)
    
    # 设置第三方库日志级别
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


# 初始化配置
configure_third_party_loggers() 