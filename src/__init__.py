"""
消防单位建筑数据关联系统核心模块包
"""

__version__ = "1.0.0"
__author__ = "AI开发助手"
__description__ = "消防单位建筑数据关联系统"

# 导出主要组件
from .utils.config import ConfigManager
from .utils.logger import setup_logger
from .database.connection import DatabaseManager
from .matching.exact_matcher import ExactMatcher
from .matching.fuzzy_matcher import FuzzyMatcher
from .matching.match_processor import MatchProcessor

__all__ = [
    'ConfigManager',
    'setup_logger', 
    'DatabaseManager',
    'ExactMatcher',
    'FuzzyMatcher',
    'MatchProcessor'
] 