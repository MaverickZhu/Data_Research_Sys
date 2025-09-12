#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
优化后的系统启动脚本
"""

import os
import sys
import logging
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('logs/optimized_system.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def check_mongodb_connection():
    """检查MongoDB连接"""
    try:
        from src.database.connection import DatabaseManager
        from src.utils.config import ConfigManager
        
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config=config_manager.get_database_config())
        
        # 测试连接
        db_manager.get_collection('test_collection')
        logger.info("MongoDB连接正常")
        return True
    except Exception as e:
        logger.error(f"MongoDB连接失败: {e}")
        return False

def start_optimized_system():
    """启动优化后的系统"""
    logger.info("=" * 60)
    logger.info("启动优化后的智能关联匹配系统 V2.1")
    logger.info("=" * 60)
    
    # 1. 检查MongoDB连接
    if not check_mongodb_connection():
        logger.error("MongoDB连接失败，请检查数据库服务")
        return False
    
    # 2. 启动Web应用
    try:
        from src.web.app import app
        
        # 应用优化配置
        app.config.update({
            'MAX_CONTENT_LENGTH': 500 * 1024 * 1024,  # 500MB
            'SEND_FILE_MAX_AGE_DEFAULT': 0,
            'TEMPLATES_AUTO_RELOAD': True
        })
        
        logger.info("Web应用配置完成")
        logger.info("系统启动地址: http://localhost:5000")
        logger.info("优化特性:")
        logger.info("  - 批次大小优化: 50条/批次")
        logger.info("  - 并发控制: 4个工作线程")
        logger.info("  - 连接池优化: 50个连接")
        logger.info("  - 智能冲突检测: 行业/企业性质冲突")
        logger.info("  - 动态阈值调整: 0.6/0.4/0.25")
        
        # 启动应用
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
        
    except Exception as e:
        logger.error(f"Web应用启动失败: {e}")
        return False
    
    return True

if __name__ == "__main__":
    try:
        start_optimized_system()
    except KeyboardInterrupt:
        logger.info("系统已停止")
    except Exception as e:
        logger.error(f"系统启动异常: {e}")
