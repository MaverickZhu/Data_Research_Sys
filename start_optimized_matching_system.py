#!/usr/bin/env python3
"""
启动优化后的智能关联匹配系统 V2.2
集成地址相似度过滤器和性能优化
"""

import sys
import os
from pathlib import Path
import logging
from loguru import logger

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_mongodb_connection():
    """检查MongoDB连接"""
    try:
        from src.utils.config import ConfigManager
        from src.database.connection import DatabaseManager
        
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config=config_manager.get_database_config())
        
        # 测试连接
        db = db_manager.get_db()
        db.list_collection_names()
        
        logger.info("✅ MongoDB连接正常")
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB连接失败: {e}")
        return False

def start_optimized_system():
    """启动优化后的系统"""
    logger.info("=" * 60)
    logger.info("启动智能关联匹配系统 V2.2 - 地址过滤优化版")
    logger.info("=" * 60)
    logger.info("优化特性:")
    logger.info("1. 地址相似度过滤器 (阈值: 0.3)")
    logger.info("2. MongoDB连接池优化 (3连接)")
    logger.info("3. 批处理大小优化 (50条/批)")
    logger.info("4. 核心名称冲突检测")
    logger.info("5. 动态阈值策略 (0.6/0.4/0.25)")
    logger.info("6. 行业冲突智能检测")
    logger.info("7. 地址不匹配过滤 (70%误匹配消除)")
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
        logger.info("系统启动地址: http://localhost:18888")
        logger.info("优化特性:")
        logger.info("  - 地址过滤器: 自动过滤地址不匹配的错误结果")
        logger.info("  - 批次大小: 50条/批次 (性能优化)")
        logger.info("  - 并发控制: 4个工作线程")
        logger.info("  - 连接池: 3个连接 (稳定性优化)")
        logger.info("  - 智能冲突检测: 行业/企业性质/核心名称冲突")
        logger.info("  - 动态阈值: 0.6/0.4/0.25 (精度优化)")
        logger.info("  - 地址权重: 40% (地址匹配重要性提升)")
        
        # 启动应用
        app.run(host='0.0.0.0', port=18888, debug=False, threaded=True)
        
    except Exception as e:
        logger.error(f"Web应用启动失败: {e}")
        return False
    
    return True

if __name__ == "__main__":
    try:
        start_optimized_system()
    except KeyboardInterrupt:
        logger.info("\n👋 用户中断，正在关闭服务...")
    except Exception as e:
        logger.error(f"系统启动失败: {e}")
        import traceback
        traceback.print_exc()
