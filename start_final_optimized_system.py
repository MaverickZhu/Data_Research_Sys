#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
优化后的系统启动脚本
集成所有性能和精度优化
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

def start_optimized_system():
    """启动优化后的系统"""
    logger.info("=" * 60)
    logger.info("启动优化后的智能关联匹配系统 V2.1")
    logger.info("=" * 60)
    logger.info("优化特性:")
    logger.info("1. MongoDB连接池优化 (50连接)")
    logger.info("2. 批处理大小优化 (50条/批)")
    logger.info("3. 核心名称冲突检测")
    logger.info("4. 动态阈值策略 (0.6/0.4/0.25)")
    logger.info("5. 行业冲突智能检测")
    logger.info("=" * 60)
    
    try:
        # 导入并启动Flask应用
        from src.web.app import app
        
        # 应用优化配置
        app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB
        
        logger.info("🚀 系统启动成功，访问地址: http://localhost:5000")
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=False,  # 生产环境关闭调试
            threaded=True,
            use_reloader=False  # 避免重复加载
        )
        
    except Exception as e:
        logger.error(f"系统启动失败: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = start_optimized_system()
    if not success:
        sys.exit(1)
