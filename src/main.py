"""
消防单位建筑数据关联系统主程序
"""

import os
import sys
import logging
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logger
from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
from src.web.app import create_app, app

# 设置日志
logger = setup_logger(__name__)


def main():
    """主程序入口"""
    try:
        logger.info("🔥 消防单位建筑数据关联系统启动中...")
        
        # 验证配置文件
        logger.info("验证配置文件...")
        config_manager = ConfigManager()
        validation_results = config_manager.validate_configs()
        
        if not all(validation_results.values()):
            logger.error(f"配置验证失败: {validation_results}")
            return False
        
        logger.info("配置验证通过")
        
        # 测试数据库连接
        logger.info("测试数据库连接...")
        db_manager = DatabaseManager(config_manager.get_database_config())
        
        # 获取数据统计
        supervision_count = db_manager.get_collection_count('xxj_shdwjbxx')
        inspection_count = db_manager.get_collection_count('xfaqpc_jzdwxx')
        
        logger.info(f"数据源统计:")
        logger.info(f"  - 消防监督管理系统: {supervision_count} 条记录")
        logger.info(f"  - 消防隐患安全排查系统: {inspection_count} 条记录")
        
        # 初始化Flask应用
        logger.info("初始化Web应用...")
        create_app()
        
        # 获取Web配置
        web_config = config_manager.get_web_config()
        flask_config = web_config.get('flask', {})
        
        host = flask_config.get('host', '0.0.0.0')
        port = flask_config.get('port', 5000)
        debug = flask_config.get('debug', False)
        
        logger.info(f"Web服务启动: http://{host}:{port}")
        logger.info("系统已就绪! 🚀")
        
        # 启动Flask应用
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True
        )
        
    except KeyboardInterrupt:
        logger.info("系统正在关闭...")
        return True
    except Exception as e:
        logger.error(f"系统启动失败: {str(e)}")
        return False
    finally:
        # 清理资源
        try:
            if 'db_manager' in locals():
                db_manager.close()
                logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"清理资源失败: {str(e)}")


def check_dependencies():
    """检查依赖项"""
    try:
        import pymongo
        import redis
        import flask
        import yaml
        import jieba
        import pypinyin
        import fuzzywuzzy
        import sklearn
        import pandas
        import numpy
        
        logger.info("依赖项检查通过")
        return True
        
    except ImportError as e:
        logger.error(f"缺少依赖项: {str(e)}")
        logger.error("请运行: pip install -r requirements.txt")
        return False


def check_environment():
    """检查运行环境"""
    try:
        # 检查Python版本
        if sys.version_info < (3, 8):
            logger.error("需要Python 3.8或更高版本")
            return False
        
        # 检查配置目录
        config_dir = Path(__file__).parent.parent / "config"
        if not config_dir.exists():
            logger.error(f"配置目录不存在: {config_dir}")
            return False
        
        # 检查必要的配置文件
        required_configs = ["database.yaml", "matching.yaml"]
        for config_file in required_configs:
            config_path = config_dir / config_file
            if not config_path.exists():
                logger.error(f"配置文件不存在: {config_path}")
                return False
        
        # 检查日志目录
        log_dir = Path(__file__).parent.parent / "logs"
        if not log_dir.exists():
            log_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"创建日志目录: {log_dir}")
        
        logger.info("环境检查通过")
        return True
        
    except Exception as e:
        logger.error(f"环境检查失败: {str(e)}")
        return False


def print_banner():
    """打印启动横幅"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║       🔥消防单位建筑数据关联系统 (Data Research System)        ║
    ║                                                              ║
    ║                    版本: 1.0.1                               ║
    ║                    作者: 朱鸣君                               ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


if __name__ == "__main__":
    # 打印启动横幅
    print_banner()
    
    # 设置基础日志
    logger = setup_logger(__name__, level="INFO")
    
    try:
        # 环境检查
        if not check_environment():
            sys.exit(1)
        
        # 依赖检查
        if not check_dependencies():
            sys.exit(1)
        
        # 启动主程序
        success = main()
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        sys.exit(1) 