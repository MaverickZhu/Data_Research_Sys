#!/usr/bin/env python3
"""
在没有Redis的情况下启动系统的临时脚本
用于演示Web界面
"""

import sys
import os
from pathlib import Path
from unittest.mock import patch, Mock

# 添加src目录到Python路径
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

try:
    from utils.logger import setup_logger
    from utils.config import ConfigManager
    from database.connection import DatabaseManager
    from web.app import create_app, app
except ImportError as e:
    print(f"❌ 导入模块失败: {e}")
    sys.exit(1)

# 设置日志
logger = setup_logger(__name__)


def mock_redis_connection():
    """模拟Redis连接"""
    mock_redis = Mock()
    
    # 模拟Redis基本操作
    mock_redis.ping.return_value = True
    mock_redis.info.return_value = {
        'redis_version': '7.0.0',
        'used_memory_human': '1.2MB'
    }
    mock_redis.set.return_value = True
    mock_redis.get.return_value = "测试连接"
    mock_redis.delete.return_value = 1
    
    return mock_redis


def main():
    """主程序入口"""
    print("=" * 60)
    print("🔥 消防单位建筑数据关联系统 - 演示模式（无Redis）")
    print("=" * 60)
    
    try:
        logger.info("🔥 消防单位建筑数据关联系统启动中... (演示模式)")
        
        # 验证配置文件
        logger.info("验证配置文件...")
        config_manager = ConfigManager()
        validation_results = config_manager.validate_configs()
        
        if not all(validation_results.values()):
            logger.error(f"配置验证失败: {validation_results}")
            return False
        
        logger.info("配置验证通过")
        
        # 测试数据库连接（模拟Redis）
        logger.info("测试数据库连接...")
        
        with patch('redis.Redis') as mock_redis_class:
            # 设置模拟Redis
            mock_redis_class.return_value = mock_redis_connection()
            
            # 创建数据库管理器（MongoDB真实连接，Redis模拟）
            db_manager = DatabaseManager(config_manager.get_database_config())
            
            # 获取数据统计
            supervision_count = db_manager.get_collection_count('xxj_shdwjbxx')
            inspection_count = db_manager.get_collection_count('xfaqpc_jzdwxx')
            
            logger.info(f"数据源统计:")
            logger.info(f"  - 消防监督管理系统: {supervision_count} 条记录")
            logger.info(f"  - 消防隐患安全排查系统: {inspection_count} 条记录")
            
            # 初始化Flask应用（Redis模拟）
            logger.info("初始化Web应用...")
            
            with patch('database.connection.redis.Redis') as mock_db_redis:
                mock_db_redis.return_value = mock_redis_connection()
                create_app()
            
            # 获取Web配置
            web_config = config_manager.get_web_config()
            flask_config = web_config.get('flask', {})
            
            host = flask_config.get('host', '0.0.0.0')
            port = flask_config.get('port', 5000)
            debug = flask_config.get('debug', False)
            
            print(f"\n🎉 系统启动成功！")
            print(f"🌐 Web服务地址: http://{host}:{port}")
            print(f"📝 状态: 演示模式（MongoDB真实连接，Redis模拟）")
            print(f"⚠️  注意: 缓存功能将使用内存模拟，重启后数据不会保存")
            print(f"🔧 要启用完整功能，请安装Redis服务")
            print("\n按 Ctrl+C 停止服务...")
            
            # 启动Flask应用
            app.run(
                host=host,
                port=port,
                debug=debug,
                threaded=True
            )
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  系统正在关闭...")
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


if __name__ == "__main__":
    print("\n🔥 消防单位建筑数据关联系统")
    print("📋 演示模式启动器")
    print("=" * 40)
    
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        sys.exit(1) 