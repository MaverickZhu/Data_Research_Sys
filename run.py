#!/usr/bin/env python3
"""
消防单位建筑数据关联系统启动脚本
快速启动入口
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    """主函数"""
    try:
        # 打印启动横幅
        from src.main import print_banner
        print_banner()

        # 环境和依赖检查
        from src.main import check_environment, check_dependencies
        logger.info("系统环境检查...")
        if not check_environment():
            return False
        
        logger.info("依赖项检查...")
        if not check_dependencies():
            return False
            
        # 导入并运行初始化主程序
        from src.main import main as initialize_system
        
        initialized_app, config_manager = initialize_system()
        
        if not initialized_app or not config_manager:
            logger.error("❌ 系统初始化失败，请检查日志获取详细信息。")
            return False
        
        # 从配置中获取Web服务参数
        web_config = config_manager.get_web_config()
        flask_config = web_config.get('flask', {})
        
        host = flask_config.get('host', '0.0.0.0')
        port = flask_config.get('port', 8888) # 修正端口为8888
        debug = flask_config.get('debug', False)

        logger.info(f"🚀 准备在 http://{host}:{port} 上启动Web服务...")
        
        # 启动Flask应用
        initialized_app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True
        )
        
        return True
        
    except ImportError as e:
        logger.error(f"❌ 导入错误: {e}")
        logger.error("请确保已安装所有依赖: pip install -r requirements.txt")
        return False
    except Exception as e:
        logger.error(f"❌ 启动失败: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    # 需要一个基础的logger来捕获早期错误
    from src.utils.logger import setup_logger
    logger = setup_logger(__name__)

    success = main()
    sys.exit(0 if success else 1) 