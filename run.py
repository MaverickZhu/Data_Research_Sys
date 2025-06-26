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
        # 检查Python版本
        if sys.version_info < (3, 8):
            print("❌ 错误: 需要Python 3.8或更高版本")
            print(f"当前版本: {sys.version}")
            return False
        
        # 导入主程序
        from src.main import main as main_program
        
        # 运行主程序
        return main_program()
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        print("请确保已安装所有依赖: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 