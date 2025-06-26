#!/usr/bin/env python3
"""
消防单位建筑数据关联系统 - 启动脚本
确保干净启动
"""

import sys
import time
import subprocess
from pathlib import Path

def main():
    """主启动函数"""
    print("🔥 消防单位建筑数据关联系统")
    print("=" * 50)
    
    # 检查端口
    print("检查端口8888...")
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', 8888))
        sock.close()
        
        if result == 0:
            print("⚠️ 端口8888已被占用")
            print("请先关闭占用端口的程序，或重启计算机")
            return
        else:
            print("✅ 端口8888可用")
    except Exception as e:
        print(f"端口检查失败: {e}")
    
    # 启动系统
    print("\n启动系统...")
    try:
        # 添加项目根目录到Python路径
        project_root = Path(__file__).parent
        sys.path.insert(0, str(project_root))
        
        # 导入并启动
        from src.main import main as start_main
        start_main()
        
    except KeyboardInterrupt:
        print("\n\n系统已停止")
    except Exception as e:
        print(f"\n启动失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 