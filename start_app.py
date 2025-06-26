#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统启动脚本
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    """主启动函数"""
    print("🔥 启动消防单位建筑数据关联系统")
    print("=" * 50)
    
    try:
        # 导入并启动Web应用
        from src.web.app import create_app, app
        
        print("📋 初始化应用组件...")
        create_app()
        
        print("✅ 应用初始化成功!")
        print(f"🌐 访问地址: http://localhost:5000")
        print(f"🔗 增强关联: http://localhost:5000/enhanced_association")
        print("=" * 50)
        print("按 Ctrl+C 停止服务")
        
        # 启动Flask开发服务器
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=True,
            threaded=True
        )
        
    except KeyboardInterrupt:
        print("\n\n👋 用户中断，正在关闭服务...")
    except Exception as e:
        print(f"\n❌ 启动失败: {str(e)}")
        print("\n🔧 故障排除建议:")
        print("1. 检查MongoDB是否运行 (mongodb://localhost:27017)")
        print("2. 检查Redis是否运行 (localhost:6379)")
        print("3. 运行验证脚本: python verify_enhanced_features.py")
        sys.exit(1)


if __name__ == "__main__":
    main() 