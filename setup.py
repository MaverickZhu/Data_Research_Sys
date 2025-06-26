#!/usr/bin/env python3
"""
消防单位建筑数据关联系统安装和配置脚本
"""

import sys
import os
import subprocess
from pathlib import Path
import shutil

def print_header():
    """打印头部信息"""
    print("=" * 60)
    print("🔥 消防单位建筑数据关联系统 - 安装配置向导")
    print("=" * 60)

def check_python_version():
    """检查Python版本"""
    print("\n📋 1. 检查Python版本...")
    if sys.version_info < (3, 8):
        print(f"❌ Python版本过低: {sys.version}")
        print("需要Python 3.8或更高版本")
        return False
    print(f"✅ Python版本: {sys.version}")
    return True

def install_dependencies():
    """安装依赖"""
    print("\n📦 2. 安装项目依赖...")
    
    requirements_file = Path("requirements.txt")
    if not requirements_file.exists():
        print("❌ requirements.txt文件不存在")
        return False
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
        ], check=True, capture_output=True, text=True)
        print("✅ 依赖安装成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 依赖安装失败: {e}")
        print(f"错误输出: {e.stderr}")
        return False

def setup_directories():
    """创建必要的目录"""
    print("\n📁 3. 创建项目目录...")
    
    directories = [
        "logs",
        "temp", 
        "uploads",
        "exports"
    ]
    
    for directory in directories:
        dir_path = Path(directory)
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✅ 创建目录: {directory}")
        else:
            print(f"✅ 目录已存在: {directory}")
    
    return True

def check_config_files():
    """检查配置文件"""
    print("\n⚙️  4. 检查配置文件...")
    
    config_files = [
        "config/database.yaml",
        "config/matching.yaml", 
        "config/web.yaml"
    ]
    
    all_exist = True
    for config_file in config_files:
        config_path = Path(config_file)
        if config_path.exists():
            print(f"✅ 配置文件存在: {config_file}")
        else:
            print(f"❌ 配置文件缺失: {config_file}")
            all_exist = False
    
    return all_exist

def test_imports():
    """测试关键模块导入"""
    print("\n🧪 5. 测试模块导入...")
    
    modules = [
        ("pymongo", "MongoDB驱动"),
        ("redis", "Redis驱动"),
        ("flask", "Flask框架"),
        ("yaml", "YAML解析"),
        ("jieba", "中文分词"),
        ("pypinyin", "拼音转换"),
        ("fuzzywuzzy", "模糊匹配"),
        ("sklearn", "机器学习"),
        ("pandas", "数据处理"),
        ("numpy", "数值计算")
    ]
    
    failed_modules = []
    
    for module_name, description in modules:
        try:
            __import__(module_name)
            print(f"✅ {description}: {module_name}")
        except ImportError:
            print(f"❌ {description}: {module_name}")
            failed_modules.append(module_name)
    
    return len(failed_modules) == 0

def create_env_example():
    """创建环境变量示例文件"""
    print("\n🔧 6. 创建环境配置示例...")
    
    env_content = """# 消防单位建筑数据关联系统环境配置
# 复制此文件为 .env 并修改相应配置

# MongoDB配置
MONGODB_HOST=localhost
MONGODB_PORT=27017
MONGODB_DATABASE=Unit_Info

# Redis配置  
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# Flask配置
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
FLASK_DEBUG=false
FLASK_SECRET_KEY=your-secret-key-here

# 日志级别
LOG_LEVEL=INFO
"""
    
    env_file = Path(".env.example")
    with open(env_file, 'w', encoding='utf-8') as f:
        f.write(env_content)
    
    print("✅ 创建 .env.example 文件")
    return True

def print_next_steps():
    """打印后续步骤"""
    print("\n🎉 安装配置完成!")
    print("\n📋 接下来的步骤:")
    print("1. 启动MongoDB和Redis服务")
    print("2. 检查并修改config/目录下的配置文件")
    print("3. 导入测试数据到MongoDB")
    print("4. 运行系统: python run.py")
    print("\n📚 更多信息请查看README.md文件")

def main():
    """主函数"""
    print_header()
    
    # 检查Python版本
    if not check_python_version():
        return False
    
    # 安装依赖
    if not install_dependencies():
        print("\n⚠️  可以手动安装依赖: pip install -r requirements.txt")
    
    # 创建目录
    setup_directories()
    
    # 检查配置文件
    config_ok = check_config_files()
    if not config_ok:
        print("\n⚠️  请确保所有配置文件都存在并正确配置")
    
    # 测试导入
    import_ok = test_imports()
    if not import_ok:
        print("\n⚠️  某些模块导入失败，请检查依赖安装")
    
    # 创建环境配置示例
    create_env_example()
    
    # 打印后续步骤
    print_next_steps()
    
    return config_ok and import_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 