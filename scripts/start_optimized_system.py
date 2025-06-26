#!/usr/bin/env python3
"""
启动优化后的消防单位建筑数据关联系统
并进行性能测试
"""

import os
import sys
import time
import psutil
import threading
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def display_system_info():
    """显示系统信息"""
    print("🔥" * 80)
    print("🔥 消防单位建筑数据关联系统 - 优化后启动")
    print("🔥" * 80)
    
    cpu_count = psutil.cpu_count(logical=True)
    memory = psutil.virtual_memory()
    print(f"\n💻 系统配置:")
    print(f"   CPU核心数: {cpu_count}")
    print(f"   总内存: {memory.total / (1024**3):.1f} GB")
    print(f"   可用内存: {memory.available / (1024**3):.1f} GB")
    print(f"   CPU使用率: {psutil.cpu_percent(interval=1):.1f}%")
    print(f"   内存使用率: {memory.percent:.1f}%")

def check_optimizations():
    """检查优化状态"""
    print("\n🔍 检查优化状态...")
    
    # 检查数据库配置
    config_path = project_root / "config" / "database.yaml"
    if config_path.exists():
        try:
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if 'mongodb' in config and 'connection_pool' in config['mongodb']:
                pool_size = config['mongodb']['connection_pool'].get('max_pool_size', 0)
                print(f"   ✅ MongoDB连接池: {pool_size}个连接")
            else:
                print("   ⚠️ MongoDB连接池未优化")
        except Exception as e:
            print(f"   ❌ 配置检查失败: {e}")
    
    # 检查匹配处理器优化
    processor_path = project_root / "src" / "matching" / "optimized_match_processor.py"
    if processor_path.exists():
        try:
            with open(processor_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            if 'limit = None' in code or 'batch_size = 10000' in code:
                print("   ✅ 匹配算法已优化")
            else:
                print("   ⚠️ 匹配算法可能未完全优化")
        except Exception as e:
            print(f"   ❌ 算法检查失败: {e}")

def start_system():
    """启动系统"""
    print("\n🚀 启动优化后的系统...")
    
    try:
        # 导入主程序
        from src.main import main as main_program
        
        # 在新线程中启动系统
        system_thread = threading.Thread(target=main_program, daemon=True)
        system_thread.start()
        
        print("   ✅ 系统启动成功")
        return True
        
    except Exception as e:
        print(f"   ❌ 系统启动失败: {e}")
        return False

def monitor_performance():
    """监控性能"""
    print("\n📊 开始性能监控...")
    print("   (按 Ctrl+C 停止监控)")
    
    start_time = time.time()
    try:
        while True:
            cpu = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # 性能状态指示器
            cpu_status = "🟢" if cpu < 50 else "🟡" if cpu < 80 else "🔴"
            mem_status = "🟢" if memory.percent < 70 else "🟡" if memory.percent < 90 else "🔴"
            
            # 运行时间
            runtime = time.time() - start_time
            runtime_str = f"{int(runtime//3600):02d}:{int((runtime%3600)//60):02d}:{int(runtime%60):02d}"
            
            print(f"\r🔥 运行时间: {runtime_str} | {cpu_status} CPU: {cpu:.1f}% | {mem_status} 内存: {memory.percent:.1f}% | 可用: {memory.available/(1024**3):.1f}GB", end="")
            
            time.sleep(5)  # 每5秒更新一次
            
    except KeyboardInterrupt:
        print("\n\n✅ 性能监控已停止")
        print(f"📈 总运行时间: {time.time() - start_time:.1f} 秒")

def run_performance_test():
    """运行性能测试"""
    print("\n🧪 运行性能测试...")
    
    try:
        # 测试数据库连接
        from src.database.connection import get_database_connection
        
        start_time = time.time()
        db = get_database_connection()
        connection_time = time.time() - start_time
        
        print(f"   ✅ 数据库连接测试: {connection_time:.3f}秒")
        
        # 测试数据查询
        start_time = time.time()
        supervision_collection = db.get_collection("xxj_shdwjbxx")
        count = supervision_collection.count_documents({})
        query_time = time.time() - start_time
        
        print(f"   ✅ 数据查询测试: {query_time:.3f}秒, 记录数: {count:,}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ 性能测试失败: {e}")
        return False

def main():
    """主函数"""
    display_system_info()
    check_optimizations()
    
    # 启动系统
    if start_system():
        time.sleep(3)  # 等待系统完全启动
        
        # 运行性能测试
        run_performance_test()
        
        # 开始监控
        monitor_performance()
    else:
        print("\n❌ 系统启动失败，无法继续")
        return False

if __name__ == "__main__":
    main() 