#!/usr/bin/env python3
"""
用户数据智能匹配性能监控器
目标：监控达到原项目188万数据30分钟的处理速度
"""
import psutil
import time
import pymongo
from datetime import datetime

def monitor_user_matching_performance():
    """监控用户数据匹配性能"""
    print("🚀 用户数据智能匹配性能监控启动...")
    print("🎯 目标性能：1040条/秒（原项目级别）")
    
    # 连接数据库
    try:
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        db = client['Unit_Info']
        tasks_collection = db['user_matching_tasks']
        results_collection = db['user_match_results']
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return
    
    last_processed = 0
    start_time = time.time()
    
    while True:
        try:
            # 系统资源监控
            cpu = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # 任务进度监控
            active_tasks = list(tasks_collection.find({"status": "running"}))
            total_results = results_collection.count_documents({})
            
            current_time = time.time()
            elapsed = current_time - start_time
            
            if active_tasks:
                task = active_tasks[0]
                processed = task.get('processed', 0)
                total = task.get('total', 0)
                
                # 计算实时速度
                if elapsed > 0:
                    speed = (processed - last_processed) / (elapsed / 60)  # 每分钟处理数
                    speed_per_sec = speed / 60  # 每秒处理数
                    
                    # 性能状态
                    if speed_per_sec >= 1040:
                        speed_status = "🟢 超越原项目"
                    elif speed_per_sec >= 500:
                        speed_status = "🟡 接近目标"
                    else:
                        speed_status = "🔴 需要优化"
                    
                    progress = (processed / total * 100) if total > 0 else 0
                    
                    print(f"\r{speed_status} 速度: {speed_per_sec:.1f}条/秒 | 进度: {progress:.1f}% ({processed}/{total}) | CPU: {cpu:.1f}% | 内存: {memory.percent:.1f}%", end="")
                    
                    last_processed = processed
                
                start_time = current_time
            else:
                print(f"\r⏸️  无活动任务 | 总结果: {total_results} | CPU: {cpu:.1f}% | 内存: {memory.percent:.1f}%", end="")
            
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\n📊 监控已停止")
            break
        except Exception as e:
            print(f"\n❌ 监控错误: {e}")
            time.sleep(5)

if __name__ == "__main__":
    monitor_user_matching_performance()
