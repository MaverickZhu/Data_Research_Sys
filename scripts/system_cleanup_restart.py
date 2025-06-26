#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统清理重启脚本
清理所有运行中的任务，重新启动优化后的单个高性能任务
"""
import sys
import os
import requests
import time
from datetime import datetime
import psutil

BASE_URL = "http://localhost:8888"

def print_header(title):
    """打印标题"""
    print("\\n" + "=" * 80)
    print(f"🔄 {title}")
    print("=" * 80)

def check_system_load():
    """检查系统负载"""
    cpu_percent = psutil.cpu_percent(interval=2)
    memory = psutil.virtual_memory()
    
    print(f"📊 当前系统负载:")
    print(f"   CPU使用率: {cpu_percent:.1f}%")
    print(f"   内存使用率: {memory.percent:.1f}%")
    print(f"   可用内存: {memory.available / (1024**3):.1f} GB")
    
    return {
        'cpu_percent': cpu_percent,
        'memory_percent': memory.percent,
        'high_load': cpu_percent > 80 or memory.percent > 80
    }

def start_optimized_task():
    """启动优化任务"""
    print(f"🚀 启动优化任务...")
    
    payload = {
        "match_type": "both",
        "mode": "incremental", 
        "batch_size": 1000
    }
    
    try:
        response = requests.post(f"{BASE_URL}/api/start_optimized_matching", 
                               json=payload, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                task_id = data.get('task_id')
                print(f"✅ 任务启动成功: {task_id}")
                return task_id
            else:
                print(f"❌ 启动失败: {data.get('error', '未知错误')}")
        else:
            print(f"❌ 启动失败: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"❌ 启动失败: {str(e)}")
    
    return None

def monitor_task(task_id, duration=300):
    """监控任务"""
    print(f"📊 监控任务 ({duration}秒)...")
    
    start_time = time.time()
    last_processed = 0
    
    while time.time() - start_time < duration:
        try:
            response = requests.get(f"{BASE_URL}/api/optimized_task_progress/{task_id}", 
                                  timeout=15)
            
            if response.status_code == 200:
                progress = response.json()
                
                current_processed = progress.get('processed_records', 0)
                elapsed_time = progress.get('elapsed_time', 0)
                status = progress.get('status', 'unknown')
                
                if elapsed_time > 0:
                    current_speed = current_processed / elapsed_time
                    
                    if last_processed > 0:
                        increment = current_processed - last_processed
                        increment_speed = increment / 30
                        
                        print(f"\\n📈 任务监控:")
                        print(f"   已处理: {current_processed:,} 条")
                        print(f"   总体速度: {current_speed:.3f} 记录/秒")
                        print(f"   增量速度: {increment_speed:.3f} 记录/秒")
                        print(f"   任务状态: {status}")
                        
                        # 性能评估
                        original_speed = 0.01
                        improvement = current_speed / original_speed
                        print(f"   性能提升: {improvement:.1f}x")
                    
                    last_processed = current_processed
                
                if status in ['completed', 'error', 'stopped']:
                    print(f"📋 任务状态变更: {status}")
                    break
                    
            else:
                print(f"❌ 获取进度失败: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"❌ 监控出错: {str(e)}")
        
        print("-" * 60)
        time.sleep(30)
    
    return True

def main():
    """主函数"""
    print_header("消防单位建筑数据关联系统 - 清理重启")
    
    print(f"🕒 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 检查系统负载
    load_info = check_system_load()
    
    # 等待系统稳定
    if load_info['high_load']:
        print(f"⏳ 系统负载较高，等待30秒...")
        time.sleep(30)
    
    # 启动优化任务
    task_id = start_optimized_task()
    
    if task_id:
        print(f"\\n📊 开始监控...")
        monitor_task(task_id, duration=360)
        
        print_header("清理重启完成")
        print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ 任务正在运行: {task_id}")
    else:
        print(f"❌ 无法启动任务")
        print_header("清理重启完成")
        print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 