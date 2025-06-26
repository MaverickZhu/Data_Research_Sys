#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时进度监控演示脚本
"""

import requests
import json
import time
from datetime import datetime

def demo_progress_monitor():
    """演示实时进度监控功能"""
    print("🚀 实时进度监控演示")
    print("="*60)
    print(f"演示时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    base_url = "http://127.0.0.1:8888"
    
    # 1. 显示当前系统状态
    print("\n📊 当前系统状态:")
    try:
        response = requests.get(f"{base_url}/api/stats", timeout=5)
        if response.status_code == 200:
            data = response.json()
            ds = data.get('data_sources', {})
            print(f"   监督管理系统: {ds.get('supervision_count', 0):,} 条")
            print(f"   安全排查系统: {ds.get('inspection_count', 0):,} 条")
            print(f"   匹配结果: {ds.get('match_results_count', 0):,} 条")
        else:
            print("   ❌ 无法获取系统状态")
    except Exception as e:
        print(f"   ❌ 获取系统状态失败: {e}")
    
    # 2. 检查当前运行的任务
    print("\n🔍 检查当前运行任务:")
    try:
        response = requests.get(f"{base_url}/api/running_tasks", timeout=5)
        if response.status_code == 200:
            data = response.json()
            running_count = data.get('count', 0)
            print(f"   运行中任务数: {running_count}")
            
            if running_count > 0:
                tasks = data.get('tasks', [])
                for i, task in enumerate(tasks, 1):
                    print(f"   任务 {i}: {task.get('id', 'N/A')}")
                    print(f"     进度: {task.get('progress', 0)}%")
                    print(f"     状态: {task.get('status', 'N/A')}")
            else:
                print("   当前无运行中的任务")
        else:
            print("   ❌ 无法获取运行任务")
    except Exception as e:
        print(f"   ❌ 获取运行任务失败: {e}")
    
    # 3. 显示任务历史
    print("\n📋 任务历史:")
    try:
        response = requests.get(f"{base_url}/api/task_history?page=1&per_page=5", timeout=5)
        if response.status_code == 200:
            data = response.json()
            tasks = data.get('tasks', [])
            total = data.get('total', 0)
            print(f"   总任务数: {total}")
            
            if tasks:
                print("   最近任务:")
                for i, task in enumerate(tasks, 1):
                    print(f"   {i}. ID: {task.get('id', 'N/A')[:8]}...")
                    print(f"      状态: {task.get('status', 'N/A')}")
                    print(f"      开始: {task.get('start_time', 'N/A')}")
                    print(f"      结束: {task.get('end_time', '进行中')}")
                    print(f"      匹配: {task.get('matches', 0)} 条")
                    print(f"      耗时: {task.get('duration', 0)} 秒")
                    print()
            else:
                print("   暂无任务历史")
        else:
            print("   ❌ 无法获取任务历史")
    except Exception as e:
        print(f"   ❌ 获取任务历史失败: {e}")
    
    # 4. 演示启动新任务（可选）
    print("\n🎯 演示功能:")
    print("   1. 访问进度监控页面: http://127.0.0.1:8888/progress")
    print("   2. 访问匹配任务页面: http://127.0.0.1:8888/matching")
    print("   3. 查看匹配结果页面: http://127.0.0.1:8888/results")
    
    # 5. 实时监控演示
    print("\n⏱️  实时监控演示 (10秒):")
    for i in range(10):
        print(f"   刷新 {i+1}/10...", end=" ")
        
        try:
            # 检查运行任务
            response = requests.get(f"{base_url}/api/running_tasks", timeout=2)
            if response.status_code == 200:
                data = response.json()
                running_count = data.get('count', 0)
                if running_count > 0:
                    tasks = data.get('tasks', [])
                    for task in tasks:
                        progress = task.get('progress', 0)
                        print(f"任务进度: {progress}%", end=" ")
                else:
                    print("无运行任务", end=" ")
            else:
                print("API错误", end=" ")
        except:
            print("连接失败", end=" ")
        
        print("✓")
        time.sleep(1)
    
    print("\n" + "="*60)
    print("✅ 实时进度监控演示完成")
    print("\n💡 使用提示:")
    print("   - 进度监控页面会每5秒自动刷新")
    print("   - 可以在匹配任务页面启动新任务")
    print("   - 运行中的任务会显示实时进度")
    print("   - 任务历史会记录所有已完成的任务")

if __name__ == "__main__":
    demo_progress_monitor() 