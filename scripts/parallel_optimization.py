#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统并行处理优化脚本
利用32核CPU进行多线程处理，解决严重的性能瓶颈
"""
import sys
import os
import requests
import time
import json
import threading
import concurrent.futures
from datetime import datetime
import psutil

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

BASE_URL = "http://localhost:8888"

class ParallelOptimizer:
    def __init__(self):
        self.base_url = BASE_URL
        self.cpu_count = psutil.cpu_count()
        self.memory_info = psutil.virtual_memory()
        self.active_tasks = []
        
    def print_header(self, title):
        """打印标题"""
        print("\n" + "=" * 80)
        print(f"⚡ {title}")
        print("=" * 80)
    
    def analyze_system_resources(self):
        """分析系统资源"""
        print(f"🔍 系统资源分析:")
        print(f"   CPU核心数: {self.cpu_count}")
        print(f"   总内存: {self.memory_info.total / (1024**3):.1f} GB")
        print(f"   可用内存: {self.memory_info.available / (1024**3):.1f} GB")
        print(f"   内存使用率: {self.memory_info.percent:.1f}%")
        
        # 计算最优并行数
        optimal_workers = min(self.cpu_count - 2, 16)  # 保留2个核心，最多16个工作线程
        print(f"   建议并行数: {optimal_workers}")
        
        return optimal_workers
    
    def stop_existing_tasks(self):
        """停止现有的低效任务"""
        print(f"🛑 停止现有低效任务...")
        
        # 已知的任务ID
        task_ids = [
            "c2e93daf-b41c-47ce-b2bb-84fb372adfae",
            "fe67e811-77bc-4b3c-a2e1-1f56e4cdb521",
            "ebf0421e-4836-4ec6-90ac-298afeff8cfc"
        ]
        
        stopped_count = 0
        for task_id in task_ids:
            try:
                # 先检查任务状态
                response = requests.get(f"{self.base_url}/api/optimized_task_progress/{task_id}", 
                                      timeout=5)
                if response.status_code == 200:
                    status = response.json()
                    if status.get('status') == 'running':
                        print(f"   发现运行中任务: {task_id[:8]}...")
                        
                        # 尝试停止任务（即使API可能返回404）
                        try:
                            stop_response = requests.post(f"{self.base_url}/api/stop_optimized_matching", 
                                                        json={"task_id": task_id}, timeout=10)
                            if stop_response.status_code == 200:
                                print(f"   ✅ 任务已停止: {task_id[:8]}...")
                                stopped_count += 1
                            else:
                                print(f"   ⚠️ 停止请求发送: {task_id[:8]}... (HTTP {stop_response.status_code})")
                        except:
                            print(f"   ⚠️ 停止请求发送: {task_id[:8]}...")
                            
            except Exception as e:
                continue
        
        if stopped_count > 0:
            print(f"   成功停止 {stopped_count} 个任务")
            time.sleep(5)  # 等待任务完全停止
        else:
            print(f"   没有找到需要停止的任务")
        
        return stopped_count
    
    def start_parallel_tasks(self, num_workers=8):
        """启动多个并行任务"""
        print(f"🚀 启动 {num_workers} 个并行优化任务...")
        
        # 不同的配置组合
        configs = [
            {"batch_size": 1000, "mode": "incremental"},
            {"batch_size": 1500, "mode": "incremental"},
            {"batch_size": 2000, "mode": "incremental"},
            {"batch_size": 1200, "mode": "incremental"},
            {"batch_size": 800, "mode": "incremental"},
            {"batch_size": 1800, "mode": "incremental"},
            {"batch_size": 1000, "mode": "update"},
            {"batch_size": 1500, "mode": "update"}
        ]
        
        successful_tasks = []
        
        # 使用线程池启动多个任务
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_config = {}
            
            for i in range(min(num_workers, len(configs))):
                config = configs[i]
                future = executor.submit(self.start_single_task, config, i+1)
                future_to_config[future] = config
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_config, timeout=60):
                config = future_to_config[future]
                try:
                    task_id = future.result()
                    if task_id:
                        successful_tasks.append({
                            'task_id': task_id,
                            'config': config
                        })
                        print(f"   ✅ 任务 {len(successful_tasks)} 启动成功: {task_id[:8]}...")
                    else:
                        print(f"   ❌ 任务启动失败: {config}")
                except Exception as e:
                    print(f"   ❌ 任务启动异常: {config} - {str(e)}")
        
        self.active_tasks = successful_tasks
        print(f"🎯 成功启动 {len(successful_tasks)} 个并行任务")
        
        return successful_tasks
    
    def start_single_task(self, config, task_num):
        """启动单个任务"""
        try:
            payload = {
                "match_type": "both",
                "mode": config["mode"],
                "batch_size": config["batch_size"]
            }
            
            response = requests.post(f"{self.base_url}/api/start_optimized_matching", 
                                   json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data.get('task_id')
            
            return None
            
        except Exception as e:
            return None
    
    def monitor_parallel_performance(self, duration=300):
        """监控并行任务性能"""
        if not self.active_tasks:
            print(f"❌ 没有活动任务可监控")
            return False
        
        print(f"📊 开始监控 {len(self.active_tasks)} 个并行任务 ({duration}秒)...")
        
        start_time = time.time()
        performance_history = []
        
        while time.time() - start_time < duration:
            try:
                # 并行获取所有任务状态
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.active_tasks)) as executor:
                    future_to_task = {}
                    
                    for task_info in self.active_tasks:
                        task_id = task_info['task_id']
                        future = executor.submit(self.get_task_progress, task_id)
                        future_to_task[future] = task_info
                    
                    # 收集所有任务状态
                    current_stats = {
                        'total_processed': 0,
                        'total_speed': 0,
                        'active_tasks': 0,
                        'task_details': []
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_task, timeout=30):
                        task_info = future_to_task[future]
                        try:
                            progress = future.result()
                            if progress:
                                processed = progress.get('processed_records', 0)
                                elapsed = progress.get('elapsed_time', 0)
                                status = progress.get('status', 'unknown')
                                
                                if status == 'running' and elapsed > 0:
                                    speed = processed / elapsed
                                    current_stats['total_processed'] += processed
                                    current_stats['total_speed'] += speed
                                    current_stats['active_tasks'] += 1
                                    
                                    current_stats['task_details'].append({
                                        'task_id': task_info['task_id'][:8],
                                        'processed': processed,
                                        'speed': speed,
                                        'batch_size': task_info['config']['batch_size'],
                                        'status': status
                                    })
                        except Exception as e:
                            continue
                
                # 显示并行性能统计
                if current_stats['active_tasks'] > 0:
                    avg_speed = current_stats['total_speed'] / current_stats['active_tasks']
                    
                    print(f"\n📈 并行性能监控报告:")
                    print(f"   活跃任务数: {current_stats['active_tasks']}")
                    print(f"   总处理量: {current_stats['total_processed']:,} 条")
                    print(f"   总体速度: {current_stats['total_speed']:.3f} 记录/秒")
                    print(f"   平均速度: {avg_speed:.3f} 记录/秒")
                    
                    # 与单任务对比
                    single_task_speed = 0.01  # 原始单任务速度
                    improvement = current_stats['total_speed'] / single_task_speed if single_task_speed > 0 else 0
                    
                    print(f"   性能提升: {improvement:.1f}x")
                    
                    # 性能评级
                    if current_stats['total_speed'] > 5:
                        grade = "🟢 优秀"
                    elif current_stats['total_speed'] > 1:
                        grade = "🟡 良好"
                    elif current_stats['total_speed'] > 0.1:
                        grade = "🟠 一般"
                    else:
                        grade = "🔴 偏低"
                    
                    print(f"   性能评级: {grade}")
                    
                    # 显示各任务详情
                    print(f"   任务详情:")
                    for detail in current_stats['task_details']:
                        print(f"     {detail['task_id']}: {detail['processed']:,}条 "
                              f"({detail['speed']:.3f}/秒, 批次:{detail['batch_size']})")
                    
                    # 记录性能历史
                    performance_history.append({
                        'time': time.time() - start_time,
                        'total_speed': current_stats['total_speed'],
                        'active_tasks': current_stats['active_tasks'],
                        'total_processed': current_stats['total_processed']
                    })
                
                print("-" * 80)
                time.sleep(30)  # 每30秒检查一次
                
            except Exception as e:
                print(f"❌ 监控出错: {str(e)}")
                time.sleep(30)
        
        # 生成并行性能报告
        self.generate_parallel_report(performance_history)
        
        return True
    
    def get_task_progress(self, task_id):
        """获取单个任务进度"""
        try:
            response = requests.get(f"{self.base_url}/api/optimized_task_progress/{task_id}", 
                                  timeout=10)
            if response.status_code == 200:
                return response.json()
        except:
            pass
        return None
    
    def generate_parallel_report(self, performance_history):
        """生成并行性能报告"""
        if not performance_history:
            print(f"❌ 没有性能数据可分析")
            return
        
        # 计算性能指标
        speeds = [p['total_speed'] for p in performance_history]
        avg_speed = sum(speeds) / len(speeds)
        max_speed = max(speeds)
        min_speed = min(speeds)
        
        final_processed = performance_history[-1]['total_processed']
        final_tasks = performance_history[-1]['active_tasks']
        
        # 与原始性能对比
        original_speed = 0.01
        improvement_ratio = avg_speed / original_speed
        
        print(f"\n📊 并行处理性能报告:")
        print(f"   平均总速度: {avg_speed:.3f} 记录/秒")
        print(f"   最高总速度: {max_speed:.3f} 记录/秒")
        print(f"   最低总速度: {min_speed:.3f} 记录/秒")
        print(f"   最终处理量: {final_processed:,} 条")
        print(f"   活跃任务数: {final_tasks}")
        print(f"   性能提升: {improvement_ratio:.1f}x")
        print(f"   监控样本: {len(performance_history)} 次")
        
        # 效果评估
        if improvement_ratio > 100:
            print(f"   优化效果: 🟢 卓越 - 速度提升{improvement_ratio:.0f}倍!")
        elif improvement_ratio > 50:
            print(f"   优化效果: 🟢 优秀 - 速度提升{improvement_ratio:.0f}倍!")
        elif improvement_ratio > 10:
            print(f"   优化效果: 🟡 良好 - 速度提升{improvement_ratio:.0f}倍")
        elif improvement_ratio > 3:
            print(f"   优化效果: 🟠 一般 - 速度提升{improvement_ratio:.1f}倍")
        else:
            print(f"   优化效果: 🔴 有限 - 需要进一步优化")
        
        # 预估完成时间
        if avg_speed > 0:
            total_records = 1659320
            remaining_records = total_records - final_processed
            eta_hours = remaining_records / avg_speed / 3600
            print(f"   预计完成时间: {eta_hours:.1f} 小时")
    
    def provide_next_optimization_steps(self):
        """提供下一步优化建议"""
        print(f"\n🎯 下一步优化建议:")
        print(f"   1. 🗄️ 数据库索引优化 - 创建必要的查询索引")
        print(f"   2. 🧠 算法参数调优 - 优化匹配算法参数")
        print(f"   3. 💾 内存缓存优化 - 缓存常用数据和模型")
        print(f"   4. 🔄 连接池优化 - 优化数据库连接池配置")
        print(f"   5. 📊 批处理优化 - 进一步优化批处理逻辑")
        
        print(f"\n📈 监控建议:")
        print(f"   - 继续使用并行处理")
        print(f"   - 监控系统资源使用情况")
        print(f"   - 根据性能数据调整并行数")
        print(f"   - 定期检查任务状态")
    
    def run_parallel_optimization(self):
        """运行并行处理优化"""
        self.print_header("消防单位建筑数据关联系统 - 并行处理优化")
        
        print(f"🕒 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 目标: 利用32核CPU进行并行处理，显著提升处理速度")
        
        # 分析系统资源
        optimal_workers = self.analyze_system_resources()
        
        # 停止现有低效任务
        self.stop_existing_tasks()
        
        # 启动并行任务
        successful_tasks = self.start_parallel_tasks(optimal_workers)
        
        if successful_tasks:
            print(f"\n📊 开始并行性能监控...")
            self.monitor_parallel_performance(duration=360)  # 监控6分钟
            
            # 提供下一步建议
            self.provide_next_optimization_steps()
            
            self.print_header("并行处理优化完成")
            print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"✅ 并行优化执行成功")
            print(f"📊 {len(successful_tasks)} 个并行任务正在运行")
            
            return True
        else:
            print(f"❌ 无法启动并行任务")
            print(f"🔧 建议检查系统配置")
            
            self.print_header("并行处理优化完成")
            print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⚠️ 并行优化部分完成")
            
            return False

def main():
    """主函数"""
    optimizer = ParallelOptimizer()
    optimizer.run_parallel_optimization()

if __name__ == "__main__":
    main() 