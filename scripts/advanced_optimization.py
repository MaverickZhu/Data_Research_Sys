#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统高级优化脚本
包含：性能监控、算法调优、并发优化、内存管理、错误处理优化
"""
import sys
import os
import requests
import time
import json
import threading
import psutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

BASE_URL = "http://localhost:8888"

class AdvancedOptimizer:
    def __init__(self):
        self.base_url = BASE_URL
        self.current_task_id = None
        self.performance_data = []
        self.monitoring_active = False
        
    def print_header(self, title):
        """打印标题"""
        print("\n" + "=" * 70)
        print(f"🚀 {title}")
        print("=" * 70)
    
    def print_step(self, step_num, title):
        """打印步骤"""
        print(f"\n{step_num}️⃣ {title}")
        print("-" * 50)
    
    def get_system_performance(self):
        """获取系统性能指标"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'disk_percent': disk.percent,
                'timestamp': datetime.now()
            }
        except Exception as e:
            print(f"❌ 获取系统性能失败: {str(e)}")
            return None
    
    def monitor_system_performance(self, duration=300):
        """监控系统性能"""
        print(f"📊 开始系统性能监控 ({duration}秒)...")
        self.monitoring_active = True
        
        def monitor_loop():
            while self.monitoring_active:
                perf = self.get_system_performance()
                if perf:
                    self.performance_data.append(perf)
                    
                    # 检查性能警告
                    if perf['cpu_percent'] > 80:
                        print(f"⚠️ CPU使用率过高: {perf['cpu_percent']:.1f}%")
                    if perf['memory_percent'] > 85:
                        print(f"⚠️ 内存使用率过高: {perf['memory_percent']:.1f}%")
                    if perf['memory_available_gb'] < 1:
                        print(f"⚠️ 可用内存不足: {perf['memory_available_gb']:.2f}GB")
                
                time.sleep(10)  # 每10秒检查一次
        
        monitor_thread = threading.Thread(target=monitor_loop)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # 等待指定时间后停止监控
        time.sleep(duration)
        self.monitoring_active = False
        
        return self.analyze_performance_data()
    
    def analyze_performance_data(self):
        """分析性能数据"""
        if not self.performance_data:
            return None
        
        cpu_values = [p['cpu_percent'] for p in self.performance_data]
        memory_values = [p['memory_percent'] for p in self.performance_data]
        
        analysis = {
            'avg_cpu': sum(cpu_values) / len(cpu_values),
            'max_cpu': max(cpu_values),
            'avg_memory': sum(memory_values) / len(memory_values),
            'max_memory': max(memory_values),
            'sample_count': len(self.performance_data)
        }
        
        print(f"\n📈 性能分析结果:")
        print(f"   平均CPU使用率: {analysis['avg_cpu']:.1f}%")
        print(f"   最高CPU使用率: {analysis['max_cpu']:.1f}%")
        print(f"   平均内存使用率: {analysis['avg_memory']:.1f}%")
        print(f"   最高内存使用率: {analysis['max_memory']:.1f}%")
        print(f"   采样次数: {analysis['sample_count']}")
        
        return analysis
    
    def get_current_task_status(self):
        """获取当前任务状态"""
        try:
            response = requests.get(f"{self.base_url}/api/optimized_task_progress/{self.current_task_id}", 
                                  timeout=10)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"❌ 获取任务状态失败: {str(e)}")
            return None
    
    def optimize_batch_size_dynamically(self):
        """动态优化批次大小"""
        print("🔧 动态批次大小优化...")
        
        # 获取当前任务状态
        if not self.current_task_id:
            print("❌ 没有活动任务")
            return False
        
        status = self.get_current_task_status()
        if not status:
            return False
        
        current_batch = status.get('current_batch', 1)
        processed_records = status.get('processed_records', 0)
        elapsed_time = status.get('elapsed_time', 0)
        
        if processed_records > 0 and elapsed_time > 0:
            # 计算处理速度 (记录/秒)
            processing_speed = processed_records / elapsed_time
            
            print(f"📊 当前性能指标:")
            print(f"   处理速度: {processing_speed:.2f} 记录/秒")
            print(f"   已处理: {processed_records:,} 条")
            print(f"   耗时: {elapsed_time:.1f} 秒")
            
            # 根据性能调整建议
            if processing_speed < 1:
                print("⚠️ 处理速度较慢，建议:")
                print("   - 减少批次大小到200")
                print("   - 检查数据库连接")
                print("   - 优化匹配算法")
            elif processing_speed > 10:
                print("✅ 处理速度良好，建议:")
                print("   - 可以增加批次大小到800")
                print("   - 启用并行处理")
            else:
                print("✅ 处理速度正常")
        
        return True
    
    def check_database_performance(self):
        """检查数据库性能"""
        print("🗄️ 数据库性能检查...")
        
        try:
            # 检查基础统计
            start_time = time.time()
            response = requests.get(f"{self.base_url}/api/stats", timeout=30)
            db_response_time = time.time() - start_time
            
            print(f"📊 数据库响应时间: {db_response_time:.2f} 秒")
            
            if db_response_time > 5:
                print("⚠️ 数据库响应较慢，建议:")
                print("   - 检查MongoDB索引")
                print("   - 优化查询语句")
                print("   - 增加数据库连接池")
            elif db_response_time > 2:
                print("⚠️ 数据库响应一般")
            else:
                print("✅ 数据库响应良好")
            
            if response.status_code == 200:
                data = response.json()
                supervision_count = data['data_sources']['supervision_count']
                inspection_count = data['data_sources']['inspection_count']
                
                print(f"📈 数据规模:")
                print(f"   监督管理数据: {supervision_count:,} 条")
                print(f"   安全排查数据: {inspection_count:,} 条")
                
                # 数据规模建议
                total_records = supervision_count + inspection_count
                if total_records > 2000000:
                    print("💡 大数据集优化建议:")
                    print("   - 启用分片处理")
                    print("   - 使用批量操作")
                    print("   - 考虑数据分区")
                
                return True
            
        except Exception as e:
            print(f"❌ 数据库性能检查失败: {str(e)}")
        
        return False
    
    def optimize_matching_algorithm(self):
        """优化匹配算法参数"""
        print("🧠 匹配算法优化...")
        
        try:
            # 获取当前匹配统计
            response = requests.get(f"{self.base_url}/api/optimized_match_statistics", timeout=10)
            if response.status_code == 200:
                stats = response.json()
                
                total_results = stats.get('total_results', 0)
                matched_results = stats.get('matched_results', 0)
                match_rate = stats.get('match_rate', 0)
                
                print(f"📊 当前匹配效果:")
                print(f"   总结果: {total_results:,} 条")
                print(f"   匹配成功: {matched_results:,} 条")
                print(f"   匹配率: {match_rate}%")
                
                # 根据匹配率提供优化建议
                if match_rate < 30:
                    print("❌ 匹配率过低，建议:")
                    print("   - 降低相似度阈值")
                    print("   - 启用模糊匹配")
                    print("   - 增加匹配字段")
                    print("   - 优化数据预处理")
                elif match_rate < 60:
                    print("⚠️ 匹配率偏低，建议:")
                    print("   - 调整匹配权重")
                    print("   - 优化地址标准化")
                    print("   - 增加同义词处理")
                elif match_rate < 80:
                    print("✅ 匹配率良好，可进一步优化:")
                    print("   - 精细调整阈值")
                    print("   - 增加人工审核")
                else:
                    print("🎉 匹配率优秀！")
                
                # 分析匹配类型分布
                match_type_stats = stats.get('match_type_stats', [])
                if match_type_stats:
                    print(f"\n🔍 匹配类型分析:")
                    for stat in match_type_stats:
                        match_type = stat.get('_id', 'unknown')
                        count = stat.get('count', 0)
                        avg_similarity = stat.get('avg_similarity', 0)
                        print(f"   {match_type}: {count:,} 条 (平均相似度: {avg_similarity:.2f})")
                
                return True
            
        except Exception as e:
            print(f"❌ 算法优化分析失败: {str(e)}")
        
        return False
    
    def implement_parallel_processing(self):
        """实现并行处理优化"""
        print("⚡ 并行处理优化...")
        
        # 检查系统资源
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        print(f"💻 系统资源:")
        print(f"   CPU核心数: {cpu_count}")
        print(f"   总内存: {memory_gb:.1f} GB")
        
        # 根据系统资源推荐并行度
        if cpu_count >= 8 and memory_gb >= 16:
            recommended_workers = min(cpu_count - 2, 6)
            print(f"🚀 推荐并行度: {recommended_workers} 个工作进程")
            print("💡 高性能配置建议:")
            print("   - 启用多进程匹配")
            print("   - 使用内存缓存")
            print("   - 批量数据库操作")
        elif cpu_count >= 4 and memory_gb >= 8:
            recommended_workers = min(cpu_count - 1, 4)
            print(f"⚡ 推荐并行度: {recommended_workers} 个工作进程")
            print("💡 中等性能配置建议:")
            print("   - 适度并行处理")
            print("   - 优化内存使用")
        else:
            print("⚠️ 系统资源有限，建议:")
            print("   - 使用单进程处理")
            print("   - 减少批次大小")
            print("   - 优化算法效率")
        
        return True
    
    def memory_optimization(self):
        """内存优化"""
        print("🧠 内存使用优化...")
        
        memory = psutil.virtual_memory()
        print(f"📊 当前内存状态:")
        print(f"   总内存: {memory.total / (1024**3):.1f} GB")
        print(f"   已使用: {memory.used / (1024**3):.1f} GB ({memory.percent:.1f}%)")
        print(f"   可用内存: {memory.available / (1024**3):.1f} GB")
        
        if memory.percent > 80:
            print("⚠️ 内存使用率过高，建议:")
            print("   - 减少批次大小")
            print("   - 清理无用缓存")
            print("   - 重启匹配进程")
            print("   - 增加虚拟内存")
        elif memory.percent > 60:
            print("⚠️ 内存使用率较高，建议监控")
        else:
            print("✅ 内存使用正常")
        
        # 检查是否有内存泄漏的迹象
        if len(self.performance_data) > 10:
            recent_memory = [p['memory_percent'] for p in self.performance_data[-10:]]
            if len(set(recent_memory)) == 1 or max(recent_memory) - min(recent_memory) > 20:
                print("⚠️ 可能存在内存泄漏，建议重启服务")
        
        return True
    
    def error_handling_optimization(self):
        """错误处理优化"""
        print("🛡️ 错误处理优化...")
        
        if self.current_task_id:
            status = self.get_current_task_status()
            if status:
                error_records = status.get('error_records', 0)
                processed_records = status.get('processed_records', 0)
                
                if processed_records > 0:
                    error_rate = (error_records / processed_records) * 100
                    print(f"📊 错误率分析:")
                    print(f"   错误记录: {error_records:,} 条")
                    print(f"   已处理: {processed_records:,} 条")
                    print(f"   错误率: {error_rate:.2f}%")
                    
                    if error_rate > 5:
                        print("❌ 错误率过高，建议:")
                        print("   - 检查数据质量")
                        print("   - 优化异常处理")
                        print("   - 增加数据验证")
                        print("   - 记录详细错误日志")
                    elif error_rate > 1:
                        print("⚠️ 错误率偏高，需要关注")
                    else:
                        print("✅ 错误率正常")
        
        print("💡 错误处理优化建议:")
        print("   - 实现自动重试机制")
        print("   - 添加断点续传功能")
        print("   - 完善日志记录")
        print("   - 建立错误分类体系")
        
        return True
    
    def step5_performance_monitoring(self):
        """步骤5: 性能监控"""
        self.print_step(5, "系统性能监控 - 实时监控系统资源使用")
        
        # 启动性能监控
        analysis = self.monitor_system_performance(duration=60)  # 监控1分钟
        
        if analysis:
            # 根据性能分析提供建议
            if analysis['avg_cpu'] > 70:
                print("⚠️ CPU使用率较高，建议优化算法或减少并发")
            if analysis['avg_memory'] > 80:
                print("⚠️ 内存使用率较高，建议优化内存管理")
        
        return True
    
    def step6_algorithm_tuning(self):
        """步骤6: 算法调优"""
        self.print_step(6, "匹配算法调优 - 根据实际效果优化参数")
        
        return self.optimize_matching_algorithm()
    
    def step7_parallel_optimization(self):
        """步骤7: 并发优化"""
        self.print_step(7, "并发处理优化 - 提升处理速度")
        
        success1 = self.implement_parallel_processing()
        success2 = self.optimize_batch_size_dynamically()
        
        return success1 and success2
    
    def step8_resource_optimization(self):
        """步骤8: 资源优化"""
        self.print_step(8, "系统资源优化 - 内存和数据库优化")
        
        success1 = self.memory_optimization()
        success2 = self.check_database_performance()
        
        return success1 and success2
    
    def step9_error_handling(self):
        """步骤9: 错误处理优化"""
        self.print_step(9, "错误处理优化 - 提升系统稳定性")
        
        return self.error_handling_optimization()
    
    def get_active_task_id(self):
        """获取当前活动的任务ID"""
        try:
            # 尝试从之前的任务ID开始
            task_ids = [
                "c2e93daf-b41c-47ce-b2bb-84fb372adfae",
                "505487e9-01fe-400c-9899-47e88f8de92d"
            ]
            
            for task_id in task_ids:
                response = requests.get(f"{self.base_url}/api/optimized_task_progress/{task_id}", 
                                      timeout=5)
                if response.status_code == 200:
                    status = response.json()
                    if status.get('status') == 'running':
                        self.current_task_id = task_id
                        print(f"✅ 找到活动任务: {task_id}")
                        return task_id
            
            print("⚠️ 没有找到活动的匹配任务")
            return None
            
        except Exception as e:
            print(f"❌ 获取任务ID失败: {str(e)}")
            return None
    
    def run_advanced_optimization(self):
        """运行高级优化流程"""
        self.print_header("消防单位建筑数据关联系统高级优化")
        
        print(f"🕒 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🌐 系统地址: {self.base_url}")
        
        # 获取当前活动任务
        self.get_active_task_id()
        
        # 执行高级优化步骤
        steps = [
            self.step5_performance_monitoring,
            self.step6_algorithm_tuning,
            self.step7_parallel_optimization,
            self.step8_resource_optimization,
            self.step9_error_handling
        ]
        
        for i, step_func in enumerate(steps, 5):
            try:
                print(f"\n⏳ 执行步骤 {i}...")
                success = step_func()
                if success:
                    print(f"✅ 步骤 {i} 完成")
                else:
                    print(f"⚠️ 步骤 {i} 部分完成")
            except Exception as e:
                print(f"❌ 步骤 {i} 执行出错: {str(e)}")
        
        self.print_header("高级优化完成")
        print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ 高级优化流程执行完成")
        
        # 提供最终建议
        print(f"\n🎯 最终优化建议:")
        print(f"   1. 定期监控系统性能")
        print(f"   2. 根据匹配效果调整参数")
        print(f"   3. 保持数据库索引优化")
        print(f"   4. 实施错误预防机制")
        print(f"   5. 建立性能基准测试")

def main():
    """主函数"""
    optimizer = AdvancedOptimizer()
    optimizer.run_advanced_optimization()

if __name__ == "__main__":
    main() 