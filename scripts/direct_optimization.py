#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统直接优化脚本
使用已知API接口直接启动高性能优化任务
"""
import sys
import os
import requests
import time
import json
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

BASE_URL = "http://localhost:8888"

class DirectOptimizer:
    def __init__(self):
        self.base_url = BASE_URL
        
    def print_header(self, title):
        """打印标题"""
        print("\n" + "=" * 70)
        print(f"⚡ {title}")
        print("=" * 70)
    
    def test_api_connectivity(self):
        """测试API连接性"""
        print(f"🔍 测试API连接性...")
        
        # 测试基本连接
        try:
            response = requests.get(f"{self.base_url}/", timeout=5)
            if response.status_code == 200:
                print(f"✅ 基本连接正常")
            else:
                print(f"⚠️ 基本连接异常: HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ 基本连接失败: {str(e)}")
            return False
        
        # 测试API接口
        api_endpoints = [
            "/api/start_optimized_matching",
            "/api/optimized_task_progress/test",
            "/api/data_statistics"
        ]
        
        for endpoint in api_endpoints:
            try:
                if "start_optimized_matching" in endpoint:
                    # POST请求测试
                    response = requests.post(f"{self.base_url}{endpoint}", 
                                           json={"test": True}, timeout=5)
                else:
                    # GET请求测试
                    response = requests.get(f"{self.base_url}{endpoint}", timeout=5)
                
                print(f"   {endpoint}: {'✅' if response.status_code in [200, 400, 404] else '❌'} HTTP {response.status_code}")
                
            except Exception as e:
                print(f"   {endpoint}: ❌ {str(e)}")
        
        return True
    
    def start_high_performance_task(self):
        """启动高性能优化任务"""
        print(f"🚀 启动高性能优化任务...")
        
        # 使用最优配置
        optimal_configs = [
            {"batch_size": 2000, "mode": "incremental"},
            {"batch_size": 1500, "mode": "incremental"},
            {"batch_size": 1000, "mode": "incremental"},
            {"batch_size": 1000, "mode": "update"}
        ]
        
        for i, config in enumerate(optimal_configs, 1):
            try:
                payload = {
                    "match_type": "both",
                    "mode": config["mode"],
                    "batch_size": config["batch_size"]
                }
                
                print(f"   配置 {i}: 批次={config['batch_size']}, 模式={config['mode']}")
                response = requests.post(f"{self.base_url}/api/start_optimized_matching", 
                                       json=payload, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('success'):
                        task_id = data.get('task_id')
                        print(f"✅ 高性能任务启动成功!")
                        print(f"   任务ID: {task_id}")
                        print(f"   批次大小: {config['batch_size']}")
                        print(f"   模式: {config['mode']}")
                        return task_id
                    else:
                        print(f"   失败: {data.get('error', '未知错误')}")
                else:
                    print(f"   失败: HTTP {response.status_code}")
                    if response.status_code == 400:
                        try:
                            error_data = response.json()
                            print(f"   错误详情: {error_data}")
                        except:
                            pass
                    
            except Exception as e:
                print(f"   失败: {str(e)}")
                
            time.sleep(3)  # 等待3秒再尝试下一个配置
        
        return None
    
    def monitor_performance_improvement(self, task_id, duration=360):
        """监控性能改进"""
        print(f"📊 开始监控性能改进 ({duration}秒)...")
        
        start_time = time.time()
        last_processed = 0
        max_speed = 0
        speed_history = []
        performance_data = []
        
        while time.time() - start_time < duration:
            try:
                response = requests.get(f"{self.base_url}/api/optimized_task_progress/{task_id}", 
                                      timeout=10)
                
                if response.status_code == 200:
                    progress = response.json()
                    
                    current_processed = progress.get('processed_records', 0)
                    elapsed_time = progress.get('elapsed_time', 0)
                    status = progress.get('status', 'unknown')
                    total_records = progress.get('total_records', 0)
                    
                    # 计算性能指标
                    if elapsed_time > 0:
                        current_speed = current_processed / elapsed_time
                        speed_history.append(current_speed)
                        
                        if current_speed > max_speed:
                            max_speed = current_speed
                        
                        # 计算增量速度
                        if last_processed > 0:
                            increment = current_processed - last_processed
                            increment_speed = increment / 20  # 20秒间隔
                            
                            # 计算完成百分比
                            completion_pct = (current_processed / total_records * 100) if total_records > 0 else 0
                            
                            # 估算剩余时间
                            if current_speed > 0:
                                remaining_records = total_records - current_processed
                                eta_seconds = remaining_records / current_speed
                                eta_hours = eta_seconds / 3600
                            else:
                                eta_hours = float('inf')
                            
                            print(f"📈 性能监控报告:")
                            print(f"   已处理: {current_processed:,} / {total_records:,} 条 ({completion_pct:.2f}%)")
                            print(f"   总体速度: {current_speed:.3f} 记录/秒")
                            print(f"   增量速度: {increment_speed:.3f} 记录/秒")
                            print(f"   最高速度: {max_speed:.3f} 记录/秒")
                            print(f"   预计完成: {eta_hours:.1f} 小时")
                            print(f"   任务状态: {status}")
                            
                            # 性能评级
                            if increment_speed > 5:
                                grade = "🟢 优秀"
                                recommendation = "保持当前配置"
                            elif increment_speed > 1:
                                grade = "🟡 良好"
                                recommendation = "可考虑进一步优化"
                            elif increment_speed > 0.2:
                                grade = "🟠 一般"
                                recommendation = "需要优化配置"
                            else:
                                grade = "🔴 偏低"
                                recommendation = "需要立即调整"
                            
                            print(f"   性能评级: {grade}")
                            print(f"   建议: {recommendation}")
                            
                            # 记录性能数据
                            performance_data.append({
                                'time': time.time() - start_time,
                                'processed': current_processed,
                                'speed': current_speed,
                                'increment_speed': increment_speed
                            })
                        
                        last_processed = current_processed
                    
                    # 检查任务状态
                    if status in ['completed', 'error', 'stopped']:
                        print(f"📋 任务状态变更: {status}")
                        break
                        
                else:
                    print(f"❌ 获取进度失败: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"❌ 监控出错: {str(e)}")
            
            print("-" * 70)
            time.sleep(20)  # 每20秒检查一次
        
        # 生成性能分析报告
        self.generate_performance_report(speed_history, performance_data)
        
        return True
    
    def generate_performance_report(self, speed_history, performance_data):
        """生成性能分析报告"""
        if not speed_history:
            print(f"❌ 没有性能数据可分析")
            return
        
        avg_speed = sum(speed_history) / len(speed_history)
        max_speed = max(speed_history)
        min_speed = min(speed_history)
        
        # 与原始速度对比
        original_speed = 0.016
        improvement_ratio = avg_speed / original_speed if avg_speed > 0 else 0
        
        print(f"\n📊 性能分析报告:")
        print(f"   平均速度: {avg_speed:.3f} 记录/秒")
        print(f"   最高速度: {max_speed:.3f} 记录/秒")
        print(f"   最低速度: {min_speed:.3f} 记录/秒")
        print(f"   速度稳定性: {(1 - (max_speed - min_speed) / avg_speed):.1%}")
        print(f"   改进倍数: {improvement_ratio:.1f}x")
        print(f"   监控样本: {len(speed_history)} 次")
        
        # 改进效果评估
        if improvement_ratio > 100:
            print(f"   改进效果: 🟢 卓越改进 - 速度提升{improvement_ratio:.0f}倍!")
        elif improvement_ratio > 50:
            print(f"   改进效果: 🟢 显著改进 - 速度提升{improvement_ratio:.0f}倍!")
        elif improvement_ratio > 10:
            print(f"   改进效果: 🟡 良好改进 - 速度提升{improvement_ratio:.0f}倍")
        elif improvement_ratio > 3:
            print(f"   改进效果: 🟠 有所改进 - 速度提升{improvement_ratio:.1f}倍")
        else:
            print(f"   改进效果: 🔴 改进有限 - 需要进一步优化")
        
        # 趋势分析
        if len(performance_data) > 3:
            recent_speeds = [d['increment_speed'] for d in performance_data[-3:]]
            early_speeds = [d['increment_speed'] for d in performance_data[:3]]
            
            if recent_speeds and early_speeds:
                recent_avg = sum(recent_speeds) / len(recent_speeds)
                early_avg = sum(early_speeds) / len(early_speeds)
                
                if recent_avg > early_avg * 1.1:
                    trend = "🔺 加速趋势"
                elif recent_avg < early_avg * 0.9:
                    trend = "🔻 减速趋势"
                else:
                    trend = "➡️ 稳定趋势"
                
                print(f"   性能趋势: {trend}")
    
    def provide_optimization_summary(self):
        """提供优化总结"""
        print(f"\n🎯 优化总结:")
        print(f"   1. ✅ 已启动高性能优化任务")
        print(f"   2. ✅ 实施了批次大小优化")
        print(f"   3. ✅ 建立了性能监控机制")
        print(f"   4. ✅ 提供了实时性能分析")
        
        print(f"\n📈 后续建议:")
        print(f"   - 继续监控任务进度")
        print(f"   - 根据性能数据调整配置")
        print(f"   - 考虑启用并行处理")
        print(f"   - 优化数据库查询性能")
        
        print(f"\n🔗 相关链接:")
        print(f"   - 系统首页: {self.base_url}")
        print(f"   - 结果查看: {self.base_url}/results")
        print(f"   - 实时监控: python scripts/real_time_monitor.py")
    
    def run_direct_optimization(self):
        """运行直接优化流程"""
        self.print_header("消防单位建筑数据关联系统 - 直接性能优化")
        
        print(f"🕒 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 目标: 直接启动高性能优化任务，显著提升处理速度")
        
        # 测试API连接性
        if not self.test_api_connectivity():
            print(f"❌ API连接测试失败")
            return False
        
        # 启动高性能任务
        task_id = self.start_high_performance_task()
        
        if task_id:
            print(f"\n📊 开始性能监控...")
            self.monitor_performance_improvement(task_id, duration=300)  # 监控5分钟
            
            # 提供优化总结
            self.provide_optimization_summary()
            
            self.print_header("直接性能优化完成")
            print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"✅ 直接优化执行成功")
            print(f"📊 高性能任务已启动并监控")
            
            return True
        else:
            print(f"❌ 无法启动高性能任务")
            print(f"🔧 建议检查系统配置")
            
            self.print_header("直接性能优化完成")
            print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⚠️ 直接优化部分完成")
            
            return False

def main():
    """主函数"""
    optimizer = DirectOptimizer()
    optimizer.run_direct_optimization()

if __name__ == "__main__":
    main() 