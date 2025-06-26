#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统手动速度修复脚本
绕过API问题，直接启动优化任务
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

class ManualSpeedFix:
    def __init__(self):
        self.base_url = BASE_URL
        
    def print_header(self, title):
        """打印标题"""
        print("\n" + "=" * 70)
        print(f"🔧 {title}")
        print("=" * 70)
    
    def check_system_status(self):
        """检查系统状态"""
        try:
            response = requests.get(f"{self.base_url}/api/system_status", timeout=10)
            if response.status_code == 200:
                status = response.json()
                print(f"✅ 系统状态正常")
                print(f"   数据库连接: {'正常' if status.get('database_connected') else '异常'}")
                print(f"   Redis连接: {'正常' if status.get('redis_connected') else '异常'}")
                return True
            else:
                print(f"❌ 系统状态检查失败: HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ 系统状态检查失败: {str(e)}")
        return False
    
    def start_new_optimized_task(self):
        """启动新的优化任务"""
        print(f"🚀 启动新的高速优化任务...")
        
        # 尝试不同的批次大小
        batch_sizes = [2000, 1500, 1000, 800]
        
        for batch_size in batch_sizes:
            try:
                payload = {
                    "match_type": "both",
                    "mode": "incremental", 
                    "batch_size": batch_size
                }
                
                print(f"   尝试批次大小: {batch_size}")
                response = requests.post(f"{self.base_url}/api/start_optimized_matching", 
                                       json=payload, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('success'):
                        task_id = data.get('task_id')
                        print(f"✅ 优化任务启动成功!")
                        print(f"   任务ID: {task_id}")
                        print(f"   批次大小: {batch_size}")
                        print(f"   模式: incremental")
                        return task_id
                    else:
                        print(f"   失败: {data.get('error', '未知错误')}")
                else:
                    print(f"   失败: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"   失败: {str(e)}")
                
            time.sleep(2)  # 等待2秒再尝试下一个批次大小
        
        return None
    
    def monitor_new_task(self, task_id, duration=300):
        """监控新任务的性能"""
        print(f"📊 开始监控新任务性能 ({duration}秒)...")
        
        start_time = time.time()
        last_processed = 0
        best_speed = 0
        speed_samples = []
        
        while time.time() - start_time < duration:
            try:
                response = requests.get(f"{self.base_url}/api/optimized_task_progress/{task_id}", 
                                      timeout=10)
                
                if response.status_code == 200:
                    progress = response.json()
                    
                    current_processed = progress.get('processed_records', 0)
                    elapsed_time = progress.get('elapsed_time', 0)
                    status = progress.get('status', 'unknown')
                    
                    # 计算速度
                    if elapsed_time > 0:
                        current_speed = current_processed / elapsed_time
                        speed_samples.append(current_speed)
                        
                        if current_speed > best_speed:
                            best_speed = current_speed
                        
                        # 计算增量速度
                        if last_processed > 0:
                            increment = current_processed - last_processed
                            increment_speed = increment / 15  # 15秒间隔
                            
                            print(f"📈 性能监控:")
                            print(f"   已处理: {current_processed:,} 条")
                            print(f"   总体速度: {current_speed:.3f} 记录/秒")
                            print(f"   增量速度: {increment_speed:.3f} 记录/秒")
                            print(f"   最佳速度: {best_speed:.3f} 记录/秒")
                            print(f"   状态: {status}")
                            
                            # 速度评估
                            if increment_speed > 2:
                                print(f"   评估: 🟢 速度优秀 - 目标达成!")
                            elif increment_speed > 0.5:
                                print(f"   评估: 🟡 速度良好 - 继续优化")
                            elif increment_speed > 0.1:
                                print(f"   评估: 🟠 速度一般 - 需要改进")
                            else:
                                print(f"   评估: 🔴 速度偏低 - 需要调整")
                        
                        last_processed = current_processed
                    
                    # 检查任务状态
                    if status in ['completed', 'error', 'stopped']:
                        print(f"📋 任务状态变更: {status}")
                        break
                        
                else:
                    print(f"❌ 获取进度失败: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"❌ 监控出错: {str(e)}")
            
            print("-" * 60)
            time.sleep(15)  # 每15秒检查一次
        
        # 分析性能改进
        if speed_samples:
            avg_speed = sum(speed_samples) / len(speed_samples)
            improvement_ratio = avg_speed / 0.016 if avg_speed > 0 else 0  # 与原始0.016对比
            
            print(f"\n📊 性能改进分析:")
            print(f"   平均速度: {avg_speed:.3f} 记录/秒")
            print(f"   最佳速度: {best_speed:.3f} 记录/秒")
            print(f"   改进倍数: {improvement_ratio:.1f}x")
            print(f"   监控样本: {len(speed_samples)} 次")
            
            if improvement_ratio > 50:
                print(f"   改进效果: 🟢 显著改进 - 速度提升{improvement_ratio:.0f}倍!")
            elif improvement_ratio > 10:
                print(f"   改进效果: 🟡 良好改进 - 速度提升{improvement_ratio:.0f}倍")
            elif improvement_ratio > 2:
                print(f"   改进效果: 🟠 有所改进 - 速度提升{improvement_ratio:.1f}倍")
            else:
                print(f"   改进效果: 🔴 改进有限 - 需要进一步优化")
        
        return True
    
    def provide_next_steps(self):
        """提供下一步建议"""
        print(f"\n🎯 下一步优化建议:")
        print(f"   1. 🔄 如果速度仍然偏低，考虑重启系统")
        print(f"   2. 📊 监控系统资源使用情况")
        print(f"   3. 🗄️ 检查数据库索引优化")
        print(f"   4. ⚡ 考虑启用并行处理")
        print(f"   5. 🧠 增加内存缓存机制")
        
        print(f"\n📈 持续监控:")
        print(f"   - 使用实时监控脚本: python scripts/real_time_monitor.py")
        print(f"   - 查看系统状态: {self.base_url}/api/system_status")
        print(f"   - 访问结果页面: {self.base_url}/results")
    
    def run_manual_fix(self):
        """运行手动修复流程"""
        self.print_header("消防单位建筑数据关联系统 - 手动速度修复")
        
        print(f"🕒 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 目标: 绕过API问题，直接启动高速优化任务")
        
        # 检查系统状态
        if not self.check_system_status():
            print(f"❌ 系统状态异常，请检查系统连接")
            return False
        
        # 启动新的优化任务
        task_id = self.start_new_optimized_task()
        
        if task_id:
            print(f"\n📊 开始监控新任务性能...")
            self.monitor_new_task(task_id, duration=240)  # 监控4分钟
            
            # 提供下一步建议
            self.provide_next_steps()
            
            self.print_header("手动速度修复完成")
            print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"✅ 手动修复执行成功")
            print(f"📊 新任务已启动，建议继续监控")
            
            return True
        else:
            print(f"❌ 无法启动新的优化任务")
            print(f"🔧 建议检查系统配置或重启应用")
            
            self.print_header("手动速度修复完成")
            print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⚠️ 手动修复部分完成")
            
            return False

def main():
    """主函数"""
    fixer = ManualSpeedFix()
    fixer.run_manual_fix()

if __name__ == "__main__":
    main() 