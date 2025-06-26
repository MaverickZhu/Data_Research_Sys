#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统处理速度优化脚本
解决当前处理速度过慢的问题，提升到合理的处理速度
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

class SpeedOptimizer:
    def __init__(self):
        self.base_url = BASE_URL
        self.current_task_id = None
        
    def print_header(self, title):
        """打印标题"""
        print("\n" + "=" * 70)
        print(f"🚀 {title}")
        print("=" * 70)
    
    def get_current_task_status(self):
        """获取当前任务状态"""
        task_ids = [
            "c2e93daf-b41c-47ce-b2bb-84fb372adfae",
            "505487e9-01fe-400c-9899-47e88f8de92d"
        ]
        
        for task_id in task_ids:
            try:
                response = requests.get(f"{self.base_url}/api/optimized_task_progress/{task_id}", 
                                      timeout=5)
                if response.status_code == 200:
                    status = response.json()
                    if status.get('status') == 'running':
                        self.current_task_id = task_id
                        return status
            except:
                continue
        return None
    
    def stop_current_task(self):
        """停止当前任务"""
        if not self.current_task_id:
            print("❌ 没有找到活动任务")
            return False
        
        try:
            response = requests.post(f"{self.base_url}/api/stop_optimized_matching", 
                                   json={"task_id": self.current_task_id}, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    print(f"✅ 任务 {self.current_task_id[:8]}... 已停止")
                    return True
                else:
                    print(f"❌ 停止任务失败: {data.get('error', '未知错误')}")
            else:
                print(f"❌ 停止任务失败: HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ 停止任务失败: {str(e)}")
        
        return False
    
    def start_optimized_task(self, batch_size=1000, mode="incremental"):
        """启动优化后的任务"""
        try:
            payload = {
                "match_type": "both",
                "mode": mode,
                "batch_size": batch_size
            }
            
            response = requests.post(f"{self.base_url}/api/start_optimized_matching", 
                                   json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    self.current_task_id = data.get('task_id')
                    print(f"✅ 优化任务启动成功")
                    print(f"   任务ID: {self.current_task_id}")
                    print(f"   批次大小: {batch_size}")
                    print(f"   模式: {mode}")
                    return True
                else:
                    print(f"❌ 启动失败: {data.get('error', '未知错误')}")
            else:
                print(f"❌ 启动失败: HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ 启动失败: {str(e)}")
        
        return False
    
    def monitor_speed_improvement(self, duration=180):
        """监控速度改进效果"""
        if not self.current_task_id:
            print("❌ 没有活动任务可监控")
            return False
        
        print(f"📊 开始监控速度改进效果 ({duration}秒)...")
        
        start_time = time.time()
        last_processed = 0
        speed_history = []
        
        while time.time() - start_time < duration:
            try:
                response = requests.get(f"{self.base_url}/api/optimized_task_progress/{self.current_task_id}", 
                                      timeout=10)
                
                if response.status_code == 200:
                    progress = response.json()
                    
                    current_processed = progress.get('processed_records', 0)
                    elapsed_time = progress.get('elapsed_time', 0)
                    status = progress.get('status', 'unknown')
                    
                    # 计算当前速度
                    if elapsed_time > 0:
                        current_speed = current_processed / elapsed_time
                        speed_history.append(current_speed)
                        
                        # 计算增量速度（最近10秒的处理速度）
                        if last_processed > 0:
                            increment = current_processed - last_processed
                            increment_speed = increment / 10  # 10秒间隔
                            
                            print(f"📈 实时监控:")
                            print(f"   已处理: {current_processed:,} 条")
                            print(f"   总体速度: {current_speed:.2f} 记录/秒")
                            print(f"   增量速度: {increment_speed:.2f} 记录/秒")
                            print(f"   状态: {status}")
                            
                            # 速度评估
                            if increment_speed > 5:
                                print(f"   评估: 🟢 速度优秀")
                            elif increment_speed > 1:
                                print(f"   评估: 🟡 速度良好")
                            else:
                                print(f"   评估: 🔴 速度需要改进")
                        
                        last_processed = current_processed
                    
                    # 检查任务状态
                    if status in ['completed', 'error', 'stopped']:
                        print(f"📋 任务状态变更: {status}")
                        break
                        
                else:
                    print(f"❌ 获取进度失败: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"❌ 监控出错: {str(e)}")
            
            print("-" * 50)
            time.sleep(10)  # 每10秒检查一次
        
        # 分析速度改进效果
        if speed_history:
            avg_speed = sum(speed_history) / len(speed_history)
            max_speed = max(speed_history)
            
            print(f"\n📊 速度改进分析:")
            print(f"   平均速度: {avg_speed:.2f} 记录/秒")
            print(f"   最高速度: {max_speed:.2f} 记录/秒")
            print(f"   监控样本: {len(speed_history)} 次")
            
            # 改进评估
            if avg_speed > 5:
                print(f"   改进效果: 🟢 显著改进")
            elif avg_speed > 1:
                print(f"   改进效果: 🟡 有所改进")
            else:
                print(f"   改进效果: 🔴 需要进一步优化")
        
        return True
    
    def optimize_processing_speed(self):
        """优化处理速度"""
        self.print_header("处理速度优化")
        
        print("🔍 分析当前任务状态...")
        current_status = self.get_current_task_status()
        
        if current_status:
            processed = current_status.get('processed_records', 0)
            elapsed = current_status.get('elapsed_time', 0)
            current_speed = processed / elapsed if elapsed > 0 else 0
            
            print(f"📊 当前性能:")
            print(f"   已处理: {processed:,} 条")
            print(f"   耗时: {elapsed:.1f} 秒")
            print(f"   当前速度: {current_speed:.3f} 记录/秒")
            
            if current_speed < 0.1:
                print(f"🔴 速度严重偏低，需要立即优化")
                
                print(f"\n🛑 停止当前低效任务...")
                if self.stop_current_task():
                    time.sleep(3)  # 等待任务完全停止
                    
                    print(f"\n🚀 启动优化任务...")
                    # 使用更大的批次大小
                    if self.start_optimized_task(batch_size=1000, mode="incremental"):
                        print(f"\n📊 开始监控优化效果...")
                        self.monitor_speed_improvement(duration=180)  # 监控3分钟
                        return True
                else:
                    print(f"❌ 无法停止当前任务，请手动处理")
                    return False
            else:
                print(f"✅ 当前速度可接受，继续监控")
                self.monitor_speed_improvement(duration=120)  # 监控2分钟
                return True
        else:
            print(f"❌ 没有找到活动任务")
            print(f"\n🚀 启动新的优化任务...")
            if self.start_optimized_task(batch_size=1000, mode="incremental"):
                print(f"\n📊 开始监控任务性能...")
                self.monitor_speed_improvement(duration=180)
                return True
            return False
    
    def provide_speed_recommendations(self):
        """提供速度优化建议"""
        print(f"\n💡 速度优化建议:")
        print(f"   1. 🔧 增加批次大小到1000-2000条")
        print(f"   2. ⚡ 启用多线程处理")
        print(f"   3. 🗄️ 优化数据库查询索引")
        print(f"   4. 🧠 增加内存缓存机制")
        print(f"   5. 📊 减少不必要的日志输出")
        print(f"   6. 🔄 实施连接池优化")
        print(f"   7. 📈 使用批量数据库操作")
        
        print(f"\n🎯 立即可执行的优化:")
        print(f"   - 重启任务使用1000条批次大小")
        print(f"   - 监控系统资源使用情况")
        print(f"   - 检查数据库连接状态")
    
    def run_speed_optimization(self):
        """运行速度优化流程"""
        self.print_header("消防单位建筑数据关联系统 - 处理速度优化")
        
        print(f"🕒 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 目标: 将处理速度从0.01记录/秒提升到1+记录/秒")
        
        # 执行速度优化
        success = self.optimize_processing_speed()
        
        # 提供优化建议
        self.provide_speed_recommendations()
        
        self.print_header("速度优化完成")
        print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if success:
            print(f"✅ 速度优化执行成功")
            print(f"📊 建议继续监控任务进度")
        else:
            print(f"⚠️ 速度优化部分完成")
            print(f"🔧 可能需要手动调整系统配置")
        
        print(f"🌐 访问系统: {self.base_url}")
        print(f"📈 查看进度: {self.base_url}/results")

def main():
    """主函数"""
    optimizer = SpeedOptimizer()
    optimizer.run_speed_optimization()

if __name__ == "__main__":
    main() 