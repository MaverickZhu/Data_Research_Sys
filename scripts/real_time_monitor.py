#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统实时监控脚本
持续监控匹配进度、系统性能、错误率等关键指标
"""
import sys
import os
import requests
import time
import json
import psutil
from datetime import datetime
from collections import deque

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

BASE_URL = "http://localhost:8888"

class RealTimeMonitor:
    def __init__(self):
        self.base_url = BASE_URL
        self.active_task_id = None
        self.performance_history = deque(maxlen=60)  # 保留最近60次记录
        self.last_processed = 0
        self.start_time = datetime.now()
        
    def clear_screen(self):
        """清屏"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def get_active_task(self):
        """获取活动任务"""
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
                        self.active_task_id = task_id
                        return status
            except:
                continue
        return None
    
    def get_system_stats(self):
        """获取系统统计"""
        try:
            response = requests.get(f"{self.base_url}/api/stats", timeout=10)
            if response.status_code == 200:
                return response.json()
        except:
            pass
        return None
    
    def get_match_statistics(self):
        """获取匹配统计"""
        try:
            response = requests.get(f"{self.base_url}/api/optimized_match_statistics", timeout=10)
            if response.status_code == 200:
                return response.json()
        except:
            pass
        return None
    
    def get_system_performance(self):
        """获取系统性能"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'timestamp': datetime.now()
            }
        except:
            return None
    
    def format_time_duration(self, seconds):
        """格式化时间持续时间"""
        if seconds < 60:
            return f"{seconds:.1f}秒"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}分钟"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}小时"
    
    def format_number(self, num):
        """格式化数字"""
        if num >= 1000000:
            return f"{num/1000000:.1f}M"
        elif num >= 1000:
            return f"{num/1000:.1f}K"
        else:
            return str(num)
    
    def calculate_eta(self, processed, total, elapsed_time):
        """计算预计完成时间"""
        if processed <= 0 or elapsed_time <= 0:
            return "未知"
        
        rate = processed / elapsed_time
        remaining = total - processed
        eta_seconds = remaining / rate
        
        return self.format_time_duration(eta_seconds)
    
    def display_header(self):
        """显示标题"""
        print("=" * 80)
        print("🔥 消防单位建筑数据关联系统 - 实时监控面板")
        print("=" * 80)
        print(f"📅 监控时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ 运行时长: {self.format_time_duration((datetime.now() - self.start_time).total_seconds())}")
        print(f"🌐 系统地址: {self.base_url}")
        print("-" * 80)
    
    def display_task_progress(self, task_status):
        """显示任务进度"""
        print("📊 匹配任务进度")
        print("-" * 40)
        
        if task_status:
            task_id = task_status.get('task_id', 'N/A')
            status = task_status.get('status', 'unknown')
            progress = task_status.get('progress_percent', 0)
            processed = task_status.get('processed_records', 0)
            matched = task_status.get('matched_records', 0)
            total = task_status.get('total_records', 0)
            elapsed = task_status.get('elapsed_time', 0)
            match_rate = task_status.get('match_rate', 0)
            
            # 计算处理速度
            speed = processed / elapsed if elapsed > 0 else 0
            eta = self.calculate_eta(processed, total, elapsed)
            
            print(f"任务ID: {task_id[:8]}...")
            print(f"状态: {'🟢 运行中' if status == 'running' else '🔴 ' + status}")
            print(f"进度: {progress:.2f}% [{processed:,}/{total:,}]")
            print(f"匹配: {matched:,} 条 (匹配率: {match_rate:.1f}%)")
            print(f"速度: {speed:.2f} 记录/秒")
            print(f"耗时: {self.format_time_duration(elapsed)}")
            print(f"预计: {eta}")
            
            # 进度条
            bar_length = 50
            filled_length = int(bar_length * progress / 100)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            print(f"进度条: |{bar}| {progress:.1f}%")
            
        else:
            print("❌ 没有活动的匹配任务")
    
    def display_system_performance(self):
        """显示系统性能"""
        print("\n💻 系统性能")
        print("-" * 40)
        
        perf = self.get_system_performance()
        if perf:
            self.performance_history.append(perf)
            
            cpu = perf['cpu_percent']
            memory = perf['memory_percent']
            memory_gb = perf['memory_available_gb']
            
            # CPU状态
            cpu_status = "🟢" if cpu < 50 else "🟡" if cpu < 80 else "🔴"
            print(f"CPU使用率: {cpu_status} {cpu:.1f}%")
            
            # 内存状态
            mem_status = "🟢" if memory < 60 else "🟡" if memory < 80 else "🔴"
            print(f"内存使用率: {mem_status} {memory:.1f}% (可用: {memory_gb:.1f}GB)")
            
            # 性能趋势
            if len(self.performance_history) > 10:
                recent_cpu = [p['cpu_percent'] for p in list(self.performance_history)[-10:]]
                cpu_trend = "📈" if recent_cpu[-1] > recent_cpu[0] else "📉" if recent_cpu[-1] < recent_cpu[0] else "➡️"
                print(f"CPU趋势: {cpu_trend}")
        else:
            print("❌ 无法获取系统性能数据")
    
    def display_data_statistics(self):
        """显示数据统计"""
        print("\n📈 数据统计")
        print("-" * 40)
        
        stats = self.get_system_stats()
        if stats:
            supervision_count = stats['data_sources']['supervision_count']
            inspection_count = stats['data_sources']['inspection_count']
            match_results_count = stats['data_sources']['match_results_count']
            
            print(f"监督管理数据: {self.format_number(supervision_count)} 条")
            print(f"安全排查数据: {self.format_number(inspection_count)} 条")
            print(f"匹配结果: {self.format_number(match_results_count)} 条")
            
            # 处理进度
            if supervision_count > 0:
                progress = (match_results_count / supervision_count) * 100
                print(f"整体进度: {progress:.2f}%")
        else:
            print("❌ 无法获取数据统计")
    
    def display_match_quality(self):
        """显示匹配质量"""
        print("\n🎯 匹配质量")
        print("-" * 40)
        
        match_stats = self.get_match_statistics()
        if match_stats:
            total_results = match_stats.get('total_results', 0)
            matched_results = match_stats.get('matched_results', 0)
            match_rate = match_stats.get('match_rate', 0)
            
            if total_results > 0:
                quality_status = "🟢" if match_rate >= 80 else "🟡" if match_rate >= 60 else "🔴"
                print(f"匹配率: {quality_status} {match_rate:.1f}%")
                print(f"成功匹配: {self.format_number(matched_results)} 条")
                print(f"总结果: {self.format_number(total_results)} 条")
                
                # 匹配类型分布
                match_type_stats = match_stats.get('match_type_stats', [])
                if match_type_stats:
                    print("匹配类型分布:")
                    for stat in match_type_stats[:3]:  # 显示前3种类型
                        match_type = stat.get('_id', 'unknown')
                        count = stat.get('count', 0)
                        print(f"  {match_type}: {self.format_number(count)} 条")
            else:
                print("📊 暂无匹配结果")
        else:
            print("❌ 无法获取匹配统计")
    
    def display_alerts(self, task_status):
        """显示警告信息"""
        alerts = []
        
        # 检查任务状态
        if not task_status:
            alerts.append("⚠️ 没有活动的匹配任务")
        elif task_status.get('status') == 'error':
            alerts.append("🔴 匹配任务出现错误")
        
        # 检查系统性能
        perf = self.get_system_performance()
        if perf:
            if perf['cpu_percent'] > 80:
                alerts.append(f"🔴 CPU使用率过高: {perf['cpu_percent']:.1f}%")
            if perf['memory_percent'] > 85:
                alerts.append(f"🔴 内存使用率过高: {perf['memory_percent']:.1f}%")
            if perf['memory_available_gb'] < 1:
                alerts.append(f"🔴 可用内存不足: {perf['memory_available_gb']:.1f}GB")
        
        # 检查处理速度
        if task_status:
            elapsed = task_status.get('elapsed_time', 0)
            processed = task_status.get('processed_records', 0)
            if elapsed > 0 and processed > 0:
                speed = processed / elapsed
                if speed < 0.1:
                    alerts.append(f"🟡 处理速度较慢: {speed:.3f} 记录/秒")
        
        if alerts:
            print("\n🚨 系统警告")
            print("-" * 40)
            for alert in alerts:
                print(alert)
    
    def display_footer(self):
        """显示页脚"""
        print("\n" + "-" * 80)
        print("💡 提示: 按 Ctrl+C 退出监控")
        print("🔄 数据每5秒自动刷新")
    
    def run_monitor(self):
        """运行实时监控"""
        print("🚀 启动实时监控...")
        print("📊 正在初始化监控面板...")
        time.sleep(2)
        
        try:
            while True:
                # 清屏并显示监控面板
                self.clear_screen()
                
                # 获取任务状态
                task_status = self.get_active_task()
                
                # 显示各个部分
                self.display_header()
                self.display_task_progress(task_status)
                self.display_system_performance()
                self.display_data_statistics()
                self.display_match_quality()
                self.display_alerts(task_status)
                self.display_footer()
                
                # 等待5秒后刷新
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n\n🛑 监控已停止")
            print("📊 监控会话结束")
            print(f"⏱️ 总监控时长: {self.format_time_duration((datetime.now() - self.start_time).total_seconds())}")

def main():
    """主函数"""
    monitor = RealTimeMonitor()
    monitor.run_monitor()

if __name__ == "__main__":
    main() 