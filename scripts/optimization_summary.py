#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统优化总结报告
生成完整的优化成果和建议报告
"""
import sys
import os
import requests
import time
import json
import psutil
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

BASE_URL = "http://localhost:8888"

class OptimizationSummary:
    def __init__(self):
        self.base_url = BASE_URL
        self.report_data = {}
        
    def collect_system_info(self):
        """收集系统信息"""
        try:
            # 系统硬件信息
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            
            self.report_data['system_info'] = {
                'cpu_cores': cpu_count,
                'total_memory_gb': memory_gb,
                'current_cpu_usage': cpu_percent,
                'current_memory_usage': memory_percent,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            return True
        except Exception as e:
            print(f"❌ 收集系统信息失败: {str(e)}")
            return False
    
    def collect_data_statistics(self):
        """收集数据统计"""
        try:
            response = requests.get(f"{self.base_url}/api/stats", timeout=10)
            if response.status_code == 200:
                data = response.json()
                self.report_data['data_stats'] = data['data_sources']
                return True
        except Exception as e:
            print(f"❌ 收集数据统计失败: {str(e)}")
        return False
    
    def collect_match_statistics(self):
        """收集匹配统计"""
        try:
            response = requests.get(f"{self.base_url}/api/optimized_match_statistics", timeout=10)
            if response.status_code == 200:
                self.report_data['match_stats'] = response.json()
                return True
        except Exception as e:
            print(f"❌ 收集匹配统计失败: {str(e)}")
        return False
    
    def collect_task_progress(self):
        """收集任务进度"""
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
                    if status.get('status') in ['running', 'completed']:
                        self.report_data['task_progress'] = status
                        return True
            except:
                continue
        return False
    
    def generate_report_header(self):
        """生成报告头部"""
        print("=" * 100)
        print("🔥 消防单位建筑数据关联系统 - 优化总结报告")
        print("=" * 100)
        print(f"📅 报告生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
        print(f"🌐 系统地址: {self.base_url}")
        print(f"📊 报告版本: v2.0")
        print("=" * 100)
    
    def generate_optimization_achievements(self):
        """生成优化成果"""
        print("\n🎯 优化成果总览")
        print("-" * 80)
        
        achievements = [
            "✅ 完成基础优化流程 (步骤1-4)",
            "✅ 实施高级优化策略 (步骤5-9)", 
            "✅ 建立实时监控机制",
            "✅ 优化批次处理大小",
            "✅ 实现增量匹配机制",
            "✅ 完善错误处理机制",
            "✅ 提升系统性能监控",
            "✅ 优化内存使用策略",
            "✅ 改进数据库查询效率"
        ]
        
        for achievement in achievements:
            print(f"  {achievement}")
        
        print(f"\n📈 总计完成: {len(achievements)} 项优化")
    
    def generate_system_performance_report(self):
        """生成系统性能报告"""
        print("\n💻 系统性能报告")
        print("-" * 80)
        
        if 'system_info' in self.report_data:
            info = self.report_data['system_info']
            
            print(f"🖥️ 硬件配置:")
            print(f"   CPU核心数: {info['cpu_cores']} 核")
            print(f"   总内存: {info['total_memory_gb']:.1f} GB")
            
            print(f"\n📊 当前性能:")
            print(f"   CPU使用率: {info['current_cpu_usage']:.1f}%")
            print(f"   内存使用率: {info['current_memory_usage']:.1f}%")
            
            # 性能评估
            cpu_status = "优秀" if info['current_cpu_usage'] < 30 else "良好" if info['current_cpu_usage'] < 60 else "需要关注"
            memory_status = "优秀" if info['current_memory_usage'] < 40 else "良好" if info['current_memory_usage'] < 70 else "需要关注"
            
            print(f"\n🎯 性能评估:")
            print(f"   CPU状态: {cpu_status}")
            print(f"   内存状态: {memory_status}")
            
            # 推荐配置
            if info['cpu_cores'] >= 8 and info['total_memory_gb'] >= 16:
                print(f"   系统配置: 🟢 高性能配置，适合大规模数据处理")
            elif info['cpu_cores'] >= 4 and info['total_memory_gb'] >= 8:
                print(f"   系统配置: 🟡 中等配置，适合中等规模处理")
            else:
                print(f"   系统配置: 🔴 基础配置，建议升级硬件")
    
    def generate_data_analysis_report(self):
        """生成数据分析报告"""
        print("\n📊 数据分析报告")
        print("-" * 80)
        
        if 'data_stats' in self.report_data:
            stats = self.report_data['data_stats']
            
            supervision_count = stats.get('supervision_count', 0)
            inspection_count = stats.get('inspection_count', 0)
            match_results_count = stats.get('match_results_count', 0)
            
            print(f"📈 数据规模:")
            print(f"   监督管理系统数据: {supervision_count:,} 条")
            print(f"   安全排查系统数据: {inspection_count:,} 条")
            print(f"   匹配结果数据: {match_results_count:,} 条")
            
            # 处理进度
            if supervision_count > 0:
                progress = (match_results_count / supervision_count) * 100
                print(f"   整体处理进度: {progress:.2f}%")
                
                remaining = supervision_count - match_results_count
                print(f"   剩余待处理: {remaining:,} 条")
            
            # 数据规模评估
            total_data = supervision_count + inspection_count
            if total_data > 2000000:
                scale = "🔴 超大规模"
                recommendation = "建议使用分布式处理"
            elif total_data > 500000:
                scale = "🟡 大规模"
                recommendation = "当前配置适合，可考虑并行优化"
            else:
                scale = "🟢 中等规模"
                recommendation = "当前配置充足"
            
            print(f"\n🎯 数据规模评估:")
            print(f"   规模等级: {scale} ({total_data:,} 条)")
            print(f"   处理建议: {recommendation}")
    
    def generate_matching_quality_report(self):
        """生成匹配质量报告"""
        print("\n🎯 匹配质量报告")
        print("-" * 80)
        
        if 'match_stats' in self.report_data:
            stats = self.report_data['match_stats']
            
            total_results = stats.get('total_results', 0)
            matched_results = stats.get('matched_results', 0)
            unmatched_results = stats.get('unmatched_results', 0)
            match_rate = stats.get('match_rate', 0)
            
            print(f"📊 匹配统计:")
            print(f"   总结果数: {total_results:,} 条")
            print(f"   匹配成功: {matched_results:,} 条")
            print(f"   未匹配: {unmatched_results:,} 条")
            print(f"   匹配率: {match_rate:.2f}%")
            
            # 质量评估
            if match_rate >= 80:
                quality_level = "🟢 优秀"
                quality_desc = "匹配质量很高，系统运行良好"
            elif match_rate >= 60:
                quality_level = "🟡 良好"
                quality_desc = "匹配质量较好，可进一步优化"
            elif match_rate >= 40:
                quality_level = "🟠 一般"
                quality_desc = "匹配质量一般，需要优化算法"
            else:
                quality_level = "🔴 较差"
                quality_desc = "匹配质量较差，需要重新调整参数"
            
            print(f"\n🎯 质量评估:")
            print(f"   质量等级: {quality_level}")
            print(f"   评估说明: {quality_desc}")
            
            # 匹配类型分析
            match_type_stats = stats.get('match_type_stats', [])
            if match_type_stats:
                print(f"\n🔍 匹配类型分布:")
                for stat in match_type_stats:
                    match_type = stat.get('_id', 'unknown')
                    count = stat.get('count', 0)
                    avg_similarity = stat.get('avg_similarity', 0)
                    percentage = (count / total_results * 100) if total_results > 0 else 0
                    print(f"   {match_type}: {count:,} 条 ({percentage:.1f}%) - 平均相似度: {avg_similarity:.2f}")
            
            # 置信度分析
            confidence_stats = stats.get('confidence_stats', [])
            if confidence_stats:
                print(f"\n🎯 置信度分布:")
                for stat in confidence_stats:
                    confidence = stat.get('_id', 'unknown')
                    count = stat.get('count', 0)
                    percentage = (count / total_results * 100) if total_results > 0 else 0
                    print(f"   {confidence}: {count:,} 条 ({percentage:.1f}%)")
    
    def generate_task_progress_report(self):
        """生成任务进度报告"""
        print("\n⚡ 任务执行报告")
        print("-" * 80)
        
        if 'task_progress' in self.report_data:
            progress = self.report_data['task_progress']
            
            task_id = progress.get('task_id', 'N/A')
            status = progress.get('status', 'unknown')
            progress_percent = progress.get('progress_percent', 0)
            processed_records = progress.get('processed_records', 0)
            matched_records = progress.get('matched_records', 0)
            total_records = progress.get('total_records', 0)
            elapsed_time = progress.get('elapsed_time', 0)
            
            print(f"📋 任务信息:")
            print(f"   任务ID: {task_id}")
            print(f"   任务状态: {status}")
            print(f"   执行进度: {progress_percent:.2f}%")
            
            print(f"\n📊 处理统计:")
            print(f"   已处理记录: {processed_records:,} 条")
            print(f"   匹配成功: {matched_records:,} 条")
            print(f"   总记录数: {total_records:,} 条")
            print(f"   执行时长: {elapsed_time:.1f} 秒")
            
            # 性能指标
            if elapsed_time > 0 and processed_records > 0:
                processing_speed = processed_records / elapsed_time
                print(f"   处理速度: {processing_speed:.2f} 记录/秒")
                
                if total_records > processed_records:
                    remaining_records = total_records - processed_records
                    eta_seconds = remaining_records / processing_speed
                    eta_hours = eta_seconds / 3600
                    print(f"   预计剩余时间: {eta_hours:.1f} 小时")
            
            # 任务状态评估
            if status == 'running':
                status_desc = "🟢 任务正在正常运行"
            elif status == 'completed':
                status_desc = "✅ 任务已成功完成"
            elif status == 'error':
                status_desc = "🔴 任务执行出现错误"
            else:
                status_desc = "🟡 任务状态未知"
            
            print(f"\n🎯 状态评估: {status_desc}")
        else:
            print("❌ 无法获取任务进度信息")
    
    def generate_optimization_recommendations(self):
        """生成优化建议"""
        print("\n💡 优化建议")
        print("-" * 80)
        
        recommendations = []
        
        # 基于系统性能的建议
        if 'system_info' in self.report_data:
            info = self.report_data['system_info']
            if info['current_cpu_usage'] > 70:
                recommendations.append("🔧 CPU使用率较高，建议优化算法或增加硬件资源")
            if info['current_memory_usage'] > 80:
                recommendations.append("🧠 内存使用率较高，建议优化内存管理或增加内存")
            if info['cpu_cores'] >= 8:
                recommendations.append("⚡ 系统具备多核优势，建议启用并行处理")
        
        # 基于匹配质量的建议
        if 'match_stats' in self.report_data:
            stats = self.report_data['match_stats']
            match_rate = stats.get('match_rate', 0)
            
            if match_rate < 60:
                recommendations.append("🎯 匹配率偏低，建议调整匹配算法参数")
                recommendations.append("📝 建议增加数据预处理和清洗步骤")
            elif match_rate < 80:
                recommendations.append("🔍 匹配率良好，可通过精细调参进一步提升")
            
            total_results = stats.get('total_results', 0)
            if total_results > 0:
                unmatched_results = stats.get('unmatched_results', 0)
                if unmatched_results > total_results * 0.3:
                    recommendations.append("👥 未匹配记录较多，建议加强人工审核")
        
        # 基于数据规模的建议
        if 'data_stats' in self.report_data:
            stats = self.report_data['data_stats']
            supervision_count = stats.get('supervision_count', 0)
            
            if supervision_count > 1000000:
                recommendations.append("📊 数据量较大，建议实施分批处理策略")
                recommendations.append("🗄️ 建议优化数据库索引以提升查询性能")
        
        # 基于任务进度的建议
        if 'task_progress' in self.report_data:
            progress = self.report_data['task_progress']
            elapsed_time = progress.get('elapsed_time', 0)
            processed_records = progress.get('processed_records', 0)
            
            if elapsed_time > 0 and processed_records > 0:
                speed = processed_records / elapsed_time
                if speed < 1:
                    recommendations.append("🚀 处理速度较慢，建议增加批次大小或优化算法")
        
        # 通用建议
        recommendations.extend([
            "📈 建议定期监控系统性能和匹配质量",
            "🔄 建议建立自动化的性能基准测试",
            "📋 建议完善错误日志和监控机制",
            "🎯 建议根据业务需求调整匹配策略",
            "💾 建议定期备份重要的匹配结果数据"
        ])
        
        print("🎯 短期优化建议 (1-2周内):")
        for i, rec in enumerate(recommendations[:5], 1):
            print(f"   {i}. {rec}")
        
        print("\n🚀 中期优化建议 (1-3个月内):")
        for i, rec in enumerate(recommendations[5:10], 1):
            print(f"   {i}. {rec}")
        
        print("\n📈 长期优化建议 (3个月以上):")
        for i, rec in enumerate(recommendations[10:], 1):
            print(f"   {i}. {rec}")
    
    def generate_next_steps(self):
        """生成下一步行动计划"""
        print("\n🎯 下一步行动计划")
        print("-" * 80)
        
        action_plan = [
            {
                "priority": "🔴 高优先级",
                "actions": [
                    "启动实时监控脚本，持续跟踪系统状态",
                    "检查当前匹配任务进度，确保正常运行",
                    "对未匹配记录进行人工审核"
                ]
            },
            {
                "priority": "🟡 中优先级", 
                "actions": [
                    "根据匹配结果调整算法参数",
                    "优化数据库查询和索引",
                    "实施批次大小动态调整"
                ]
            },
            {
                "priority": "🟢 低优先级",
                "actions": [
                    "建立性能基准测试体系",
                    "完善错误处理和重试机制",
                    "开发更多的监控和分析工具"
                ]
            }
        ]
        
        for plan in action_plan:
            print(f"\n{plan['priority']}:")
            for i, action in enumerate(plan['actions'], 1):
                print(f"   {i}. {action}")
    
    def generate_footer(self):
        """生成报告尾部"""
        print("\n" + "=" * 100)
        print("📊 报告生成完成")
        print(f"🕒 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🔗 相关链接:")
        print(f"   系统访问: {self.base_url}")
        print(f"   结果查看: {self.base_url}/results")
        print(f"   匹配管理: {self.base_url}/matching")
        print("=" * 100)
    
    def generate_full_report(self):
        """生成完整报告"""
        print("🚀 正在生成优化总结报告...")
        
        # 收集数据
        print("📊 收集系统信息...")
        self.collect_system_info()
        
        print("📈 收集数据统计...")
        self.collect_data_statistics()
        
        print("🎯 收集匹配统计...")
        self.collect_match_statistics()
        
        print("⚡ 收集任务进度...")
        self.collect_task_progress()
        
        print("📋 生成报告...")
        time.sleep(1)
        
        # 生成报告各部分
        self.generate_report_header()
        self.generate_optimization_achievements()
        self.generate_system_performance_report()
        self.generate_data_analysis_report()
        self.generate_matching_quality_report()
        self.generate_task_progress_report()
        self.generate_optimization_recommendations()
        self.generate_next_steps()
        self.generate_footer()

def main():
    """主函数"""
    summary = OptimizationSummary()
    summary.generate_full_report()

if __name__ == "__main__":
    main() 