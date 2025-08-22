#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能基准测试
使用10,000条数据进行压力测试，验证优化后的系统性能表现
"""

import os
import sys
import logging
import json
import time
import threading
import multiprocessing
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import psutil

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
from src.matching.similarity_scorer import SimilarityCalculator
from src.matching.address_normalizer import normalize_address_for_matching

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PerformanceBenchmarkTest:
    """性能基准测试器"""
    
    def __init__(self):
        """初始化测试器"""
        self.config_manager = ConfigManager()
        self.db_manager = DatabaseManager(config=self.config_manager.get_database_config())
        self.similarity_calculator = SimilarityCalculator(self.config_manager.get_matching_config())
        
        # 性能统计
        self.performance_stats = {
            'total_comparisons': 0,
            'successful_matches': 0,
            'processing_times': [],
            'memory_usage': [],
            'cpu_usage': [],
            'throughput_per_second': 0,
            'average_response_time': 0,
            'peak_memory_usage': 0,
            'peak_cpu_usage': 0
        }
        
        # 测试配置
        self.test_config = {
            'total_test_records': 10000,
            'batch_size': 100,
            'thread_count': 4,
            'similarity_threshold': 0.6,
            'max_comparisons_per_record': 50
        }
        
    def run_performance_benchmark(self):
        """运行性能基准测试"""
        print("🚀 性能基准测试")
        print("=" * 50)
        
        try:
            # 1. 准备测试数据
            print("📊 准备测试数据...")
            test_data = self._prepare_large_test_dataset()
            
            if not test_data['source_addresses'] or not test_data['target_addresses']:
                print("❌ 测试数据准备失败")
                return
            
            print(f"✅ 测试数据准备完成：{len(test_data['source_addresses'])} 条源地址，{len(test_data['target_addresses'])} 条目标地址")
            
            # 2. 单线程性能测试
            print("\n🔄 单线程性能测试...")
            single_thread_results = self._run_single_thread_test(test_data)
            
            # 3. 多线程性能测试
            print("\n🔄 多线程性能测试...")
            multi_thread_results = self._run_multi_thread_test(test_data)
            
            # 4. 批处理性能测试
            print("\n🔄 批处理性能测试...")
            batch_processing_results = self._run_batch_processing_test(test_data)
            
            # 5. 内存和CPU监控测试
            print("\n🔄 资源使用监控测试...")
            resource_monitoring_results = self._run_resource_monitoring_test(test_data)
            
            # 6. 生成综合性能报告
            performance_report = self._generate_performance_report({
                'single_thread': single_thread_results,
                'multi_thread': multi_thread_results,
                'batch_processing': batch_processing_results,
                'resource_monitoring': resource_monitoring_results,
                'test_config': self.test_config,
                'system_info': self._get_system_info()
            })
            
            # 7. 保存报告
            report_filename = f"performance_benchmark_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(performance_report, f, ensure_ascii=False, indent=2)
            
            # 8. 显示结果摘要
            self._display_performance_summary(performance_report)
            
            print(f"\n📋 详细性能报告已保存到: {report_filename}")
            print("✅ 性能基准测试完成")
            
        except Exception as e:
            logger.error(f"性能基准测试失败: {str(e)}")
            print("❌ 性能基准测试失败")
    
    def _prepare_large_test_dataset(self) -> Dict:
        """准备大规模测试数据集"""
        try:
            # 获取源表数据
            source_collection = self.db_manager.get_collection('hztj_hzxx')
            source_addresses = list(source_collection.find(
                {'起火地点': {'$exists': True, '$ne': ''}},
                {'起火地点': 1}
            ).limit(self.test_config['total_test_records']))
            
            # 获取目标表数据
            target_collection = self.db_manager.get_collection('dwd_yljgxx')
            target_addresses = list(target_collection.find(
                {'ZCDZ': {'$exists': True, '$ne': ''}},
                {'ZCDZ': 1}
            ).limit(self.test_config['total_test_records']))
            
            return {
                'source_addresses': [addr['起火地点'] for addr in source_addresses],
                'target_addresses': [addr['ZCDZ'] for addr in target_addresses]
            }
            
        except Exception as e:
            logger.error(f"准备测试数据失败: {str(e)}")
            return {'source_addresses': [], 'target_addresses': []}
    
    def _run_single_thread_test(self, test_data: Dict) -> Dict:
        """运行单线程性能测试"""
        start_time = time.time()
        successful_matches = 0
        total_comparisons = 0
        processing_times = []
        
        # 限制测试数量以避免过长时间
        test_limit = min(1000, len(test_data['source_addresses']))
        
        for i, source_addr in enumerate(test_data['source_addresses'][:test_limit]):
            if i % 100 == 0:
                print(f"  处理进度: {i}/{test_limit}")
            
            record_start_time = time.time()
            
            # 标准化源地址
            normalized_source = normalize_address_for_matching(source_addr)
            
            best_score = 0.0
            # 限制比较数量
            comparison_limit = min(self.test_config['max_comparisons_per_record'], len(test_data['target_addresses']))
            
            for target_addr in test_data['target_addresses'][:comparison_limit]:
                normalized_target = normalize_address_for_matching(target_addr)
                
                # 计算相似度
                similarity = self.similarity_calculator.calculate_string_similarity(
                    normalized_source, normalized_target
                )
                
                if similarity > best_score:
                    best_score = similarity
                
                total_comparisons += 1
            
            if best_score >= self.test_config['similarity_threshold']:
                successful_matches += 1
            
            record_time = time.time() - record_start_time
            processing_times.append(record_time)
        
        total_time = time.time() - start_time
        
        return {
            'total_time': total_time,
            'successful_matches': successful_matches,
            'total_comparisons': total_comparisons,
            'records_processed': test_limit,
            'throughput_per_second': test_limit / total_time if total_time > 0 else 0,
            'average_response_time': sum(processing_times) / len(processing_times) if processing_times else 0,
            'match_rate': successful_matches / test_limit if test_limit > 0 else 0
        }
    
    def _run_multi_thread_test(self, test_data: Dict) -> Dict:
        """运行多线程性能测试"""
        start_time = time.time()
        successful_matches = 0
        total_comparisons = 0
        
        # 限制测试数量
        test_limit = min(1000, len(test_data['source_addresses']))
        
        def process_batch(batch_data):
            batch_matches = 0
            batch_comparisons = 0
            
            for source_addr in batch_data:
                normalized_source = normalize_address_for_matching(source_addr)
                best_score = 0.0
                
                comparison_limit = min(self.test_config['max_comparisons_per_record'], len(test_data['target_addresses']))
                
                for target_addr in test_data['target_addresses'][:comparison_limit]:
                    normalized_target = normalize_address_for_matching(target_addr)
                    similarity = self.similarity_calculator.calculate_string_similarity(
                        normalized_source, normalized_target
                    )
                    
                    if similarity > best_score:
                        best_score = similarity
                    
                    batch_comparisons += 1
                
                if best_score >= self.test_config['similarity_threshold']:
                    batch_matches += 1
            
            return batch_matches, batch_comparisons
        
        # 分批处理
        batch_size = self.test_config['batch_size']
        batches = [test_data['source_addresses'][i:i+batch_size] for i in range(0, test_limit, batch_size)]
        
        with ThreadPoolExecutor(max_workers=self.test_config['thread_count']) as executor:
            results = list(executor.map(process_batch, batches))
        
        for matches, comparisons in results:
            successful_matches += matches
            total_comparisons += comparisons
        
        total_time = time.time() - start_time
        
        return {
            'total_time': total_time,
            'successful_matches': successful_matches,
            'total_comparisons': total_comparisons,
            'records_processed': test_limit,
            'throughput_per_second': test_limit / total_time if total_time > 0 else 0,
            'match_rate': successful_matches / test_limit if test_limit > 0 else 0,
            'thread_count': self.test_config['thread_count']
        }
    
    def _run_batch_processing_test(self, test_data: Dict) -> Dict:
        """运行批处理性能测试"""
        start_time = time.time()
        
        # 批量预处理地址
        print("  批量标准化源地址...")
        normalized_sources = []
        for addr in test_data['source_addresses'][:1000]:
            normalized_sources.append(normalize_address_for_matching(addr))
        
        print("  批量标准化目标地址...")
        normalized_targets = []
        for addr in test_data['target_addresses'][:1000]:
            normalized_targets.append(normalize_address_for_matching(addr))
        
        preprocessing_time = time.time() - start_time
        
        # 批量相似度计算
        matching_start_time = time.time()
        successful_matches = 0
        total_comparisons = 0
        
        for i, source in enumerate(normalized_sources[:500]):  # 限制数量
            if i % 100 == 0:
                print(f"  批处理进度: {i}/500")
            
            best_score = 0.0
            for target in normalized_targets[:50]:  # 限制比较数量
                similarity = self.similarity_calculator.calculate_string_similarity(source, target)
                if similarity > best_score:
                    best_score = similarity
                total_comparisons += 1
            
            if best_score >= self.test_config['similarity_threshold']:
                successful_matches += 1
        
        matching_time = time.time() - matching_start_time
        total_time = time.time() - start_time
        
        return {
            'total_time': total_time,
            'preprocessing_time': preprocessing_time,
            'matching_time': matching_time,
            'successful_matches': successful_matches,
            'total_comparisons': total_comparisons,
            'records_processed': 500,
            'throughput_per_second': 500 / total_time if total_time > 0 else 0,
            'match_rate': successful_matches / 500 if successful_matches > 0 else 0
        }
    
    def _run_resource_monitoring_test(self, test_data: Dict) -> Dict:
        """运行资源使用监控测试"""
        memory_usage = []
        cpu_usage = []
        
        def monitor_resources():
            while getattr(monitor_resources, 'running', True):
                memory_usage.append(psutil.virtual_memory().percent)
                cpu_usage.append(psutil.cpu_percent())
                time.sleep(0.5)
        
        # 启动监控线程
        monitor_resources.running = True
        monitor_thread = threading.Thread(target=monitor_resources)
        monitor_thread.start()
        
        try:
            # 执行一个中等规模的测试
            start_time = time.time()
            successful_matches = 0
            
            for i, source_addr in enumerate(test_data['source_addresses'][:500]):
                if i % 100 == 0:
                    print(f"  资源监控测试进度: {i}/500")
                
                normalized_source = normalize_address_for_matching(source_addr)
                best_score = 0.0
                
                for target_addr in test_data['target_addresses'][:30]:
                    normalized_target = normalize_address_for_matching(target_addr)
                    similarity = self.similarity_calculator.calculate_string_similarity(
                        normalized_source, normalized_target
                    )
                    
                    if similarity > best_score:
                        best_score = similarity
                
                if best_score >= self.test_config['similarity_threshold']:
                    successful_matches += 1
            
            total_time = time.time() - start_time
            
        finally:
            # 停止监控
            monitor_resources.running = False
            monitor_thread.join()
        
        return {
            'total_time': total_time,
            'successful_matches': successful_matches,
            'records_processed': 500,
            'peak_memory_usage': max(memory_usage) if memory_usage else 0,
            'average_memory_usage': sum(memory_usage) / len(memory_usage) if memory_usage else 0,
            'peak_cpu_usage': max(cpu_usage) if cpu_usage else 0,
            'average_cpu_usage': sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0,
            'memory_samples': len(memory_usage),
            'cpu_samples': len(cpu_usage)
        }
    
    def _get_system_info(self) -> Dict:
        """获取系统信息"""
        return {
            'cpu_count': multiprocessing.cpu_count(),
            'memory_total_gb': round(psutil.virtual_memory().total / (1024**3), 2),
            'python_version': sys.version,
            'platform': sys.platform
        }
    
    def _generate_performance_report(self, results: Dict) -> Dict:
        """生成性能报告"""
        return {
            'test_timestamp': datetime.now().isoformat(),
            'test_configuration': results['test_config'],
            'system_information': results['system_info'],
            'performance_results': {
                'single_thread_performance': results['single_thread'],
                'multi_thread_performance': results['multi_thread'],
                'batch_processing_performance': results['batch_processing'],
                'resource_monitoring': results['resource_monitoring']
            },
            'performance_comparison': {
                'single_vs_multi_thread_speedup': (
                    results['multi_thread']['throughput_per_second'] / 
                    results['single_thread']['throughput_per_second']
                ) if results['single_thread']['throughput_per_second'] > 0 else 0,
                'batch_processing_efficiency': (
                    results['batch_processing']['throughput_per_second'] / 
                    results['single_thread']['throughput_per_second']
                ) if results['single_thread']['throughput_per_second'] > 0 else 0
            },
            'recommendations': self._generate_performance_recommendations(results)
        }
    
    def _generate_performance_recommendations(self, results: Dict) -> List[str]:
        """生成性能优化建议"""
        recommendations = []
        
        # 基于测试结果生成建议
        single_thread = results['single_thread']
        multi_thread = results['multi_thread']
        batch_processing = results['batch_processing']
        resource_monitoring = results['resource_monitoring']
        
        if multi_thread['throughput_per_second'] > single_thread['throughput_per_second'] * 1.5:
            recommendations.append("多线程处理显著提升性能，建议在生产环境中使用多线程处理")
        
        if batch_processing['throughput_per_second'] > single_thread['throughput_per_second'] * 1.2:
            recommendations.append("批处理模式提升性能，建议对地址进行批量预处理")
        
        if resource_monitoring['peak_memory_usage'] > 80:
            recommendations.append("内存使用率较高，建议优化内存管理或增加系统内存")
        
        if resource_monitoring['peak_cpu_usage'] > 90:
            recommendations.append("CPU使用率较高，建议优化算法或使用更强的CPU")
        
        if single_thread['match_rate'] < 0.3:
            recommendations.append("匹配率较低，建议调整相似度阈值或优化匹配算法")
        
        return recommendations
    
    def _display_performance_summary(self, report: Dict):
        """显示性能测试摘要"""
        print("\n🎯 性能测试结果摘要:")
        print("=" * 50)
        
        results = report['performance_results']
        
        print(f"📊 单线程性能:")
        print(f"  处理速度: {results['single_thread_performance']['throughput_per_second']:.2f} 条/秒")
        print(f"  匹配率: {results['single_thread_performance']['match_rate']:.1%}")
        print(f"  平均响应时间: {results['single_thread_performance']['average_response_time']:.3f} 秒")
        
        print(f"\n📊 多线程性能:")
        print(f"  处理速度: {results['multi_thread_performance']['throughput_per_second']:.2f} 条/秒")
        print(f"  匹配率: {results['multi_thread_performance']['match_rate']:.1%}")
        print(f"  线程数: {results['multi_thread_performance']['thread_count']}")
        
        print(f"\n📊 批处理性能:")
        print(f"  处理速度: {results['batch_processing_performance']['throughput_per_second']:.2f} 条/秒")
        print(f"  匹配率: {results['batch_processing_performance']['match_rate']:.1%}")
        print(f"  预处理时间: {results['batch_processing_performance']['preprocessing_time']:.2f} 秒")
        
        print(f"\n📊 资源使用:")
        print(f"  峰值内存使用: {results['resource_monitoring']['peak_memory_usage']:.1f}%")
        print(f"  峰值CPU使用: {results['resource_monitoring']['peak_cpu_usage']:.1f}%")
        print(f"  平均内存使用: {results['resource_monitoring']['average_memory_usage']:.1f}%")
        print(f"  平均CPU使用: {results['resource_monitoring']['average_cpu_usage']:.1f}%")
        
        print(f"\n🚀 性能对比:")
        comparison = report['performance_comparison']
        print(f"  多线程加速比: {comparison['single_vs_multi_thread_speedup']:.2f}x")
        print(f"  批处理效率比: {comparison['batch_processing_efficiency']:.2f}x")
        
        print(f"\n💡 优化建议:")
        for i, recommendation in enumerate(report['recommendations'], 1):
            print(f"  {i}. {recommendation}")

def main():
    """主函数"""
    try:
        benchmark = PerformanceBenchmarkTest()
        benchmark.run_performance_benchmark()
    except Exception as e:
        logger.error(f"性能基准测试执行失败: {str(e)}")
        print("❌ 性能基准测试执行失败")

if __name__ == "__main__":
    main()

