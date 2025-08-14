#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能优化测试脚本
基于昨日系统稳定性修复，进行深度性能优化测试
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PerformanceOptimizer:
    """性能优化测试器"""
    
    def __init__(self):
        """初始化性能优化器"""
        self.system_info = self._get_system_info()
        self.test_results = {}
        self.optimal_config = {}
        
    def _get_system_info(self) -> Dict:
        """获取系统信息"""
        try:
            memory = psutil.virtual_memory()
            cpu_count = psutil.cpu_count()
            cpu_count_logical = psutil.cpu_count(logical=True)
            
            return {
                'cpu_cores_physical': cpu_count,
                'cpu_cores_logical': cpu_count_logical,
                'total_memory_gb': round(memory.total / (1024**3), 2),
                'available_memory_gb': round(memory.available / (1024**3), 2),
                'memory_usage_percent': memory.percent,
                'recommended_workers': min(cpu_count_logical * 2, 64)  # 建议的工作线程数
            }
        except Exception as e:
            logger.error(f"获取系统信息失败: {e}")
            return {}
    
    def test_batch_size_performance(self) -> Dict:
        """测试不同批处理大小的性能"""
        logger.info("测试批处理大小性能...")
        
        batch_sizes = [1000, 5000, 10000, 25000, 50000, 100000]
        results = {}
        
        for batch_size in batch_sizes:
            logger.info(f"测试批处理大小: {batch_size}")
            
            try:
                # 模拟批处理操作
                start_time = time.time()
                start_memory = psutil.Process().memory_info().rss
                
                # 创建测试数据
                test_data = []
                for i in range(batch_size):
                    test_data.append({
                        'id': i,
                        'unit_name': f'测试单位{i}',
                        'address': f'测试地址{i}号',
                        'contact': f'138{i:08d}',
                        'business_type': f'行业{i % 100}'
                    })
                
                # 模拟批处理操作
                processed_count = 0
                for batch_start in range(0, len(test_data), min(1000, batch_size)):
                    batch_end = min(batch_start + 1000, len(test_data))
                    batch = test_data[batch_start:batch_end]
                    
                    # 模拟处理时间
                    for record in batch:
                        processed_count += 1
                        # 模拟一些计算
                        hash(str(record))
                
                end_time = time.time()
                end_memory = psutil.Process().memory_info().rss
                
                processing_time = end_time - start_time
                memory_usage = (end_memory - start_memory) / (1024 * 1024)  # MB
                speed = processed_count / processing_time if processing_time > 0 else 0
                
                results[batch_size] = {
                    'processing_time': processing_time,
                    'memory_usage_mb': memory_usage,
                    'speed_per_second': speed,
                    'records_processed': processed_count
                }
                
                logger.info(f"批处理大小 {batch_size}: {speed:.2f} 条/秒, 内存: {memory_usage:.2f} MB")
                
                # 清理内存
                del test_data
                
            except Exception as e:
                logger.error(f"测试批处理大小 {batch_size} 失败: {e}")
                results[batch_size] = {'error': str(e)}
        
        # 找到最优批处理大小
        best_batch_size = max(
            [k for k, v in results.items() if 'error' not in v],
            key=lambda k: results[k].get('speed_per_second', 0),
            default=None
        )
        
        return {
            'results': results,
            'optimal_batch_size': best_batch_size,
            'optimal_performance': results.get(best_batch_size, {}) if best_batch_size else {}
        }
    
    def test_thread_pool_performance(self) -> Dict:
        """测试不同线程池大小的性能"""
        logger.info("测试线程池性能...")
        
        max_workers_options = [4, 8, 16, 32, 64, 128]
        # 限制最大线程数不超过系统建议值
        max_recommended = self.system_info.get('recommended_workers', 32)
        max_workers_options = [w for w in max_workers_options if w <= max_recommended * 2]
        
        results = {}
        test_data_size = 10000
        
        # 准备测试任务
        def cpu_intensive_task(data_chunk):
            """CPU密集型任务模拟"""
            result = 0
            for item in data_chunk:
                # 模拟复杂计算
                for i in range(100):
                    result += hash(str(item)) % 1000
            return result
        
        for max_workers in max_workers_options:
            logger.info(f"测试线程数: {max_workers}")
            
            try:
                # 准备测试数据
                test_data = list(range(test_data_size))
                chunk_size = max(1, test_data_size // max_workers)
                data_chunks = [test_data[i:i + chunk_size] for i in range(0, test_data_size, chunk_size)]
                
                start_time = time.time()
                start_memory = psutil.Process().memory_info().rss
                
                # 使用线程池执行任务
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(cpu_intensive_task, chunk) for chunk in data_chunks]
                    
                    completed_tasks = 0
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            completed_tasks += 1
                        except Exception as e:
                            logger.error(f"任务执行失败: {e}")
                
                end_time = time.time()
                end_memory = psutil.Process().memory_info().rss
                
                processing_time = end_time - start_time
                memory_usage = (end_memory - start_memory) / (1024 * 1024)  # MB
                speed = test_data_size / processing_time if processing_time > 0 else 0
                
                results[max_workers] = {
                    'processing_time': processing_time,
                    'memory_usage_mb': memory_usage,
                    'speed_per_second': speed,
                    'completed_tasks': completed_tasks,
                    'total_chunks': len(data_chunks)
                }
                
                logger.info(f"线程数 {max_workers}: {speed:.2f} 条/秒, 内存: {memory_usage:.2f} MB")
                
            except Exception as e:
                logger.error(f"测试线程数 {max_workers} 失败: {e}")
                results[max_workers] = {'error': str(e)}
        
        # 找到最优线程数
        best_workers = max(
            [k for k, v in results.items() if 'error' not in v],
            key=lambda k: results[k].get('speed_per_second', 0),
            default=None
        )
        
        return {
            'results': results,
            'optimal_workers': best_workers,
            'optimal_performance': results.get(best_workers, {}) if best_workers else {}
        }
    
    def test_memory_optimization(self) -> Dict:
        """测试内存优化策略"""
        logger.info("测试内存优化策略...")
        
        results = {}
        
        # 测试1: 对象池vs重复创建
        logger.info("测试对象池效果...")
        
        # 不使用对象池
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        objects_created = []
        for i in range(10000):
            obj = {
                'id': i,
                'data': f'数据{i}' * 10,
                'timestamp': datetime.now().isoformat()
            }
            objects_created.append(obj)
        
        no_pool_time = time.time() - start_time
        no_pool_memory = psutil.Process().memory_info().rss - start_memory
        
        del objects_created
        
        # 使用对象池模拟
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        # 预分配对象池
        object_pool = []
        for i in range(1000):  # 预分配1000个对象
            object_pool.append({
                'id': 0,
                'data': '',
                'timestamp': ''
            })
        
        pool_objects = []
        for i in range(10000):
            if i < len(object_pool):
                # 重用对象
                obj = object_pool[i % len(object_pool)]
                obj['id'] = i
                obj['data'] = f'数据{i}' * 10
                obj['timestamp'] = datetime.now().isoformat()
            else:
                # 创建新对象
                obj = {
                    'id': i,
                    'data': f'数据{i}' * 10,
                    'timestamp': datetime.now().isoformat()
                }
            pool_objects.append(obj.copy())
        
        pool_time = time.time() - start_time
        pool_memory = psutil.Process().memory_info().rss - start_memory
        
        del pool_objects, object_pool
        
        results['object_pooling'] = {
            'no_pool': {
                'time': no_pool_time,
                'memory_mb': no_pool_memory / (1024 * 1024)
            },
            'with_pool': {
                'time': pool_time,
                'memory_mb': pool_memory / (1024 * 1024)
            },
            'improvement': {
                'time_reduction_percent': ((no_pool_time - pool_time) / no_pool_time * 100) if no_pool_time > 0 else 0,
                'memory_reduction_percent': ((no_pool_memory - pool_memory) / no_pool_memory * 100) if no_pool_memory > 0 else 0
            }
        }
        
        logger.info(f"对象池优化: 时间减少 {results['object_pooling']['improvement']['time_reduction_percent']:.1f}%")
        logger.info(f"对象池优化: 内存减少 {results['object_pooling']['improvement']['memory_reduction_percent']:.1f}%")
        
        # 测试2: 流式处理vs批量加载
        logger.info("测试流式处理效果...")
        
        # 批量加载
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        # 一次性加载所有数据
        all_data = []
        for i in range(50000):
            all_data.append(f'大数据项{i}' * 20)
        
        # 处理所有数据
        processed = [len(item) for item in all_data]
        
        batch_time = time.time() - start_time
        batch_memory = psutil.Process().memory_info().rss - start_memory
        
        del all_data, processed
        
        # 流式处理
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        def data_generator():
            for i in range(50000):
                yield f'大数据项{i}' * 20
        
        processed_count = 0
        for item in data_generator():
            length = len(item)
            processed_count += 1
        
        stream_time = time.time() - start_time
        stream_memory = psutil.Process().memory_info().rss - start_memory
        
        results['streaming_vs_batch'] = {
            'batch_processing': {
                'time': batch_time,
                'memory_mb': batch_memory / (1024 * 1024)
            },
            'stream_processing': {
                'time': stream_time,
                'memory_mb': stream_memory / (1024 * 1024)
            },
            'improvement': {
                'time_change_percent': ((stream_time - batch_time) / batch_time * 100) if batch_time > 0 else 0,
                'memory_reduction_percent': ((batch_memory - stream_memory) / batch_memory * 100) if batch_memory > 0 else 0
            }
        }
        
        logger.info(f"流式处理: 内存减少 {results['streaming_vs_batch']['improvement']['memory_reduction_percent']:.1f}%")
        
        return results
    
    def test_database_connection_optimization(self) -> Dict:
        """测试数据库连接优化"""
        logger.info("测试数据库连接优化...")
        
        # 模拟不同连接池配置的性能
        connection_configs = [
            {'pool_size': 50, 'timeout': 30},
            {'pool_size': 100, 'timeout': 30},
            {'pool_size': 200, 'timeout': 30},
            {'pool_size': 500, 'timeout': 30},
            {'pool_size': 1000, 'timeout': 30}
        ]
        
        results = {}
        
        for config in connection_configs:
            pool_size = config['pool_size']
            logger.info(f"测试连接池大小: {pool_size}")
            
            try:
                # 模拟数据库连接操作
                start_time = time.time()
                
                # 模拟并发数据库操作
                def simulate_db_operation(operation_id):
                    # 模拟数据库查询时间
                    time.sleep(0.001 + (operation_id % 10) * 0.0001)
                    return f"result_{operation_id}"
                
                # 使用线程池模拟连接池
                max_workers = min(pool_size, 100)  # 限制测试线程数
                operations_count = 1000
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(simulate_db_operation, i) for i in range(operations_count)]
                    
                    completed = 0
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            completed += 1
                        except Exception as e:
                            logger.error(f"模拟数据库操作失败: {e}")
                
                total_time = time.time() - start_time
                operations_per_second = completed / total_time if total_time > 0 else 0
                
                results[pool_size] = {
                    'total_time': total_time,
                    'completed_operations': completed,
                    'operations_per_second': operations_per_second,
                    'max_workers': max_workers
                }
                
                logger.info(f"连接池 {pool_size}: {operations_per_second:.2f} 操作/秒")
                
            except Exception as e:
                logger.error(f"测试连接池 {pool_size} 失败: {e}")
                results[pool_size] = {'error': str(e)}
        
        # 找到最优连接池大小
        best_pool_size = max(
            [k for k, v in results.items() if 'error' not in v],
            key=lambda k: results[k].get('operations_per_second', 0),
            default=None
        )
        
        return {
            'results': results,
            'optimal_pool_size': best_pool_size,
            'optimal_performance': results.get(best_pool_size, {}) if best_pool_size else {}
        }
    
    def generate_optimization_recommendations(self) -> Dict:
        """生成优化建议"""
        recommendations = {
            'batch_processing': {},
            'threading': {},
            'memory': {},
            'database': {},
            'overall': []
        }
        
        # 批处理建议
        if 'batch_size' in self.test_results:
            batch_result = self.test_results['batch_size']
            optimal_batch = batch_result.get('optimal_batch_size')
            if optimal_batch:
                recommendations['batch_processing'] = {
                    'recommended_batch_size': optimal_batch,
                    'expected_speed': batch_result['optimal_performance'].get('speed_per_second', 0),
                    'reason': f'在测试中表现最佳，处理速度达到 {batch_result["optimal_performance"].get("speed_per_second", 0):.2f} 条/秒'
                }
        
        # 线程池建议
        if 'thread_pool' in self.test_results:
            thread_result = self.test_results['thread_pool']
            optimal_workers = thread_result.get('optimal_workers')
            if optimal_workers:
                recommendations['threading'] = {
                    'recommended_workers': optimal_workers,
                    'expected_speed': thread_result['optimal_performance'].get('speed_per_second', 0),
                    'reason': f'最优线程数，相比系统默认值可提升性能'
                }
        
        # 内存优化建议
        if 'memory_optimization' in self.test_results:
            memory_result = self.test_results['memory_optimization']
            
            object_pool_improvement = memory_result.get('object_pooling', {}).get('improvement', {})
            if object_pool_improvement.get('memory_reduction_percent', 0) > 10:
                recommendations['memory']['use_object_pooling'] = {
                    'memory_reduction': object_pool_improvement.get('memory_reduction_percent', 0),
                    'reason': '对象池可显著减少内存使用和GC压力'
                }
            
            stream_improvement = memory_result.get('streaming_vs_batch', {}).get('improvement', {})
            if stream_improvement.get('memory_reduction_percent', 0) > 50:
                recommendations['memory']['use_streaming'] = {
                    'memory_reduction': stream_improvement.get('memory_reduction_percent', 0),
                    'reason': '流式处理可大幅减少内存峰值使用'
                }
        
        # 数据库连接建议
        if 'database_connections' in self.test_results:
            db_result = self.test_results['database_connections']
            optimal_pool = db_result.get('optimal_pool_size')
            if optimal_pool:
                recommendations['database'] = {
                    'recommended_pool_size': optimal_pool,
                    'expected_ops_per_second': db_result['optimal_performance'].get('operations_per_second', 0),
                    'reason': '在测试中提供最佳数据库操作性能'
                }
        
        # 综合建议
        recommendations['overall'] = [
            '基于测试结果，建议采用以下优化配置以达到最佳性能',
            '监控系统资源使用情况，根据实际负载调整参数',
            '定期进行性能测试，确保配置持续优化',
            '考虑实施自动化性能调优机制'
        ]
        
        return recommendations
    
    def run_comprehensive_test(self) -> Dict:
        """运行综合性能测试"""
        logger.info("开始综合性能优化测试")
        
        # 显示系统信息
        logger.info(f"系统信息:")
        logger.info(f"  CPU核心数: {self.system_info.get('cpu_cores_physical', 'unknown')}")
        logger.info(f"  逻辑CPU数: {self.system_info.get('cpu_cores_logical', 'unknown')}")
        logger.info(f"  总内存: {self.system_info.get('total_memory_gb', 'unknown')} GB")
        logger.info(f"  可用内存: {self.system_info.get('available_memory_gb', 'unknown')} GB")
        
        # 运行各项测试
        try:
            # 1. 批处理大小测试
            self.test_results['batch_size'] = self.test_batch_size_performance()
            
            # 2. 线程池测试
            self.test_results['thread_pool'] = self.test_thread_pool_performance()
            
            # 3. 内存优化测试
            self.test_results['memory_optimization'] = self.test_memory_optimization()
            
            # 4. 数据库连接测试
            self.test_results['database_connections'] = self.test_database_connection_optimization()
            
            # 5. 生成优化建议
            recommendations = self.generate_optimization_recommendations()
            
            # 生成完整报告
            report = {
                'test_info': {
                    'test_date': datetime.now().isoformat(),
                    'test_duration_minutes': 'calculated_later',
                    'system_info': self.system_info
                },
                'test_results': self.test_results,
                'optimization_recommendations': recommendations,
                'success': True
            }
            
            return report
            
        except Exception as e:
            logger.error(f"综合测试失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'partial_results': self.test_results
            }
    
    def save_report(self, report: Dict):
        """保存测试报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存详细报告
        detailed_file = f'logs/performance_optimization_report_{timestamp}.json'
        os.makedirs('logs', exist_ok=True)
        
        with open(detailed_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"详细报告已保存到: {detailed_file}")
        
        # 保存简化摘要
        summary_file = f'logs/performance_optimization_summary_{timestamp}.txt'
        with open(summary_file, 'w', encoding='utf-8') as f:
            self._write_summary(f, report)
        
        logger.info(f"摘要报告已保存到: {summary_file}")
    
    def _write_summary(self, f, report: Dict):
        """写入摘要报告"""
        f.write("=" * 60 + "\n")
        f.write("性能优化测试报告摘要\n")
        f.write("=" * 60 + "\n\n")
        
        # 系统信息
        system_info = report.get('test_info', {}).get('system_info', {})
        f.write("系统配置:\n")
        f.write(f"  CPU核心数: {system_info.get('cpu_cores_physical', 'unknown')}\n")
        f.write(f"  逻辑CPU数: {system_info.get('cpu_cores_logical', 'unknown')}\n")
        f.write(f"  总内存: {system_info.get('total_memory_gb', 'unknown')} GB\n")
        f.write(f"  可用内存: {system_info.get('available_memory_gb', 'unknown')} GB\n\n")
        
        # 优化建议
        recommendations = report.get('optimization_recommendations', {})
        
        f.write("优化建议:\n")
        
        # 批处理建议
        batch_rec = recommendations.get('batch_processing', {})
        if batch_rec:
            f.write(f"  批处理大小: {batch_rec.get('recommended_batch_size', 'N/A')}\n")
            f.write(f"  预期速度: {batch_rec.get('expected_speed', 0):.2f} 条/秒\n")
        
        # 线程池建议
        thread_rec = recommendations.get('threading', {})
        if thread_rec:
            f.write(f"  推荐线程数: {thread_rec.get('recommended_workers', 'N/A')}\n")
            f.write(f"  预期性能: {thread_rec.get('expected_speed', 0):.2f} 条/秒\n")
        
        # 数据库建议
        db_rec = recommendations.get('database', {})
        if db_rec:
            f.write(f"  连接池大小: {db_rec.get('recommended_pool_size', 'N/A')}\n")
            f.write(f"  预期操作数: {db_rec.get('expected_ops_per_second', 0):.2f} 操作/秒\n")
        
        f.write("\n")
        
        # 综合建议
        overall = recommendations.get('overall', [])
        f.write("综合建议:\n")
        for i, suggestion in enumerate(overall, 1):
            f.write(f"  {i}. {suggestion}\n")


def main():
    """主函数"""
    logger.info("开始性能优化测试")
    
    # 创建性能优化器
    optimizer = PerformanceOptimizer()
    
    # 运行综合测试
    start_time = time.time()
    report = optimizer.run_comprehensive_test()
    end_time = time.time()
    
    # 更新测试时长
    if report.get('success'):
        test_duration = (end_time - start_time) / 60  # 转换为分钟
        report['test_info']['test_duration_minutes'] = round(test_duration, 2)
    
    # 保存报告
    optimizer.save_report(report)
    
    # 打印摘要结果
    if report.get('success'):
        print("\n" + "=" * 60)
        print("性能优化测试完成！")
        print("=" * 60)
        
        recommendations = report.get('optimization_recommendations', {})
        
        # 批处理建议
        batch_rec = recommendations.get('batch_processing', {})
        if batch_rec:
            print(f"推荐批处理大小: {batch_rec.get('recommended_batch_size', 'N/A')}")
            print(f"预期处理速度: {batch_rec.get('expected_speed', 0):.2f} 条/秒")
        
        # 线程池建议
        thread_rec = recommendations.get('threading', {})
        if thread_rec:
            print(f"推荐工作线程数: {thread_rec.get('recommended_workers', 'N/A')}")
        
        # 数据库建议
        db_rec = recommendations.get('database', {})
        if db_rec:
            print(f"推荐连接池大小: {db_rec.get('recommended_pool_size', 'N/A')}")
        
        print(f"\n测试时长: {report['test_info']['test_duration_minutes']} 分钟")
        print("详细报告已保存到 logs/ 目录")
        
    else:
        print("性能优化测试失败，请查看日志获取详细信息")


if __name__ == "__main__":
    main()
