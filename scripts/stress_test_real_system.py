#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实际系统压力测试脚本
使用真实的用户数据匹配系统进行大规模压力测试
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.database.connection import DatabaseManager
from src.matching.user_data_matcher import UserDataMatcher
from src.utils.performance_optimizer import get_performance_monitor, get_memory_processor

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/stress_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RealSystemStressTest:
    """真实系统压力测试器"""
    
    def __init__(self):
        """初始化压力测试器"""
        self.db_manager = None
        self.performance_monitor = get_performance_monitor()
        self.memory_processor = get_memory_processor()
        self.test_results = {}
        self.running = False
        
        # 测试配置
        self.test_configs = {
            'small_scale': {
                'name': '小规模测试',
                'record_count': 1000,
                'concurrent_tasks': 1,
                'description': '基础功能验证'
            },
            'medium_scale': {
                'name': '中等规模测试',
                'record_count': 10000,
                'concurrent_tasks': 2,
                'description': '常规负载测试'
            },
            'large_scale': {
                'name': '大规模测试',
                'record_count': 100000,
                'concurrent_tasks': 3,
                'description': '高负载压力测试'
            },
            'extreme_scale': {
                'name': '极限测试',
                'record_count': 1000000,
                'concurrent_tasks': 5,
                'description': '极限负载测试'
            }
        }
        
    def setup_database(self) -> bool:
        """设置数据库连接"""
        try:
            from src.utils.config import ConfigManager
            config_manager = ConfigManager()
            
            self.db_manager = DatabaseManager()
            logger.info("数据库连接建立成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def generate_test_data(self, count: int) -> List[Dict]:
        """生成测试数据"""
        logger.info(f"生成 {count:,} 条测试数据...")
        
        test_data = []
        for i in range(count):
            # 生成多样化的测试数据
            unit_types = ['有限公司', '股份公司', '个体工商户', '合作社', '事业单位']
            industries = ['制造业', '服务业', '建筑业', '零售业', '科技业', '教育业', '医疗业']
            cities = ['北京市', '上海市', '深圳市', '广州市', '杭州市', '成都市', '武汉市']
            
            record = {
                'unit_name': f'测试{unit_types[i % len(unit_types)]}{i:06d}',
                'address': f'{cities[i % len(cities)]}测试区测试街道{i % 1000}号',
                'contact': f'138{i % 10000:04d}{i % 10000:04d}',
                'business_type': industries[i % len(industries)],
                'registration_code': f'{i:018d}',
                'legal_person': f'测试法人{i % 100}',
                'registration_date': (datetime.now() - timedelta(days=i % 3650)).strftime('%Y-%m-%d'),
                'business_scope': f'{industries[i % len(industries)]}相关业务',
                'registered_capital': str((i % 1000 + 1) * 10000),
                'company_status': '存续' if i % 10 != 0 else '注销'
            }
            test_data.append(record)
            
            # 每生成10000条记录记录一次进度
            if (i + 1) % 10000 == 0:
                logger.info(f"已生成 {i + 1:,} 条测试数据")
        
        logger.info(f"测试数据生成完成: {len(test_data):,} 条")
        return test_data
    
    def prepare_field_mapping(self) -> Dict:
        """准备字段映射配置"""
        return {
            'source_table': 'test_data',
            'target_table': 'xfaqpc_jzdwxx',
            'field_mappings': [
                {
                    'source_field': 'unit_name',
                    'target_field': 'dwmc',
                    'is_primary': True,
                    'similarity_threshold': 0.8
                },
                {
                    'source_field': 'address',
                    'target_field': 'dz',
                    'is_primary': False,
                    'similarity_threshold': 0.7
                },
                {
                    'source_field': 'contact',
                    'target_field': 'lxdh',
                    'is_primary': False,
                    'similarity_threshold': 0.9
                }
            ],
            'matching_algorithms': ['exact', 'fuzzy', 'enhanced_fuzzy'],
            'optimization_enabled': True
        }
    
    def upload_test_data(self, test_data: List[Dict], collection_name: str) -> bool:
        """上传测试数据到数据库"""
        try:
            logger.info(f"上传测试数据到集合 {collection_name}...")
            
            collection = self.db_manager.get_collection(collection_name)
            if not collection:
                logger.error(f"无法获取集合: {collection_name}")
                return False
            
            # 清空现有测试数据
            delete_result = collection.delete_many({'unit_name': {'$regex': '^测试'}})
            logger.info(f"清理旧测试数据: {delete_result.deleted_count} 条")
            
            # 批量插入新测试数据
            batch_size = 1000
            inserted_count = 0
            
            for i in range(0, len(test_data), batch_size):
                batch = test_data[i:i + batch_size]
                try:
                    result = collection.insert_many(batch)
                    inserted_count += len(result.inserted_ids)
                    
                    if inserted_count % 10000 == 0:
                        logger.info(f"已上传 {inserted_count:,} 条数据")
                        
                except Exception as e:
                    logger.error(f"批量插入失败: {e}")
                    return False
            
            logger.info(f"测试数据上传完成: {inserted_count:,} 条")
            return True
            
        except Exception as e:
            logger.error(f"上传测试数据失败: {e}")
            return False
    
    def run_single_matching_task(self, test_config: Dict, task_id: str) -> Dict:
        """运行单个匹配任务"""
        logger.info(f"开始匹配任务: {task_id} - {test_config['name']}")
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        try:
            # 创建用户数据匹配器
            matcher_config = {
                'batch_processing': {
                    'batch_size': 10000,  # 使用优化后的批次大小
                    'max_workers': 8,     # 使用优化后的线程数
                    'timeout': 3600
                },
                'database_optimization': {
                    'connection_pool_size': 100  # 使用优化后的连接池大小
                }
            }
            
            matcher = UserDataMatcher(self.db_manager, matcher_config)
            
            # 准备字段映射
            field_mapping = self.prepare_field_mapping()
            
            # 执行匹配
            result = matcher.execute_matching(
                source_collection='test_user_data',
                target_collection='xfaqpc_jzdwxx',
                field_mapping=field_mapping,
                task_id=task_id
            )
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss
            
            # 计算性能指标
            total_time = end_time - start_time
            memory_usage = (end_memory - start_memory) / (1024 * 1024)  # MB
            
            if result.get('success'):
                processed_count = result.get('processed_count', 0)
                matched_count = result.get('matched_count', 0)
                processing_speed = processed_count / total_time if total_time > 0 else 0
                
                task_result = {
                    'task_id': task_id,
                    'config_name': test_config['name'],
                    'success': True,
                    'metrics': {
                        'total_time_seconds': total_time,
                        'processed_count': processed_count,
                        'matched_count': matched_count,
                        'processing_speed_per_second': processing_speed,
                        'memory_usage_mb': memory_usage,
                        'match_rate': matched_count / processed_count if processed_count > 0 else 0
                    },
                    'timestamp': datetime.now().isoformat()
                }
                
                # 记录性能指标
                self.performance_monitor.record_metric('processing_speed', processing_speed)
                self.performance_monitor.record_metric('memory_usage_mb', memory_usage)
                
                logger.info(f"任务完成: {task_id}")
                logger.info(f"处理速度: {processing_speed:.2f} 条/秒")
                logger.info(f"匹配率: {task_result['metrics']['match_rate']:.2%}")
                
                return task_result
            else:
                return {
                    'task_id': task_id,
                    'config_name': test_config['name'],
                    'success': False,
                    'error': result.get('error', 'Unknown error'),
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"匹配任务失败 {task_id}: {e}")
            return {
                'task_id': task_id,
                'config_name': test_config['name'],
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_concurrent_stress_test(self, test_config: Dict) -> Dict:
        """运行并发压力测试"""
        config_name = test_config['name']
        record_count = test_config['record_count']
        concurrent_tasks = test_config['concurrent_tasks']
        
        logger.info(f"开始并发压力测试: {config_name}")
        logger.info(f"数据规模: {record_count:,} 条")
        logger.info(f"并发任务数: {concurrent_tasks}")
        
        # 生成测试数据
        test_data = self.generate_test_data(record_count)
        
        # 上传测试数据
        if not self.upload_test_data(test_data, 'test_user_data'):
            return {
                'config_name': config_name,
                'success': False,
                'error': '测试数据上传失败'
            }
        
        # 运行并发匹配任务
        test_start_time = time.time()
        task_results = []
        
        with ThreadPoolExecutor(max_workers=concurrent_tasks) as executor:
            # 提交所有任务
            futures = []
            for i in range(concurrent_tasks):
                task_id = f"stress_test_{config_name}_{i}_{int(time.time())}"
                future = executor.submit(self.run_single_matching_task, test_config, task_id)
                futures.append(future)
            
            # 收集结果
            for future in as_completed(futures):
                try:
                    result = future.result()
                    task_results.append(result)
                except Exception as e:
                    logger.error(f"任务执行异常: {e}")
                    task_results.append({
                        'success': False,
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    })
        
        test_total_time = time.time() - test_start_time
        
        # 分析测试结果
        successful_tasks = [r for r in task_results if r.get('success')]
        failed_tasks = [r for r in task_results if not r.get('success')]
        
        if successful_tasks:
            # 计算平均性能
            avg_speed = sum(t['metrics']['processing_speed_per_second'] for t in successful_tasks) / len(successful_tasks)
            max_speed = max(t['metrics']['processing_speed_per_second'] for t in successful_tasks)
            total_processed = sum(t['metrics']['processed_count'] for t in successful_tasks)
            total_matched = sum(t['metrics']['matched_count'] for t in successful_tasks)
            avg_memory = sum(t['metrics']['memory_usage_mb'] for t in successful_tasks) / len(successful_tasks)
            
            stress_test_result = {
                'config_name': config_name,
                'test_config': test_config,
                'success': True,
                'summary': {
                    'total_test_time_seconds': test_total_time,
                    'concurrent_tasks': concurrent_tasks,
                    'successful_tasks': len(successful_tasks),
                    'failed_tasks': len(failed_tasks),
                    'total_records_processed': total_processed,
                    'total_records_matched': total_matched,
                    'average_processing_speed': avg_speed,
                    'max_processing_speed': max_speed,
                    'average_memory_usage_mb': avg_memory,
                    'overall_match_rate': total_matched / total_processed if total_processed > 0 else 0
                },
                'individual_results': task_results,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"压力测试完成: {config_name}")
            logger.info(f"平均处理速度: {avg_speed:.2f} 条/秒")
            logger.info(f"最大处理速度: {max_speed:.2f} 条/秒")
            logger.info(f"成功任务: {len(successful_tasks)}/{concurrent_tasks}")
            
            return stress_test_result
        else:
            return {
                'config_name': config_name,
                'test_config': test_config,
                'success': False,
                'error': '所有任务都失败了',
                'failed_tasks': failed_tasks,
                'timestamp': datetime.now().isoformat()
            }
    
    def run_comprehensive_stress_test(self) -> Dict:
        """运行综合压力测试"""
        logger.info("开始综合系统压力测试")
        
        if not self.setup_database():
            return {
                'success': False,
                'error': '数据库连接失败',
                'timestamp': datetime.now().isoformat()
            }
        
        self.running = True
        comprehensive_results = {
            'test_start_time': datetime.now().isoformat(),
            'system_info': self._get_system_info(),
            'test_results': {},
            'success': True
        }
        
        try:
            # 按规模递增运行测试
            for config_key, test_config in self.test_configs.items():
                if not self.running:
                    logger.info("测试被中断")
                    break
                
                logger.info(f"\n{'='*60}")
                logger.info(f"运行测试配置: {config_key}")
                logger.info(f"{'='*60}")
                
                # 运行压力测试
                result = self.run_concurrent_stress_test(test_config)
                comprehensive_results['test_results'][config_key] = result
                
                # 检查是否成功
                if not result.get('success'):
                    logger.error(f"测试配置 {config_key} 失败，停止后续测试")
                    comprehensive_results['success'] = False
                    break
                
                # 测试间隔，让系统恢复
                if config_key != list(self.test_configs.keys())[-1]:  # 不是最后一个测试
                    logger.info("等待系统恢复...")
                    time.sleep(30)
                    
                    # 强制垃圾回收
                    self.memory_processor.force_gc_if_needed()
        
        except KeyboardInterrupt:
            logger.info("用户中断测试")
            comprehensive_results['success'] = False
            comprehensive_results['interrupted'] = True
        except Exception as e:
            logger.error(f"综合测试异常: {e}")
            comprehensive_results['success'] = False
            comprehensive_results['error'] = str(e)
        finally:
            self.running = False
            comprehensive_results['test_end_time'] = datetime.now().isoformat()
        
        return comprehensive_results
    
    def _get_system_info(self) -> Dict:
        """获取系统信息"""
        try:
            memory = psutil.virtual_memory()
            return {
                'cpu_cores': psutil.cpu_count(),
                'cpu_cores_logical': psutil.cpu_count(logical=True),
                'total_memory_gb': round(memory.total / (1024**3), 2),
                'available_memory_gb': round(memory.available / (1024**3), 2),
                'python_version': sys.version,
                'platform': sys.platform
            }
        except Exception as e:
            logger.error(f"获取系统信息失败: {e}")
            return {}
    
    def save_results(self, results: Dict):
        """保存测试结果"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存详细结果
        detailed_file = f'logs/stress_test_results_{timestamp}.json'
        os.makedirs('logs', exist_ok=True)
        
        with open(detailed_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"详细结果已保存到: {detailed_file}")
        
        # 保存摘要报告
        summary_file = f'logs/stress_test_summary_{timestamp}.txt'
        with open(summary_file, 'w', encoding='utf-8') as f:
            self._write_summary_report(f, results)
        
        logger.info(f"摘要报告已保存到: {summary_file}")
    
    def _write_summary_report(self, f, results: Dict):
        """写入摘要报告"""
        f.write("=" * 80 + "\n")
        f.write("系统压力测试报告摘要\n")
        f.write("=" * 80 + "\n\n")
        
        # 测试基本信息
        f.write(f"测试开始时间: {results.get('test_start_time', 'unknown')}\n")
        f.write(f"测试结束时间: {results.get('test_end_time', 'unknown')}\n")
        f.write(f"测试状态: {'成功' if results.get('success') else '失败'}\n\n")
        
        # 系统信息
        system_info = results.get('system_info', {})
        f.write("系统配置:\n")
        f.write(f"  CPU核心数: {system_info.get('cpu_cores', 'unknown')}\n")
        f.write(f"  逻辑CPU数: {system_info.get('cpu_cores_logical', 'unknown')}\n")
        f.write(f"  总内存: {system_info.get('total_memory_gb', 'unknown')} GB\n")
        f.write(f"  可用内存: {system_info.get('available_memory_gb', 'unknown')} GB\n\n")
        
        # 各测试配置结果
        test_results = results.get('test_results', {})
        f.write("测试结果汇总:\n")
        f.write("-" * 80 + "\n")
        
        for config_name, result in test_results.items():
            f.write(f"\n{result.get('config_name', config_name)}:\n")
            
            if result.get('success'):
                summary = result.get('summary', {})
                f.write(f"  状态: 成功\n")
                f.write(f"  并发任务数: {summary.get('concurrent_tasks', 0)}\n")
                f.write(f"  处理记录数: {summary.get('total_records_processed', 0):,}\n")
                f.write(f"  匹配记录数: {summary.get('total_records_matched', 0):,}\n")
                f.write(f"  平均处理速度: {summary.get('average_processing_speed', 0):.2f} 条/秒\n")
                f.write(f"  最大处理速度: {summary.get('max_processing_speed', 0):.2f} 条/秒\n")
                f.write(f"  匹配率: {summary.get('overall_match_rate', 0):.2%}\n")
                f.write(f"  平均内存使用: {summary.get('average_memory_usage_mb', 0):.2f} MB\n")
            else:
                f.write(f"  状态: 失败\n")
                f.write(f"  错误: {result.get('error', 'unknown')}\n")
        
        f.write("\n" + "=" * 80 + "\n")


def main():
    """主函数"""
    logger.info("启动系统压力测试")
    
    # 创建压力测试器
    stress_tester = RealSystemStressTest()
    
    try:
        # 运行综合压力测试
        results = stress_tester.run_comprehensive_stress_test()
        
        # 保存结果
        stress_tester.save_results(results)
        
        # 打印摘要
        print("\n" + "=" * 80)
        print("系统压力测试完成！")
        print("=" * 80)
        
        if results.get('success'):
            print("测试状态: 成功")
            
            # 找到最佳性能
            best_speed = 0
            best_config = None
            
            for config_name, result in results.get('test_results', {}).items():
                if result.get('success'):
                    summary = result.get('summary', {})
                    max_speed = summary.get('max_processing_speed', 0)
                    if max_speed > best_speed:
                        best_speed = max_speed
                        best_config = result.get('config_name')
            
            if best_config:
                print(f"最佳性能配置: {best_config}")
                print(f"最大处理速度: {best_speed:.2f} 条/秒")
        else:
            print("测试状态: 失败")
            if 'error' in results:
                print(f"错误信息: {results['error']}")
        
        print("\n详细报告已保存到 logs/ 目录")
        
    except KeyboardInterrupt:
        logger.info("用户中断测试")
        print("\n测试被用户中断")
    except Exception as e:
        logger.error(f"压力测试异常: {e}")
        print(f"\n测试异常: {e}")


if __name__ == "__main__":
    main()
