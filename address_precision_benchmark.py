#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
地址匹配精准度基准测试
基于昨日的技术突破，建立当前系统的精准度基准线
"""

import os
import sys
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DatabaseManager
from src.matching.user_data_matcher import UserDataMatcher
from src.matching.similarity_scorer import SimilarityCalculator
from src.matching.address_normalizer import normalize_address_for_matching
from src.utils.config import ConfigManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/address_precision_benchmark.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AddressPrecisionBenchmark:
    """地址匹配精准度基准测试器"""
    
    def __init__(self):
        """初始化基准测试器"""
        # 加载配置
        self.config_manager = ConfigManager()
        self.db_manager = DatabaseManager(config=self.config_manager.get_database_config())
        self.similarity_calculator = SimilarityCalculator(self.config_manager.get_matching_config())
        
        # 测试配置
        self.test_config = {
            'sample_size': 500,  # 测试样本数量
            'precision_threshold': 0.8,  # 精准匹配阈值
            'similarity_threshold': 0.6,  # 相似度阈值
            'max_candidates': 10  # 最大候选数量
        }
        
        # 测试结果统计
        self.results = {
            'total_tests': 0,
            'successful_matches': 0,
            'false_positives': 0,
            'false_negatives': 0,
            'precision': 0.0,
            'recall': 0.0,
            'f1_score': 0.0,
            'detailed_results': []
        }
        
        logger.info("🚀 地址匹配精准度基准测试器初始化完成")
    
    def prepare_test_dataset(self) -> List[Dict]:
        """
        准备测试数据集
        选择500条具有代表性的地址数据
        """
        logger.info("📊 准备测试数据集...")
        
        try:
            # 从实际的用户数据表获取地址样本
            # 使用hztj_hzxx表作为源数据
            source_collection = self.db_manager.get_collection('hztj_hzxx')
            
            # 先检查表中的字段结构
            sample_record = source_collection.find_one()
            if not sample_record:
                logger.error("hztj_hzxx表中没有数据")
                return []
            
            logger.info(f"hztj_hzxx表样本记录字段: {list(sample_record.keys())}")
            
            # 查找包含地址信息的字段
            address_fields = []
            for field in sample_record.keys():
                if any(keyword in field.lower() for keyword in ['address', 'addr', 'dz', '地址', '住址']):
                    address_fields.append(field)
            
            if not address_fields:
                # 如果没有明显的地址字段，查看所有字段的值
                logger.info("未找到明显的地址字段，检查所有字段...")
                for field, value in sample_record.items():
                    if isinstance(value, str) and len(value) > 10:
                        # 检查是否包含地址关键词
                        if any(keyword in value for keyword in ['路', '街', '区', '市', '号', '弄', '村']):
                            address_fields.append(field)
                            logger.info(f"发现可能的地址字段: {field} = {value[:50]}...")
            
            if not address_fields:
                logger.error("未找到包含地址信息的字段")
                return []
            
            # 使用第一个地址字段获取测试样本
            primary_address_field = address_fields[0]
            logger.info(f"使用地址字段: {primary_address_field}")
            
            # 构建查询条件，获取有地址数据的记录
            query = {primary_address_field: {'$exists': True, '$ne': '', '$ne': None}}
            
            # 获取测试样本
            test_samples = list(source_collection.find(query).limit(self.test_config['sample_size']))
            
            # 标准化样本格式，统一地址字段名
            for sample in test_samples:
                sample['address'] = sample.get(primary_address_field, '')
                sample['source_table'] = 'hztj_hzxx'
            
            logger.info(f"✅ 测试数据集准备完成，共 {len(test_samples)} 条样本")
            return test_samples
            
        except Exception as e:
            logger.error(f"❌ 准备测试数据集失败: {str(e)}")
            return []
    
    def create_ground_truth_pairs(self, test_samples: List[Dict]) -> List[Dict]:
        """
        创建地址匹配的标准答案对
        基于地址相似度和人工规则生成
        """
        logger.info("🎯 创建标准答案对...")
        
        ground_truth_pairs = []
        
        try:
            # 获取目标数据集（用于匹配的数据）- 使用dwd_yljgxx表
            target_collection = self.db_manager.get_collection('dwd_yljgxx')
            
            # 先检查目标表的字段结构
            target_sample = target_collection.find_one()
            if not target_sample:
                logger.error("dwd_yljgxx表中没有数据")
                return []
            
            # 查找目标表中的地址字段
            target_address_field = None
            for field in target_sample.keys():
                if any(keyword in field.lower() for keyword in ['address', 'addr', 'dz', '地址', '住址']):
                    target_address_field = field
                    break
            
            if not target_address_field:
                # 检查所有字段的值
                for field, value in target_sample.items():
                    if isinstance(value, str) and len(value) > 10:
                        if any(keyword in value for keyword in ['路', '街', '区', '市', '号', '弄', '村']):
                            target_address_field = field
                            break
            
            if not target_address_field:
                logger.error("目标表中未找到地址字段")
                return []
            
            logger.info(f"目标表使用地址字段: {target_address_field}")
            
            target_samples = list(target_collection.find(
                {target_address_field: {'$exists': True, '$ne': ''}},
                {'_id': 1, target_address_field: 1}
            ).limit(2000))  # 限制目标数据量以提高效率
            
            # 统一地址字段名
            for sample in target_samples:
                sample['address'] = sample.get(target_address_field, '')
            
            logger.info(f"获取到 {len(target_samples)} 条目标数据用于匹配")
            
            for i, source_record in enumerate(test_samples[:100]):  # 限制到100条以提高效率
                source_address = source_record.get('address', '')
                if not source_address:
                    continue
                
                logger.info(f"处理第 {i+1}/100 条源地址: {source_address[:50]}...")
                
                # 标准化源地址
                normalized_source = normalize_address_for_matching(source_address)
                
                # 寻找最佳匹配
                best_matches = []
                
                for target_record in target_samples:
                    target_address = target_record.get('address', '')
                    if not target_address:
                        continue
                    
                    # 标准化目标地址
                    normalized_target = normalize_address_for_matching(target_address)
                    
                    # 计算地址相似度
                    similarity = self.similarity_calculator.calculate_address_similarity(
                        normalized_source, normalized_target
                    )
                    
                    if similarity >= 0.3:  # 只考虑有一定相似度的候选
                        best_matches.append({
                            'target_record': target_record,
                            'similarity': similarity,
                            'source_address': source_address,
                            'target_address': target_address,
                            'normalized_source': normalized_source,
                            'normalized_target': normalized_target
                        })
                
                # 按相似度排序，取前5个
                best_matches.sort(key=lambda x: x['similarity'], reverse=True)
                best_matches = best_matches[:5]
                
                # 创建标准答案对
                for match in best_matches:
                    # 基于相似度和规则判断是否为正确匹配
                    is_correct_match = self._evaluate_match_correctness(
                        source_record, match['target_record'], match['similarity']
                    )
                    
                    ground_truth_pairs.append({
                        'source_record': source_record,
                        'target_record': match['target_record'],
                        'similarity_score': match['similarity'],
                        'is_correct_match': is_correct_match,
                        'source_address': match['source_address'],
                        'target_address': match['target_address'],
                        'normalized_source': match['normalized_source'],
                        'normalized_target': match['normalized_target'],
                        'match_type': self._classify_match_type(match['similarity'])
                    })
            
            logger.info(f"✅ 创建了 {len(ground_truth_pairs)} 个标准答案对")
            return ground_truth_pairs
            
        except Exception as e:
            logger.error(f"❌ 创建标准答案对失败: {str(e)}")
            return []
    
    def _evaluate_match_correctness(self, source_record: Dict, target_record: Dict, similarity: float) -> bool:
        """
        评估匹配的正确性
        基于相似度阈值和业务规则
        """
        # 高相似度直接认为正确
        if similarity >= 0.9:
            return True
        
        # 中等相似度需要进一步验证
        if similarity >= 0.7:
            # 检查单位名称是否也相似
            source_name = source_record.get('unit_name', '')
            target_name = target_record.get('unit_name', '')
            
            if source_name and target_name:
                name_similarity = self.similarity_calculator.calculate_string_similarity(
                    source_name, target_name
                )
                # 地址和单位名称都有一定相似度，认为正确
                return name_similarity >= 0.6
            
            # 只有地址相似度，需要更高的阈值
            return similarity >= 0.8
        
        # 低相似度认为不正确
        return False
    
    def _classify_match_type(self, similarity: float) -> str:
        """分类匹配类型"""
        if similarity >= 0.9:
            return "high_precision"
        elif similarity >= 0.7:
            return "medium_precision"
        elif similarity >= 0.5:
            return "low_precision"
        else:
            return "poor_match"
    
    def run_baseline_test(self, ground_truth_pairs: List[Dict]) -> Dict:
        """
        运行基准测试
        使用当前系统对标准答案对进行匹配测试
        """
        logger.info("🧪 开始运行基准测试...")
        
        test_results = {
            'total_pairs': len(ground_truth_pairs),
            'correct_predictions': 0,
            'false_positives': 0,
            'false_negatives': 0,
            'true_positives': 0,
            'true_negatives': 0,
            'detailed_results': [],
            'performance_metrics': {}
        }
        
        start_time = time.time()
        
        try:
            for i, pair in enumerate(ground_truth_pairs):
                logger.info(f"测试第 {i+1}/{len(ground_truth_pairs)} 对...")
                
                source_record = pair['source_record']
                target_record = pair['target_record']
                expected_match = pair['is_correct_match']
                
                # 使用当前系统计算相似度
                predicted_similarity = self.similarity_calculator.calculate_address_similarity(
                    pair['source_address'], pair['target_address']
                )
                
                # 基于阈值判断系统预测结果
                predicted_match = predicted_similarity >= self.test_config['similarity_threshold']
                
                # 统计结果
                if expected_match and predicted_match:
                    test_results['true_positives'] += 1
                    test_results['correct_predictions'] += 1
                elif not expected_match and not predicted_match:
                    test_results['true_negatives'] += 1
                    test_results['correct_predictions'] += 1
                elif not expected_match and predicted_match:
                    test_results['false_positives'] += 1
                elif expected_match and not predicted_match:
                    test_results['false_negatives'] += 1
                
                # 记录详细结果
                test_results['detailed_results'].append({
                    'pair_index': i,
                    'source_address': pair['source_address'],
                    'target_address': pair['target_address'],
                    'expected_match': expected_match,
                    'predicted_match': predicted_match,
                    'predicted_similarity': predicted_similarity,
                    'ground_truth_similarity': pair['similarity_score'],
                    'match_type': pair['match_type'],
                    'is_correct': expected_match == predicted_match
                })
        
        except Exception as e:
            logger.error(f"❌ 基准测试执行失败: {str(e)}")
            return test_results
        
        # 计算性能指标
        end_time = time.time()
        test_duration = end_time - start_time
        
        tp = test_results['true_positives']
        tn = test_results['true_negatives']
        fp = test_results['false_positives']
        fn = test_results['false_negatives']
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0.0
        
        test_results['performance_metrics'] = {
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score,
            'accuracy': accuracy,
            'test_duration': test_duration,
            'pairs_per_second': len(ground_truth_pairs) / test_duration if test_duration > 0 else 0
        }
        
        logger.info(f"✅ 基准测试完成")
        logger.info(f"📊 准确率: {accuracy:.3f}, 精确率: {precision:.3f}, 召回率: {recall:.3f}, F1分数: {f1_score:.3f}")
        
        return test_results
    
    def analyze_results(self, test_results: Dict) -> Dict:
        """
        分析测试结果
        识别误匹配案例和优化方向
        """
        logger.info("📈 分析测试结果...")
        
        analysis = {
            'summary': {},
            'error_analysis': {},
            'optimization_recommendations': []
        }
        
        try:
            # 基础统计
            metrics = test_results['performance_metrics']
            analysis['summary'] = {
                'total_pairs_tested': test_results['total_pairs'],
                'accuracy': metrics['accuracy'],
                'precision': metrics['precision'],
                'recall': metrics['recall'],
                'f1_score': metrics['f1_score'],
                'test_duration': metrics['test_duration'],
                'processing_speed': metrics['pairs_per_second']
            }
            
            # 错误分析
            false_positives = []
            false_negatives = []
            
            for result in test_results['detailed_results']:
                if not result['is_correct']:
                    if result['predicted_match'] and not result['expected_match']:
                        false_positives.append(result)
                    elif not result['predicted_match'] and result['expected_match']:
                        false_negatives.append(result)
            
            analysis['error_analysis'] = {
                'false_positives_count': len(false_positives),
                'false_negatives_count': len(false_negatives),
                'false_positive_examples': false_positives[:5],  # 前5个示例
                'false_negative_examples': false_negatives[:5]   # 前5个示例
            }
            
            # 生成优化建议
            recommendations = []
            
            if metrics['precision'] < 0.8:
                recommendations.append({
                    'issue': 'precision_low',
                    'description': f'精确率偏低 ({metrics["precision"]:.3f})',
                    'suggestion': '提高相似度阈值，减少误匹配',
                    'priority': 'high'
                })
            
            if metrics['recall'] < 0.7:
                recommendations.append({
                    'issue': 'recall_low', 
                    'description': f'召回率偏低 ({metrics["recall"]:.3f})',
                    'suggestion': '降低相似度阈值或优化地址标准化',
                    'priority': 'high'
                })
            
            if len(false_positives) > len(false_negatives) * 2:
                recommendations.append({
                    'issue': 'too_many_false_positives',
                    'description': '误匹配过多',
                    'suggestion': '加强地址层级验证，提高匹配严格性',
                    'priority': 'medium'
                })
            
            analysis['optimization_recommendations'] = recommendations
            
            logger.info(f"✅ 结果分析完成，生成了 {len(recommendations)} 条优化建议")
            
        except Exception as e:
            logger.error(f"❌ 结果分析失败: {str(e)}")
        
        return analysis
    
    def save_benchmark_report(self, test_results: Dict, analysis: Dict) -> str:
        """
        保存基准测试报告
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_filename = f'address_precision_benchmark_report_{timestamp}.json'
        
        report = {
            'test_info': {
                'timestamp': timestamp,
                'test_config': self.test_config,
                'system_version': '2.0.8'
            },
            'test_results': test_results,
            'analysis': analysis
        }
        
        try:
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"📄 基准测试报告已保存: {report_filename}")
            return report_filename
            
        except Exception as e:
            logger.error(f"❌ 保存报告失败: {str(e)}")
            return ""
    
    def run_complete_benchmark(self) -> Dict:
        """
        运行完整的基准测试流程
        """
        logger.info("🚀 开始完整基准测试流程...")
        
        try:
            # 1. 准备测试数据集
            test_samples = self.prepare_test_dataset()
            if not test_samples:
                raise Exception("无法准备测试数据集")
            
            # 2. 创建标准答案对
            ground_truth_pairs = self.create_ground_truth_pairs(test_samples)
            if not ground_truth_pairs:
                raise Exception("无法创建标准答案对")
            
            # 3. 运行基准测试
            test_results = self.run_baseline_test(ground_truth_pairs)
            
            # 4. 分析结果
            analysis = self.analyze_results(test_results)
            
            # 5. 保存报告
            report_file = self.save_benchmark_report(test_results, analysis)
            
            logger.info("🎉 完整基准测试流程完成！")
            
            return {
                'success': True,
                'test_results': test_results,
                'analysis': analysis,
                'report_file': report_file
            }
            
        except Exception as e:
            logger.error(f"❌ 基准测试流程失败: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """主函数"""
    logger.info("🎯 地址匹配精准度基准测试开始")
    
    try:
        # 创建基准测试器
        benchmark = AddressPrecisionBenchmark()
        
        # 运行完整测试
        result = benchmark.run_complete_benchmark()
        
        if result['success']:
            # 输出关键指标
            metrics = result['test_results']['performance_metrics']
            print("\n" + "="*60)
            print("📊 地址匹配精准度基准测试结果")
            print("="*60)
            print(f"准确率 (Accuracy): {metrics['accuracy']:.3f}")
            print(f"精确率 (Precision): {metrics['precision']:.3f}")
            print(f"召回率 (Recall): {metrics['recall']:.3f}")
            print(f"F1分数: {metrics['f1_score']:.3f}")
            print(f"处理速度: {metrics['pairs_per_second']:.1f} 对/秒")
            print(f"测试用时: {metrics['test_duration']:.1f} 秒")
            print("="*60)
            
            # 输出优化建议
            recommendations = result['analysis']['optimization_recommendations']
            if recommendations:
                print("\n🎯 优化建议:")
                for i, rec in enumerate(recommendations, 1):
                    print(f"{i}. {rec['description']}")
                    print(f"   建议: {rec['suggestion']}")
                    print(f"   优先级: {rec['priority']}")
            
            print(f"\n📄 详细报告已保存: {result['report_file']}")
        else:
            print(f"❌ 测试失败: {result['error']}")
    
    except Exception as e:
        logger.error(f"❌ 程序执行失败: {str(e)}")
        print(f"❌ 程序执行失败: {str(e)}")


if __name__ == "__main__":
    main()
