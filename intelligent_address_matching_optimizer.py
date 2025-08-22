#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能地址匹配优化器
解决当前0%匹配率的问题，实现动态权重配置和智能匹配策略
"""

import os
import sys
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
from src.matching.similarity_scorer import SimilarityCalculator
from src.matching.address_normalizer import normalize_address_for_matching

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntelligentAddressMatchingOptimizer:
    """智能地址匹配优化器"""
    
    def __init__(self):
        """初始化优化器"""
        self.config_manager = ConfigManager()
        self.db_manager = DatabaseManager(config=self.config_manager.get_database_config())
        
        # 定义智能匹配策略
        self.intelligent_strategies = {
            'flexible_hierarchical': {
                'name': '灵活层级策略',
                'description': '允许跨区匹配，重点关注街道和门牌号',
                'config': {
                    'similarity_threshold': 0.6,
                    'high_precision_threshold': 0.85,
                    'medium_precision_threshold': 0.6,
                    'enable_cross_district_matching': True,  # 允许跨区匹配
                    'street_weight': 0.4,  # 街道权重
                    'number_weight': 0.3,  # 门牌号权重
                    'district_weight': 0.2,  # 区县权重（降低）
                    'building_weight': 0.1   # 建筑物权重
                }
            },
            'semantic_matching': {
                'name': '语义匹配策略',
                'description': '基于地址语义相似度，忽略行政区划差异',
                'config': {
                    'similarity_threshold': 0.5,
                    'high_precision_threshold': 0.8,
                    'medium_precision_threshold': 0.5,
                    'enable_semantic_matching': True,  # 启用语义匹配
                    'ignore_district_mismatch': True,  # 忽略区县不匹配
                    'focus_on_location_keywords': True,  # 关注位置关键词
                    'keyword_weight': 0.5,  # 关键词权重
                    'location_weight': 0.3,  # 位置权重
                    'administrative_weight': 0.2  # 行政区划权重（大幅降低）
                }
            },
            'fuzzy_matching': {
                'name': '模糊匹配策略',
                'description': '大幅降低所有阈值，最大化匹配可能性',
                'config': {
                    'similarity_threshold': 0.3,
                    'high_precision_threshold': 0.7,
                    'medium_precision_threshold': 0.3,
                    'province_threshold': 0.7,  # 省级阈值降低
                    'city_threshold': 0.6,     # 市级阈值降低
                    'district_threshold': 0.4,  # 区县级阈值大幅降低
                    'town_threshold': 0.4,     # 镇街级阈值降低
                    'community_threshold': 0.3, # 小区级阈值降低
                    'street_threshold': 0.3,   # 路级阈值降低
                    'lane_threshold': 0.3,     # 弄级阈值降低
                    'number_threshold': 0.3    # 门牌级阈值降低
                }
            }
        }
    
    def run_intelligent_optimization(self) -> Dict:
        """运行智能优化测试"""
        print("🧠 智能地址匹配优化测试")
        print("=" * 50)
        
        try:
            # 1. 准备测试数据
            test_data = self._prepare_test_data()
            if not test_data or not test_data.get('source_samples'):
                return {'error': '无法获取测试数据'}
            
            print(f"📊 测试数据准备完成:")
            print(f"  源数据: {len(test_data['source_samples'])} 条")
            print(f"  目标数据: {len(test_data['target_samples'])} 条")
            
            # 2. 执行基准测试（当前配置）
            print("\\n📈 执行基准测试...")
            baseline_result = self._test_baseline_matching(test_data)
            
            # 3. 测试智能策略
            optimization_results = []
            for strategy_key, strategy in self.intelligent_strategies.items():
                print(f"\\n🧪 测试 {strategy['name']}...")
                result = self._test_intelligent_strategy(strategy_key, strategy, test_data)
                result['improvement'] = self._calculate_improvement(baseline_result, result)
                optimization_results.append(result)
            
            # 4. 选择最佳策略
            best_strategy = self._select_best_strategy(optimization_results)
            
            # 5. 生成优化建议
            recommendations = self._generate_intelligent_recommendations(
                baseline_result, optimization_results, best_strategy
            )
            
            return {
                'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'baseline_performance': baseline_result,
                'optimization_results': optimization_results,
                'best_strategy': best_strategy,
                'recommendations': recommendations
            }
            
        except Exception as e:
            logger.error(f"智能优化测试失败: {str(e)}")
            return {'error': str(e)}
    
    def _prepare_test_data(self) -> Dict:
        """准备测试数据"""
        try:
            # 获取源表数据（火灾地点）
            source_collection = self.db_manager.get_collection('hztj_hzxx')
            source_samples = list(source_collection.find(
                {'起火地点': {'$exists': True, '$ne': '', '$ne': None}}
            ).limit(15))  # 减少到15条以提高测试效率
            
            # 获取目标表数据（养老机构地址）
            target_collection = self.db_manager.get_collection('dwd_yljgxx')
            target_samples = list(target_collection.find(
                {'ZCDZ': {'$exists': True, '$ne': '', '$ne': None}}
            ).limit(25))  # 减少到25条
            
            # 标准化字段名
            for sample in source_samples:
                sample['address'] = sample.get('起火地点', '')
            
            for sample in target_samples:
                sample['address'] = sample.get('ZCDZ', '')
            
            return {
                'source_samples': source_samples,
                'target_samples': target_samples,
                'source_field': '起火地点',
                'target_field': 'ZCDZ'
            }
            
        except Exception as e:
            logger.error(f"准备测试数据失败: {str(e)}")
            return {}
    
    def _test_baseline_matching(self, test_data: Dict) -> Dict:
        """测试基准匹配性能"""
        start_time = time.time()
        
        # 使用当前配置
        similarity_calculator = SimilarityCalculator(self.config_manager.get_matching_config())
        
        results = {
            'strategy_name': '当前配置（基准）',
            'total_tests': len(test_data['source_samples']),
            'matches_found': 0,
            'high_precision_matches': 0,
            'medium_precision_matches': 0,
            'match_details': []
        }
        
        for i, source_record in enumerate(test_data['source_samples']):
            source_address = source_record.get('address', '')
            if not source_address:
                continue
            
            # 标准化源地址
            normalized_source = normalize_address_for_matching(source_address)
            
            # 寻找最佳匹配
            best_similarity = 0.0
            best_match = None
            
            for target_record in test_data['target_samples']:
                target_address = target_record.get('address', '')
                if not target_address:
                    continue
                
                # 标准化目标地址
                normalized_target = normalize_address_for_matching(target_address)
                
                # 计算相似度
                similarity = similarity_calculator.calculate_address_similarity(
                    normalized_source, normalized_target
                )
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = {
                        'target_address': target_address,
                        'similarity': similarity
                    }
            
            # 分类匹配结果
            if best_similarity >= 0.7:  # 当前阈值
                results['matches_found'] += 1
                if best_similarity >= 0.9:
                    results['high_precision_matches'] += 1
                elif best_similarity >= 0.7:
                    results['medium_precision_matches'] += 1
            
            # 记录详细结果（前5条）
            if len(results['match_details']) < 5:
                results['match_details'].append({
                    'source_address': source_address,
                    'best_match_address': best_match['target_address'] if best_match else '',
                    'similarity_score': best_similarity,
                    'has_match': best_similarity >= 0.7
                })
        
        # 计算性能指标
        total_time = time.time() - start_time
        results['processing_time'] = total_time
        results['tests_per_second'] = len(test_data['source_samples']) / total_time if total_time > 0 else 0
        results['match_rate'] = results['matches_found'] / results['total_tests'] * 100
        results['high_precision_rate'] = results['high_precision_matches'] / results['total_tests'] * 100
        
        return results
    
    def _test_intelligent_strategy(self, strategy_key: str, strategy: Dict, test_data: Dict) -> Dict:
        """测试智能策略"""
        start_time = time.time()
        
        strategy_config = strategy['config']
        
        results = {
            'strategy_key': strategy_key,
            'strategy_name': strategy['name'],
            'description': strategy['description'],
            'total_tests': len(test_data['source_samples']),
            'matches_found': 0,
            'high_precision_matches': 0,
            'medium_precision_matches': 0,
            'match_details': []
        }
        
        for i, source_record in enumerate(test_data['source_samples']):
            source_address = source_record.get('address', '')
            if not source_address:
                continue
            
            # 标准化源地址
            normalized_source = normalize_address_for_matching(source_address)
            
            # 寻找最佳匹配
            best_similarity = 0.0
            best_match = None
            
            for target_record in test_data['target_samples']:
                target_address = target_record.get('address', '')
                if not target_address:
                    continue
                
                # 标准化目标地址
                normalized_target = normalize_address_for_matching(target_address)
                
                # 使用智能策略计算相似度
                similarity = self._calculate_intelligent_similarity(
                    normalized_source, normalized_target, strategy_config
                )
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = {
                        'target_address': target_address,
                        'similarity': similarity
                    }
            
            # 分类匹配结果
            similarity_threshold = strategy_config.get('similarity_threshold', 0.7)
            high_precision_threshold = strategy_config.get('high_precision_threshold', 0.9)
            medium_precision_threshold = strategy_config.get('medium_precision_threshold', 0.7)
            
            if best_similarity >= similarity_threshold:
                results['matches_found'] += 1
                if best_similarity >= high_precision_threshold:
                    results['high_precision_matches'] += 1
                elif best_similarity >= medium_precision_threshold:
                    results['medium_precision_matches'] += 1
            
            # 记录详细结果（前5条）
            if len(results['match_details']) < 5:
                results['match_details'].append({
                    'source_address': source_address,
                    'best_match_address': best_match['target_address'] if best_match else '',
                    'similarity_score': best_similarity,
                    'has_match': best_similarity >= similarity_threshold
                })
        
        # 计算性能指标
        total_time = time.time() - start_time
        results['processing_time'] = total_time
        results['tests_per_second'] = len(test_data['source_samples']) / total_time if total_time > 0 else 0
        results['match_rate'] = results['matches_found'] / results['total_tests'] * 100
        results['high_precision_rate'] = results['high_precision_matches'] / results['total_tests'] * 100
        
        return results
    
    def _calculate_intelligent_similarity(self, addr1: str, addr2: str, strategy_config: Dict) -> float:
        """使用智能策略计算地址相似度"""
        
        # 基础文本相似度（使用简单的关键词匹配）
        def simple_text_similarity(text1: str, text2: str) -> float:
            # 提取关键词
            keywords1 = set([char for char in text1 if char.isalnum()])
            keywords2 = set([char for char in text2 if char.isalnum()])
            
            if not keywords1 or not keywords2:
                return 0.0
            
            # 计算交集比例
            intersection = keywords1.intersection(keywords2)
            union = keywords1.union(keywords2)
            
            return len(intersection) / len(union) if union else 0.0
        
        # 策略1: 灵活层级策略
        if strategy_config.get('enable_cross_district_matching'):
            # 提取关键信息
            addr1_keywords = [word for word in ['路', '街', '号', '弄', '村', '镇'] if word in addr1]
            addr2_keywords = [word for word in ['路', '街', '号', '弄', '村', '镇'] if word in addr2]
            
            # 如果都包含路/街信息，重点比较
            if any(kw in addr1 for kw in ['路', '街']) and any(kw in addr2 for kw in ['路', '街']):
                return simple_text_similarity(addr1, addr2) * 1.2  # 加权
        
        # 策略2: 语义匹配策略
        if strategy_config.get('enable_semantic_matching'):
            # 忽略行政区划，重点关注具体位置
            location_keywords = ['路', '街', '号', '弄', '村', '镇', '院', '中心', '大厦', '广场']
            
            # 提取位置关键词
            addr1_locations = [kw for kw in location_keywords if kw in addr1]
            addr2_locations = [kw for kw in location_keywords if kw in addr2]
            
            if addr1_locations and addr2_locations:
                # 基于位置关键词的相似度
                common_locations = set(addr1_locations).intersection(set(addr2_locations))
                total_locations = set(addr1_locations).union(set(addr2_locations))
                
                if total_locations:
                    location_similarity = len(common_locations) / len(total_locations)
                    text_similarity = simple_text_similarity(addr1, addr2)
                    
                    # 综合相似度
                    return location_similarity * 0.6 + text_similarity * 0.4
        
        # 策略3: 模糊匹配策略（默认）
        return simple_text_similarity(addr1, addr2)
    
    def _calculate_improvement(self, baseline: Dict, candidate: Dict) -> Dict:
        """计算相对于基准的改进"""
        baseline_match_rate = baseline.get('match_rate', 0)
        candidate_match_rate = candidate.get('match_rate', 0)
        
        baseline_high_rate = baseline.get('high_precision_rate', 0)
        candidate_high_rate = candidate.get('high_precision_rate', 0)
        
        return {
            'match_rate_improvement': candidate_match_rate - baseline_match_rate,
            'high_precision_improvement': candidate_high_rate - baseline_high_rate,
            'speed_improvement': candidate.get('tests_per_second', 0) - baseline.get('tests_per_second', 0),
            'match_rate_change_percent': ((candidate_match_rate - baseline_match_rate) / baseline_match_rate * 100) if baseline_match_rate > 0 else float('inf') if candidate_match_rate > 0 else 0
        }
    
    def _select_best_strategy(self, results: List[Dict]) -> Dict:
        """选择最佳策略"""
        if not results:
            return None
        
        # 综合评分：匹配率 * 0.7 + 高精度率 * 0.3
        best_strategy = None
        best_score = -1
        
        for result in results:
            match_rate = result.get('match_rate', 0)
            high_precision_rate = result.get('high_precision_rate', 0)
            
            composite_score = match_rate * 0.7 + high_precision_rate * 0.3
            
            if composite_score > best_score:
                best_score = composite_score
                best_strategy = result
        
        if best_strategy:
            best_strategy['composite_score'] = best_score
        
        return best_strategy
    
    def _generate_intelligent_recommendations(self, baseline: Dict, results: List[Dict], best_strategy: Dict) -> List[str]:
        """生成智能优化建议"""
        recommendations = []
        
        baseline_match_rate = baseline.get('match_rate', 0)
        
        # 分析当前问题
        if baseline_match_rate == 0:
            recommendations.append("🚨 严重问题：当前配置匹配率为0%，层级终止验证过于严格")
            recommendations.append("💡 建议：立即采用'模糊匹配策略'，大幅降低所有层级阈值")
        
        # 分析最佳策略
        if best_strategy and best_strategy.get('match_rate', 0) > baseline_match_rate:
            improvement = best_strategy.get('match_rate', 0) - baseline_match_rate
            recommendations.append(
                f"🏆 推荐采用'{best_strategy['strategy_name']}'，可将匹配率提升 {improvement:.1f}%"
            )
            recommendations.append(f"📋 策略描述：{best_strategy.get('description', '')}")
        
        # 技术建议
        recommendations.append("🔧 技术优化建议：")
        recommendations.append("  1. 实现跨区匹配机制，允许不同区县间的地址匹配")
        recommendations.append("  2. 增强语义匹配能力，重点关注街道、门牌号等关键信息")
        recommendations.append("  3. 优化层级终止验证，避免过早终止匹配过程")
        recommendations.append("  4. 引入地址类型识别，针对不同类型采用不同匹配策略")
        
        return recommendations

def main():
    """主函数"""
    try:
        optimizer = IntelligentAddressMatchingOptimizer()
        results = optimizer.run_intelligent_optimization()
        
        if 'error' in results:
            print(f"❌ 智能优化测试失败: {results['error']}")
            return
        
        # 显示基准测试结果
        baseline = results['baseline_performance']
        print(f"\\n📊 基准测试结果:")
        print(f"  总测试数: {baseline['total_tests']}")
        print(f"  匹配率: {baseline['match_rate']:.1f}%")
        print(f"  高精度率: {baseline['high_precision_rate']:.1f}%")
        print(f"  处理速度: {baseline['tests_per_second']:.1f} 条/秒")
        
        # 显示智能策略结果
        print(f"\\n🧠 智能策略测试结果:")
        for result in results['optimization_results']:
            improvement = result['improvement']
            print(f"\\n  {result['strategy_name']}:")
            print(f"    匹配率: {result['match_rate']:.1f}% (改进: {improvement['match_rate_improvement']:+.1f}%)")
            print(f"    高精度率: {result['high_precision_rate']:.1f}% (改进: {improvement['high_precision_improvement']:+.1f}%)")
            print(f"    处理速度: {result['tests_per_second']:.1f} 条/秒")
            print(f"    策略描述: {result['description']}")
        
        # 显示最佳策略
        best_strategy = results['best_strategy']
        if best_strategy:
            print(f"\\n🏆 推荐最佳策略: {best_strategy['strategy_name']}")
            print(f"  综合评分: {best_strategy['composite_score']:.1f}")
            print(f"  匹配率: {best_strategy['match_rate']:.1f}%")
            print(f"  高精度率: {best_strategy['high_precision_rate']:.1f}%")
        
        # 显示优化建议
        print(f"\\n💡 智能优化建议:")
        for i, rec in enumerate(results['recommendations'], 1):
            print(f"  {i}. {rec}")
        
        # 保存详细报告
        report_file = f"intelligent_optimization_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\\n📄 详细报告已保存至: {report_file}")
        
    except Exception as e:
        print(f"❌ 智能优化测试执行失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

