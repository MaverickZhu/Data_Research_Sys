#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
地址匹配阈值参数精细调优
基于基准测试结果，优化各级地址组件的相似度阈值
"""

import os
import sys
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import copy

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
from src.matching.similarity_scorer import SimilarityCalculator
from src.matching.address_normalizer import normalize_address_for_matching

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ThresholdOptimizer:
    """地址匹配阈值优化器"""
    
    def __init__(self):
        """初始化优化器"""
        self.config_manager = ConfigManager()
        self.db_manager = DatabaseManager(config=self.config_manager.get_database_config())
        
        # 当前阈值配置（从基准测试分析得出）
        self.current_thresholds = {
            'similarity_threshold': 0.7,  # 总体匹配阈值
            'high_precision_threshold': 0.9,  # 高精度阈值
            'medium_precision_threshold': 0.7,  # 中精度阈值
            
            # 层级验证阈值（从similarity_scorer.py分析得出）
            'province_threshold': 0.85,  # 省级阈值
            'city_threshold': 0.85,      # 市级阈值
            'district_threshold': 0.80,  # 区县级阈值（关键问题所在）
            'town_threshold': 0.75,      # 镇街级阈值
            'community_threshold': 0.70, # 小区级阈值
            'street_threshold': 0.70,    # 路级阈值
            'lane_threshold': 0.65,      # 弄级阈值
            'number_threshold': 0.60     # 门牌级阈值
        }
        
        # 优化候选阈值配置
        self.optimization_candidates = [
            {
                'name': '宽松配置',
                'description': '降低区县级阈值，提高匹配率',
                'thresholds': {
                    'similarity_threshold': 0.6,   # 降低总体阈值
                    'high_precision_threshold': 0.85,  # 降低高精度阈值
                    'medium_precision_threshold': 0.6,  # 降低中精度阈值
                    'province_threshold': 0.80,
                    'city_threshold': 0.80,
                    'district_threshold': 0.65,    # 关键：大幅降低区县级阈值
                    'town_threshold': 0.60,        # 降低镇街级阈值
                    'community_threshold': 0.55,
                    'street_threshold': 0.55,
                    'lane_threshold': 0.50,
                    'number_threshold': 0.45
                }
            },
            {
                'name': '平衡配置',
                'description': '适度降低阈值，平衡精准度和召回率',
                'thresholds': {
                    'similarity_threshold': 0.65,
                    'high_precision_threshold': 0.88,
                    'medium_precision_threshold': 0.65,
                    'province_threshold': 0.82,
                    'city_threshold': 0.82,
                    'district_threshold': 0.72,    # 适度降低区县级阈值
                    'town_threshold': 0.68,
                    'community_threshold': 0.62,
                    'street_threshold': 0.62,
                    'lane_threshold': 0.58,
                    'number_threshold': 0.52
                }
            },
            {
                'name': '渐进配置',
                'description': '小幅调整阈值，保持较高精准度',
                'thresholds': {
                    'similarity_threshold': 0.68,
                    'high_precision_threshold': 0.90,
                    'medium_precision_threshold': 0.68,
                    'province_threshold': 0.83,
                    'city_threshold': 0.83,
                    'district_threshold': 0.75,    # 小幅降低区县级阈值
                    'town_threshold': 0.72,
                    'community_threshold': 0.68,
                    'street_threshold': 0.68,
                    'lane_threshold': 0.62,
                    'number_threshold': 0.55
                }
            }
        ]
        
    def run_threshold_optimization(self) -> Dict:
        """运行阈值优化测试"""
        logger.info("🔧 开始地址匹配阈值优化测试")
        
        optimization_results = {
            'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'baseline_performance': None,
            'optimization_results': [],
            'best_configuration': None,
            'recommendations': []
        }
        
        # 1. 获取测试数据
        test_data = self._prepare_test_data()
        if not test_data:
            return {'error': '无法获取测试数据'}
        
        logger.info(f"准备了 {len(test_data['source_samples'])} 条源数据和 {len(test_data['target_samples'])} 条目标数据")
        
        # 2. 基准测试（当前配置）
        logger.info("📊 执行基准测试（当前配置）...")
        baseline_result = self._test_threshold_configuration(
            "当前配置", 
            self.current_thresholds, 
            test_data
        )
        optimization_results['baseline_performance'] = baseline_result
        
        # 3. 测试优化候选配置
        for candidate in self.optimization_candidates:
            logger.info(f"🧪 测试 {candidate['name']} - {candidate['description']}")
            
            result = self._test_threshold_configuration(
                candidate['name'],
                candidate['thresholds'],
                test_data
            )
            
            # 计算相对于基准的改进
            result['improvement'] = self._calculate_improvement(baseline_result, result)
            optimization_results['optimization_results'].append(result)
        
        # 4. 选择最佳配置
        best_config = self._select_best_configuration(optimization_results['optimization_results'])
        optimization_results['best_configuration'] = best_config
        
        # 5. 生成优化建议
        optimization_results['recommendations'] = self._generate_optimization_recommendations(
            baseline_result, optimization_results['optimization_results']
        )
        
        return optimization_results
    
    def _prepare_test_data(self) -> Dict:
        """准备测试数据"""
        try:
            # 获取源表数据\n            source_collection = self.db_manager.get_collection('hztj_hzxx')\n            source_sample = source_collection.find_one()\n            if not source_sample:\n                return {}\n            \n            # 查找源表地址字段\n            source_address_field = None\n            for field, value in source_sample.items():\n                if isinstance(value, str) and len(value) > 5:\n                    if any(keyword in value for keyword in ['路', '街', '区', '市', '号', '弄', '村']):\n                        source_address_field = field\n                        break\n            \n            if not source_address_field:\n                return {}\n            \n            # 获取目标表数据\n            target_collection = self.db_manager.get_collection('dwd_yljgxx')\n            target_sample = target_collection.find_one()\n            if not target_sample:\n                return {}\n            \n            # 查找目标表地址字段\n            target_address_field = None\n            for field, value in target_sample.items():\n                if isinstance(value, str) and len(value) > 5:\n                    if any(keyword in value for keyword in ['路', '街', '区', '市', '号', '弄', '村']):\n                        target_address_field = field\n                        break\n            \n            if not target_address_field:\n                return {}\n            \n            # 获取测试样本（减少数量以提高测试速度）\n            source_samples = list(source_collection.find(\n                {source_address_field: {'$exists': True, '$ne': '', '$ne': None}}\n            ).limit(30))  # 减少到30条\n            \n            target_samples = list(target_collection.find(\n                {target_address_field: {'$exists': True, '$ne': '', '$ne': None}}\n            ).limit(50))  # 减少到50条\n            \n            # 标准化地址字段名\n            for sample in source_samples:\n                sample['address'] = sample.get(source_address_field, '')\n            \n            for sample in target_samples:\n                sample['address'] = sample.get(target_address_field, '')\n            \n            return {\n                'source_samples': source_samples,\n                'target_samples': target_samples,\n                'source_field': source_address_field,\n                'target_field': target_address_field\n            }\n            \n        except Exception as e:\n            logger.error(f\"准备测试数据失败: {str(e)}\")\n            return {}\n    \n    def _test_threshold_configuration(self, config_name: str, thresholds: Dict, test_data: Dict) -> Dict:\n        \"\"\"测试特定阈值配置\"\"\"\n        start_time = time.time()\n        \n        # 创建临时的相似度计算器，使用新的阈值配置\n        temp_config = self.config_manager.get_matching_config().copy()\n        temp_config.update(thresholds)\n        \n        similarity_calculator = SimilarityCalculator(temp_config)\n        \n        # 临时修改层级验证阈值（通过monkey patch）\n        original_method = similarity_calculator._hierarchical_termination_verification\n        \n        def patched_verification(comp1, comp2):\n            # 使用新的阈值配置\n            hierarchy_levels = [\n                ('province', '省级', thresholds.get('province_threshold', 0.85)),\n                ('city', '市级', thresholds.get('city_threshold', 0.85)),\n                ('district', '区县级', thresholds.get('district_threshold', 0.80)),\n                ('town', '镇街级', thresholds.get('town_threshold', 0.75)),\n                ('community', '小区级', thresholds.get('community_threshold', 0.70)),\n                ('street', '路级', thresholds.get('street_threshold', 0.70)),\n                ('lane', '弄级', thresholds.get('lane_threshold', 0.65)),\n                ('number', '门牌级', thresholds.get('number_threshold', 0.60))\n            ]\n            \n            for level_idx, (level, level_name, threshold) in enumerate(hierarchy_levels):\n                val1 = comp1.get(level, '').strip()\n                val2 = comp2.get(level, '').strip()\n                \n                if val1 and val2:\n                    level_similarity = similarity_calculator._calculate_component_similarity(val1, val2, level)\n                    if level_similarity < threshold:\n                        return 0.0\n                elif not val1 or not val2:\n                    continue\n            \n            return 1.0  # 通过层级验证\n        \n        # 应用补丁\n        similarity_calculator._hierarchical_termination_verification = patched_verification\n        \n        # 执行测试\n        results = {\n            'config_name': config_name,\n            'thresholds': thresholds,\n            'total_tests': len(test_data['source_samples']),\n            'matches_found': 0,\n            'high_precision_matches': 0,\n            'medium_precision_matches': 0,\n            'low_precision_matches': 0,\n            'no_matches': 0,\n            'match_details': [],\n            'performance': {\n                'total_time': 0,\n                'avg_time_per_test': 0,\n                'tests_per_second': 0\n            }\n        }\n        \n        for i, source_record in enumerate(test_data['source_samples']):\n            source_address = source_record.get('address', '')\n            if not source_address:\n                continue\n            \n            # 标准化源地址\n            normalized_source = normalize_address_for_matching(source_address)\n            \n            # 寻找最佳匹配\n            best_similarity = 0.0\n            best_match = None\n            \n            for target_record in test_data['target_samples']:\n                target_address = target_record.get('address', '')\n                if not target_address:\n                    continue\n                \n                # 标准化目标地址\n                normalized_target = normalize_address_for_matching(target_address)\n                \n                # 计算相似度\n                similarity = similarity_calculator.calculate_address_similarity(\n                    normalized_source, normalized_target\n                )\n                \n                if similarity > best_similarity:\n                    best_similarity = similarity\n                    best_match = {\n                        'target_address': target_address,\n                        'normalized_target': normalized_target,\n                        'similarity': similarity\n                    }\n            \n            # 分类匹配结果\n            match_category = self._categorize_match_with_thresholds(best_similarity, thresholds)\n            \n            if best_similarity >= thresholds.get('similarity_threshold', 0.7):\n                results['matches_found'] += 1\n                \n                if match_category == 'high_precision':\n                    results['high_precision_matches'] += 1\n                elif match_category == 'medium_precision':\n                    results['medium_precision_matches'] += 1\n                else:\n                    results['low_precision_matches'] += 1\n            else:\n                results['no_matches'] += 1\n            \n            # 记录详细结果（只保留前5条）\n            if len(results['match_details']) < 5:\n                results['match_details'].append({\n                    'test_id': i + 1,\n                    'source_address': source_address,\n                    'normalized_source': normalized_source,\n                    'best_match_address': best_match['target_address'] if best_match else '',\n                    'best_match_normalized': best_match['normalized_target'] if best_match else '',\n                    'similarity_score': best_similarity,\n                    'match_category': match_category,\n                    'has_match': best_similarity >= thresholds.get('similarity_threshold', 0.7)\n                })\n        \n        # 计算性能指标\n        total_time = time.time() - start_time\n        results['performance']['total_time'] = total_time\n        results['performance']['avg_time_per_test'] = total_time / len(test_data['source_samples']) if test_data['source_samples'] else 0\n        results['performance']['tests_per_second'] = len(test_data['source_samples']) / total_time if total_time > 0 else 0\n        \n        return results\n    \n    def _categorize_match_with_thresholds(self, similarity: float, thresholds: Dict) -> str:\n        \"\"\"使用指定阈值分类匹配结果\"\"\"\n        if similarity >= thresholds.get('high_precision_threshold', 0.9):\n            return 'high_precision'\n        elif similarity >= thresholds.get('medium_precision_threshold', 0.7):\n            return 'medium_precision'\n        elif similarity >= thresholds.get('similarity_threshold', 0.7):\n            return 'low_precision'\n        else:\n            return 'no_match'\n    \n    def _calculate_improvement(self, baseline: Dict, candidate: Dict) -> Dict:\n        \"\"\"计算相对于基准的改进\"\"\"\n        baseline_match_rate = baseline['matches_found'] / baseline['total_tests'] if baseline['total_tests'] > 0 else 0\n        candidate_match_rate = candidate['matches_found'] / candidate['total_tests'] if candidate['total_tests'] > 0 else 0\n        \n        baseline_high_rate = baseline['high_precision_matches'] / baseline['total_tests'] if baseline['total_tests'] > 0 else 0\n        candidate_high_rate = candidate['high_precision_matches'] / candidate['total_tests'] if candidate['total_tests'] > 0 else 0\n        \n        return {\n            'match_rate_improvement': candidate_match_rate - baseline_match_rate,\n            'high_precision_improvement': candidate_high_rate - baseline_high_rate,\n            'speed_improvement': candidate['performance']['tests_per_second'] - baseline['performance']['tests_per_second'],\n            'match_rate_change_percent': ((candidate_match_rate - baseline_match_rate) / baseline_match_rate * 100) if baseline_match_rate > 0 else 0,\n            'high_precision_change_percent': ((candidate_high_rate - baseline_high_rate) / baseline_high_rate * 100) if baseline_high_rate > 0 else float('inf') if candidate_high_rate > 0 else 0\n        }\n    \n    def _select_best_configuration(self, results: List[Dict]) -> Dict:\n        \"\"\"选择最佳配置\"\"\"\n        if not results:\n            return None\n        \n        # 综合评分：匹配率 * 0.6 + 高精度率 * 0.4\n        best_config = None\n        best_score = -1\n        \n        for result in results:\n            match_rate = result['matches_found'] / result['total_tests'] if result['total_tests'] > 0 else 0\n            high_precision_rate = result['high_precision_matches'] / result['total_tests'] if result['total_tests'] > 0 else 0\n            \n            composite_score = match_rate * 0.6 + high_precision_rate * 0.4\n            \n            if composite_score > best_score:\n                best_score = composite_score\n                best_config = result\n        \n        if best_config:\n            best_config['composite_score'] = best_score\n        \n        return best_config\n    \n    def _generate_optimization_recommendations(self, baseline: Dict, results: List[Dict]) -> List[str]:\n        \"\"\"生成优化建议\"\"\"\n        recommendations = []\n        \n        # 分析最佳改进\n        best_match_improvement = max(results, key=lambda x: x['improvement']['match_rate_improvement'])\n        best_precision_improvement = max(results, key=lambda x: x['improvement']['high_precision_improvement'])\n        \n        if best_match_improvement['improvement']['match_rate_improvement'] > 0:\n            recommendations.append(\n                f\"采用'{best_match_improvement['config_name']}'可将匹配率提升 \"\n                f\"{best_match_improvement['improvement']['match_rate_change_percent']:.1f}%\"\n            )\n        \n        if best_precision_improvement['improvement']['high_precision_improvement'] > 0:\n            recommendations.append(\n                f\"采用'{best_precision_improvement['config_name']}'可提升高精度匹配率\"\n            )\n        \n        # 分析阈值调整建议\n        baseline_match_rate = baseline['matches_found'] / baseline['total_tests'] if baseline['total_tests'] > 0 else 0\n        if baseline_match_rate < 0.3:\n            recommendations.append(\"当前匹配率过低，建议采用'宽松配置'大幅降低区县级阈值\")\n        elif baseline_match_rate < 0.5:\n            recommendations.append(\"匹配率偏低，建议采用'平衡配置'适度调整阈值\")\n        else:\n            recommendations.append(\"匹配率尚可，建议采用'渐进配置'进行微调\")\n        \n        return recommendations\n\ndef main():\n    \"\"\"主函数\"\"\"\n    print(\"🔧 地址匹配阈值优化测试\")\n    print(\"=\" * 50)\n    \n    try:\n        optimizer = ThresholdOptimizer()\n        results = optimizer.run_threshold_optimization()\n        \n        if 'error' in results:\n            print(f\"❌ 优化测试失败: {results['error']}\")\n            return\n        \n        # 显示基准测试结果\n        baseline = results['baseline_performance']\n        print(f\"\\n📊 基准测试结果（当前配置）:\")\n        print(f\"  总测试数: {baseline['total_tests']}\")\n        print(f\"  找到匹配: {baseline['matches_found']} ({baseline['matches_found']/baseline['total_tests']*100:.1f}%)\")\n        print(f\"  高精度匹配: {baseline['high_precision_matches']} ({baseline['high_precision_matches']/baseline['total_tests']*100:.1f}%)\")\n        print(f\"  处理速度: {baseline['performance']['tests_per_second']:.1f} 条/秒\")\n        \n        # 显示优化结果\n        print(f\"\\n🧪 优化配置测试结果:\")\n        for result in results['optimization_results']:\n            match_rate = result['matches_found'] / result['total_tests'] * 100\n            high_precision_rate = result['high_precision_matches'] / result['total_tests'] * 100\n            improvement = result['improvement']\n            \n            print(f\"\\n  {result['config_name']}:\")\n            print(f\"    匹配率: {match_rate:.1f}% (改进: {improvement['match_rate_change_percent']:+.1f}%)\")\n            print(f\"    高精度率: {high_precision_rate:.1f}% (改进: {improvement['high_precision_change_percent']:+.1f}%)\")\n            print(f\"    处理速度: {result['performance']['tests_per_second']:.1f} 条/秒\")\n        \n        # 显示最佳配置\n        best_config = results['best_configuration']\n        if best_config:\n            print(f\"\\n🏆 推荐最佳配置: {best_config['config_name']}\")\n            print(f\"  综合评分: {best_config['composite_score']:.3f}\")\n            print(f\"  匹配率: {best_config['matches_found']/best_config['total_tests']*100:.1f}%\")\n            print(f\"  高精度率: {best_config['high_precision_matches']/best_config['total_tests']*100:.1f}%\")\n        \n        # 显示优化建议\n        print(f\"\\n💡 优化建议:\")\n        for i, rec in enumerate(results['recommendations'], 1):\n            print(f\"  {i}. {rec}\")\n        \n        # 保存详细报告\n        report_file = f\"threshold_optimization_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json\"\n        with open(report_file, 'w', encoding='utf-8') as f:\n            json.dump(results, f, ensure_ascii=False, indent=2)\n        \n        print(f\"\\n📄 详细报告已保存至: {report_file}\")\n        \n    except Exception as e:\n        print(f\"❌ 优化测试执行失败: {str(e)}\")\n        import traceback\n        traceback.print_exc()\n\nif __name__ == \"__main__\":\n    main()
