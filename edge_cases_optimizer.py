#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
边界情况处理优化器
处理特殊地址格式，优化非标准地址表述识别，增强容错机制
"""

import os
import sys
import logging
import json
import re
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

class EdgeCasesOptimizer:
    """边界情况处理优化器"""
    
    def __init__(self):
        """初始化优化器"""
        self.config_manager = ConfigManager()
        self.db_manager = DatabaseManager(config=self.config_manager.get_database_config())
        self.similarity_calculator = SimilarityCalculator(self.config_manager.get_matching_config())
        
        # 定义边界情况模式
        self.edge_case_patterns = {
            'highway': {
                'pattern': r'(S\d+|G\d+|高速|国道|省道|出口|公里)',
                'description': '高速公路/国道地址',
                'handler': self._handle_highway_address
            },
            'village': {
                'pattern': r'(村|组|队|号|叶家|石桥)',
                'description': '村组地址',
                'handler': self._handle_village_address
            },
            'complex_building': {
                'pattern': r'(\d+弄\d+号\d+室|\d+号\d+室|一般居民)',
                'description': '复杂建筑地址',
                'handler': self._handle_complex_building_address
            },
            'incomplete': {
                'pattern': r'^(上|上海|奉贤区)',
                'description': '不完整地址',
                'handler': self._handle_incomplete_address
            },
            'institution': {
                'pattern': r'(养老院|敬老院|护理院|医院|学校|政府|局)',
                'description': '机构地址',
                'handler': self._handle_institution_address
            }
        }
    
    def optimize_edge_cases(self):
        """优化边界情况处理"""
        print("🔧 边界情况处理优化")
        print("=" * 50)
        
        try:
            # 1. 准备测试数据
            test_data = self._prepare_edge_case_test_data()
            print(f"📊 准备了 {len(test_data['source_addresses'])} 条边界情况测试数据")
            
            # 2. 分析边界情况类型
            edge_case_analysis = self._analyze_edge_cases(test_data['source_addresses'])
            print(f"🔍 识别出 {len(edge_case_analysis)} 种边界情况类型")
            
            # 3. 测试当前处理效果
            baseline_results = self._test_current_handling(test_data)
            print(f"📈 当前处理效果：{baseline_results['success_rate']:.1f}% 成功率")
            
            # 4. 应用边界情况优化策略
            optimized_results = self._apply_edge_case_optimizations(test_data)
            print(f"🚀 优化后效果：{optimized_results['success_rate']:.1f}% 成功率")
            
            # 5. 生成优化报告
            optimization_report = {
                'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'edge_case_analysis': edge_case_analysis,
                'baseline_results': baseline_results,
                'optimized_results': optimized_results,
                'improvement': {
                    'success_rate_improvement': optimized_results['success_rate'] - baseline_results['success_rate'],
                    'processing_speed_improvement': optimized_results['processing_speed'] - baseline_results['processing_speed']
                },
                'recommendations': self._generate_edge_case_recommendations(edge_case_analysis, baseline_results, optimized_results)
            }
            
            # 保存报告
            report_filename = f"edge_cases_optimization_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(optimization_report, f, ensure_ascii=False, indent=2)
            
            print(f"📋 优化报告已保存到: {report_filename}")
            
            # 显示关键结果
            print("\n🎯 关键优化结果:")
            for category, info in edge_case_analysis.items():
                print(f"  {category}: {info['count']} 条 - {info['description']}")
            
            print(f"\n📊 整体改进:")
            print(f"  成功率提升: {optimization_report['improvement']['success_rate_improvement']:.1f}%")
            print(f"  处理速度提升: {optimization_report['improvement']['processing_speed_improvement']:.1f} 条/秒")
            
            return optimization_report
            
        except Exception as e:
            logger.error(f"边界情况优化失败: {e}")
            return None
    
    def _prepare_edge_case_test_data(self) -> Dict:
        """准备边界情况测试数据"""
        try:
            # 获取源表数据
            source_collection = self.db_manager.get_collection('hztj_hzxx')
            source_sample = source_collection.find_one()
            
            # 使用正确的地址字段
            source_address_field = '起火地点'  # hztj_hzxx表的地址字段
            
            # 获取包含边界情况的地址样本
            edge_case_addresses = []
            for pattern_info in self.edge_case_patterns.values():
                regex_pattern = pattern_info['pattern']
                samples = list(source_collection.find(
                    {source_address_field: {'$regex': regex_pattern, '$options': 'i'}},
                    {source_address_field: 1}
                ).limit(5))
                
                for sample in samples:
                    if source_address_field in sample and sample[source_address_field]:
                        edge_case_addresses.append(sample[source_address_field])
            
            # 获取目标表数据
            target_collection = self.db_manager.get_collection('dwd_yljgxx')
            target_sample = target_collection.find_one()
            
            # 使用正确的地址字段
            target_address_field = 'ZCDZ'  # dwd_yljgxx表的注册地址字段
            
            target_addresses = list(target_collection.find(
                {target_address_field: {'$exists': True, '$ne': ''}},
                {target_address_field: 1}
            ).limit(100))
            
            target_address_list = [addr[target_address_field] for addr in target_addresses if target_address_field in addr]
            
            return {
                'source_addresses': edge_case_addresses[:20],  # 限制测试数量
                'target_addresses': target_address_list,
                'source_field': source_address_field,
                'target_field': target_address_field
            }
            
        except Exception as e:
            logger.error(f"准备边界情况测试数据失败: {e}")
            return {'source_addresses': [], 'target_addresses': []}
    
    def _analyze_edge_cases(self, addresses: List[str]) -> Dict:
        """分析边界情况类型"""
        analysis = {}
        
        for address in addresses:
            for case_type, pattern_info in self.edge_case_patterns.items():
                if re.search(pattern_info['pattern'], address, re.IGNORECASE):
                    if case_type not in analysis:
                        analysis[case_type] = {
                            'count': 0,
                            'description': pattern_info['description'],
                            'examples': []
                        }
                    analysis[case_type]['count'] += 1
                    if len(analysis[case_type]['examples']) < 3:
                        analysis[case_type]['examples'].append(address)
        
        return analysis
    
    def _test_current_handling(self, test_data: Dict) -> Dict:
        """测试当前边界情况处理效果"""
        start_time = time.time()
        successful_matches = 0
        total_tests = len(test_data['source_addresses'])
        
        match_details = []
        
        for source_addr in test_data['source_addresses']:
            best_match = None
            best_score = 0.0
            
            # 标准化源地址
            normalized_source = normalize_address_for_matching(source_addr)
            
            for target_addr in test_data['target_addresses'][:50]:  # 限制目标数量以提高速度
                normalized_target = normalize_address_for_matching(target_addr)
                
                # 计算相似度
                similarity = self.similarity_calculator.calculate_string_similarity(
                    normalized_source, normalized_target
                )
                
                if similarity > best_score:
                    best_score = similarity
                    best_match = target_addr
            
            has_match = best_score >= 0.6  # 使用较低的阈值
            if has_match:
                successful_matches += 1
            
            match_details.append({
                'source_address': source_addr,
                'best_match_address': best_match or '',
                'similarity_score': best_score,
                'has_match': has_match
            })
        
        processing_time = time.time() - start_time
        
        return {
            'total_tests': total_tests,
            'successful_matches': successful_matches,
            'success_rate': (successful_matches / total_tests * 100) if total_tests > 0 else 0,
            'processing_time': processing_time,
            'processing_speed': total_tests / processing_time if processing_time > 0 else 0,
            'match_details': match_details[:10]  # 只保存前10个详情
        }
    
    def _apply_edge_case_optimizations(self, test_data: Dict) -> Dict:
        """应用边界情况优化策略"""
        start_time = time.time()
        successful_matches = 0
        total_tests = len(test_data['source_addresses'])
        
        match_details = []
        
        for source_addr in test_data['source_addresses']:
            # 1. 识别边界情况类型
            edge_case_type = self._identify_edge_case_type(source_addr)
            
            # 2. 应用特定的处理策略
            processed_source = self._apply_edge_case_handler(source_addr, edge_case_type)
            
            best_match = None
            best_score = 0.0
            
            for target_addr in test_data['target_addresses'][:50]:
                # 3. 使用优化的相似度计算
                similarity = self._calculate_edge_case_similarity(
                    processed_source, target_addr, edge_case_type
                )
                
                if similarity > best_score:
                    best_score = similarity
                    best_match = target_addr
            
            # 4. 使用动态阈值
            dynamic_threshold = self._get_dynamic_threshold(edge_case_type)
            has_match = best_score >= dynamic_threshold
            
            if has_match:
                successful_matches += 1
            
            match_details.append({
                'source_address': source_addr,
                'edge_case_type': edge_case_type,
                'processed_source': processed_source,
                'best_match_address': best_match or '',
                'similarity_score': best_score,
                'dynamic_threshold': dynamic_threshold,
                'has_match': has_match
            })
        
        processing_time = time.time() - start_time
        
        return {
            'total_tests': total_tests,
            'successful_matches': successful_matches,
            'success_rate': (successful_matches / total_tests * 100) if total_tests > 0 else 0,
            'processing_time': processing_time,
            'processing_speed': total_tests / processing_time if processing_time > 0 else 0,
            'match_details': match_details[:10]
        }
    
    def _identify_edge_case_type(self, address: str) -> str:
        """识别边界情况类型"""
        for case_type, pattern_info in self.edge_case_patterns.items():
            if re.search(pattern_info['pattern'], address, re.IGNORECASE):
                return case_type
        return 'normal'
    
    def _apply_edge_case_handler(self, address: str, edge_case_type: str) -> str:
        """应用边界情况处理器"""
        if edge_case_type in self.edge_case_patterns:
            handler = self.edge_case_patterns[edge_case_type]['handler']
            return handler(address)
        return normalize_address_for_matching(address)
    
    def _handle_highway_address(self, address: str) -> str:
        """处理高速公路地址"""
        # 提取关键信息：高速编号、方向、出口、公里数
        processed = address
        processed = re.sub(r'(S\d+|G\d+)', r'高速\1', processed)
        processed = re.sub(r'(\d+)-(\d+)公里', r'\1公里', processed)
        return normalize_address_for_matching(processed)
    
    def _handle_village_address(self, address: str) -> str:
        """处理村组地址"""
        # 标准化村组表述
        processed = address
        processed = re.sub(r'(\d+)组', r'\1组', processed)
        processed = re.sub(r'(\d+)号', r'\1号', processed)
        return normalize_address_for_matching(processed)
    
    def _handle_complex_building_address(self, address: str) -> str:
        """处理复杂建筑地址"""
        # 简化复杂的门牌号表述
        processed = address
        processed = re.sub(r'（.*?）', '', processed)  # 移除括号内容
        processed = re.sub(r'\d+弄\d+号\d+室', lambda m: m.group(0).split('室')[0], processed)
        return normalize_address_for_matching(processed)
    
    def _handle_incomplete_address(self, address: str) -> str:
        """处理不完整地址"""
        # 补充完整的行政区划信息
        processed = address
        if processed.startswith('上') and not processed.startswith('上海'):
            processed = '上海市' + processed[1:]
        return normalize_address_for_matching(processed)
    
    def _handle_institution_address(self, address: str) -> str:
        """处理机构地址"""
        # 保留机构关键词，增强匹配权重
        return normalize_address_for_matching(address)
    
    def _calculate_edge_case_similarity(self, source: str, target: str, edge_case_type: str) -> float:
        """计算边界情况的相似度"""
        # 标准化目标地址
        normalized_target = normalize_address_for_matching(target)
        
        # 基础相似度
        base_similarity = self.similarity_calculator.calculate_string_similarity(source, normalized_target)
        
        # 根据边界情况类型调整相似度
        if edge_case_type == 'highway':
            # 高速公路地址降低地理位置要求
            if any(keyword in target for keyword in ['高速', '国道', '省道']):
                base_similarity += 0.2
        elif edge_case_type == 'village':
            # 村组地址增强门牌号匹配权重
            if any(keyword in target for keyword in ['村', '组', '号']):
                base_similarity += 0.15
        elif edge_case_type == 'institution':
            # 机构地址增强机构名称匹配权重
            if any(keyword in target for keyword in ['院', '所', '局', '中心']):
                base_similarity += 0.1
        
        return min(base_similarity, 1.0)  # 确保不超过1.0
    
    def _get_dynamic_threshold(self, edge_case_type: str) -> float:
        """获取动态阈值"""
        thresholds = {
            'highway': 0.4,      # 高速公路地址使用较低阈值
            'village': 0.5,      # 村组地址使用中等阈值
            'complex_building': 0.55,  # 复杂建筑地址
            'incomplete': 0.45,  # 不完整地址使用较低阈值
            'institution': 0.6,  # 机构地址使用标准阈值
            'normal': 0.6        # 普通地址使用标准阈值
        }
        return thresholds.get(edge_case_type, 0.6)
    
    def _generate_edge_case_recommendations(self, analysis: Dict, baseline: Dict, optimized: Dict) -> List[str]:
        """生成边界情况优化建议"""
        recommendations = []
        
        improvement = optimized['success_rate'] - baseline['success_rate']
        
        if improvement > 0:
            recommendations.append(f"🎉 边界情况处理优化成功，成功率提升 {improvement:.1f}%")
        else:
            recommendations.append("⚠️ 边界情况处理需要进一步优化")
        
        # 针对不同边界情况类型的建议
        for case_type, info in analysis.items():
            if info['count'] > 0:
                recommendations.append(f"📋 {info['description']}({info['count']}条)需要专门的处理策略")
        
        recommendations.extend([
            "🔧 技术优化建议：",
            "  1. 实现更智能的地址类型识别算法",
            "  2. 为每种边界情况设计专门的相似度计算方法",
            "  3. 建立动态阈值调整机制",
            "  4. 增强地址标准化预处理能力",
            "  5. 建立边界情况处理的反馈学习机制"
        ])
        
        return recommendations

def main():
    """主函数"""
    optimizer = EdgeCasesOptimizer()
    result = optimizer.optimize_edge_cases()
    
    if result:
        print("\n✅ 边界情况处理优化完成")
    else:
        print("\n❌ 边界情况处理优化失败")

if __name__ == "__main__":
    main()
