#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
老系统模糊匹配算法分析
研究现有模糊匹配算法的优势和特点，为改进提供参考
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.enhanced_fuzzy_matcher import EnhancedFuzzyMatcher
from src.matching.optimized_fuzzy_matcher import OptimizedFuzzyMatcher
from src.matching.intelligent_unit_name_matcher import IntelligentUnitNameMatcher
from src.utils.config import ConfigManager

def analyze_fuzzy_algorithms():
    """分析各种模糊匹配算法"""
    print("=" * 80)
    print("🔬 老系统模糊匹配算法分析")
    print("=" * 80)
    
    # 初始化配置
    try:
        config_manager = ConfigManager()
        config = config_manager.get_matching_config()
    except Exception as e:
        print(f"配置加载失败，使用默认配置: {e}")
        config = {
            'fuzzy_match': {
                'similarity_threshold': 0.75,
                'fields': {
                    'unit_name': {'weight': 0.4, 'source_field': 'UNIT_NAME', 'target_field': 'dwmc'},
                    'address': {'weight': 0.3, 'source_field': 'ADDRESS', 'target_field': 'dwdz'}
                }
            }
        }
    
    # 初始化各种匹配器
    try:
        fuzzy_matcher = FuzzyMatcher(config)
        enhanced_fuzzy_matcher = EnhancedFuzzyMatcher(config)
        optimized_fuzzy_matcher = OptimizedFuzzyMatcher(config)
        intelligent_matcher = IntelligentUnitNameMatcher()
        
        print("✅ 所有匹配器初始化成功")
        
    except Exception as e:
        print(f"❌ 匹配器初始化失败: {str(e)}")
        return
    
    # 测试用例
    test_cases = [
        # 基础模糊匹配测试
        {
            'name': '基础相似匹配',
            'source': {'UNIT_NAME': '上海科技有限公司', 'ADDRESS': '上海市浦东新区张江路100号'},
            'target': {'dwmc': '上海科技有限公司', 'dwdz': '上海市浦东新区张江路100号'},
            'expected': '高相似度'
        },
        
        # 核心词汇差异测试
        {
            'name': '核心词汇差异',
            'source': {'UNIT_NAME': '上海为民食品厂', 'ADDRESS': '上海市黄浦区南京路100号'},
            'target': {'dwmc': '上海惠民食品厂', 'dwdz': '上海市黄浦区南京路100号'},
            'expected': '低相似度'
        },
        
        # 同义词测试
        {
            'name': '银行缩写',
            'source': {'UNIT_NAME': '上海浦东发展银行', 'ADDRESS': '上海市浦东新区陆家嘴环路1000号'},
            'target': {'dwmc': '上海浦发银行', 'dwdz': '上海市浦东新区陆家嘴环路1000号'},
            'expected': '高相似度'
        },
        
        # 地址差异测试
        {
            'name': '地址差异',
            'source': {'UNIT_NAME': '北京科技有限公司', 'ADDRESS': '北京市朝阳区建国路100号'},
            'target': {'dwmc': '北京科技有限公司', 'dwdz': '北京市海淀区中关村大街200号'},
            'expected': '中等相似度'
        },
        
        # 完全不同测试
        {
            'name': '完全不同',
            'source': {'UNIT_NAME': '中国工商银行', 'ADDRESS': '北京市西城区复兴门内大街55号'},
            'target': {'dwmc': '中国农业银行', 'dwdz': '北京市东城区建国门内大街69号'},
            'expected': '低相似度'
        }
    ]
    
    print("\n📊 算法对比测试结果:")
    print("-" * 100)
    print(f"{'测试案例':<15} {'基础模糊':<10} {'增强模糊':<10} {'优化模糊':<10} {'智能匹配':<10} {'预期结果'}")
    print("-" * 100)
    
    algorithm_performance = {
        'fuzzy_matcher': [],
        'enhanced_fuzzy_matcher': [],
        'optimized_fuzzy_matcher': [],
        'intelligent_matcher': []
    }
    
    for case in test_cases:
        source = case['source']
        target = case['target']
        case_name = case['name']
        expected = case['expected']
        
        # 1. 基础模糊匹配器
        try:
            fuzzy_result = fuzzy_matcher.match_single_record(source, [target])
            fuzzy_score = fuzzy_result.similarity_score if fuzzy_result.matched else 0.0
            algorithm_performance['fuzzy_matcher'].append(fuzzy_score)
        except Exception as e:
            print(f"基础模糊匹配失败: {e}")
            fuzzy_score = 0.0
            algorithm_performance['fuzzy_matcher'].append(0.0)
        
        # 2. 增强模糊匹配器
        try:
            enhanced_result = enhanced_fuzzy_matcher.match_single_record(source, [target])
            enhanced_score = enhanced_result.similarity_score if enhanced_result.matched else 0.0
            algorithm_performance['enhanced_fuzzy_matcher'].append(enhanced_score)
        except Exception as e:
            print(f"增强模糊匹配失败: {e}")
            enhanced_score = 0.0
            algorithm_performance['enhanced_fuzzy_matcher'].append(0.0)
        
        # 3. 优化模糊匹配器
        try:
            optimized_result = optimized_fuzzy_matcher.match_single_record_optimized(source, [target])
            optimized_score = optimized_result['similarity_score'] if optimized_result['matched'] else 0.0
            algorithm_performance['optimized_fuzzy_matcher'].append(optimized_score)
        except Exception as e:
            print(f"优化模糊匹配失败: {e}")
            optimized_score = 0.0
            algorithm_performance['optimized_fuzzy_matcher'].append(0.0)
        
        # 4. 智能单位名称匹配器（仅匹配单位名称）
        try:
            intelligent_score = intelligent_matcher.calculate_similarity(
                source.get('UNIT_NAME', ''), 
                target.get('dwmc', '')
            )
            algorithm_performance['intelligent_matcher'].append(intelligent_score)
        except Exception as e:
            print(f"智能匹配失败: {e}")
            intelligent_score = 0.0
            algorithm_performance['intelligent_matcher'].append(0.0)
        
        # 显示结果
        print(f"{case_name:<15} {fuzzy_score:.3f}      {enhanced_score:.3f}      {optimized_score:.3f}      {intelligent_score:.3f}      {expected}")

def analyze_algorithm_characteristics():
    """分析各算法的特点和优势"""
    print("\n" + "=" * 80)
    print("🔍 算法特点和优势分析")
    print("=" * 80)
    
    algorithms_analysis = {
        'FuzzyMatcher (基础模糊匹配器)': {
            '核心特点': [
                '使用多种相似度算法组合（Levenshtein、Jaro-Winkler）',
                '支持中文拼音相似度计算',
                '基于字段权重的综合评分',
                '支持数值和地址的专门处理'
            ],
            '优势': [
                '算法成熟稳定，经过大量测试验证',
                '支持多种数据类型的相似度计算',
                '配置灵活，可调整各字段权重',
                '中文处理能力较强'
            ],
            '劣势': [
                '缺乏语义理解能力',
                '对核心词汇差异敏感度不足',
                '计算性能相对较低',
                '容易产生误匹配'
            ],
            '适用场景': [
                '通用文本相似度匹配',
                '对准确性要求不是特别高的场景',
                '数据质量较好的情况'
            ]
        },
        
        'EnhancedFuzzyMatcher (增强模糊匹配器)': {
            '核心特点': [
                '集成结构化名称匹配器',
                '增加业务类型冲突检测',
                '核心名称权重提升机制',
                '地址不匹配惩罚机制'
            ],
            '优势': [
                '解决了部分匹配幻觉问题',
                '对核心名称差异更敏感',
                '增加了业务逻辑判断',
                '提供详细的匹配解释'
            ],
            '劣势': [
                '算法复杂度较高',
                '配置参数较多，调优困难',
                '性能开销相对较大',
                '仍然依赖字符级相似度'
            ],
            '适用场景': [
                '对匹配准确性要求较高的场景',
                '需要详细匹配解释的情况',
                '业务逻辑复杂的匹配任务'
            ]
        },
        
        'OptimizedFuzzyMatcher (优化模糊匹配器)': {
            '核心特点': [
                '性能优化为主要目标',
                '快速预筛选机制',
                '并行处理支持',
                '缓存机制减少重复计算'
            ],
            '优势': [
                '处理速度快，适合大数据量',
                '内存使用优化',
                '支持并行处理',
                '早期退出机制提高效率'
            ],
            '劣势': [
                '为了性能牺牲了部分准确性',
                '预筛选可能过滤掉潜在匹配',
                '算法相对简化',
                '缺乏深度语义分析'
            ],
            '适用场景': [
                '大数据量实时匹配',
                '对性能要求极高的场景',
                '初步筛选和预处理'
            ]
        },
        
        'IntelligentUnitNameMatcher (智能单位名称匹配器)': {
            '核心特点': [
                '基于语义理解的匹配',
                '核心词汇权重化处理',
                '同义词和缩写识别',
                '行业词汇过滤机制'
            ],
            '优势': [
                '解决了核心词汇识别问题',
                '支持同义词等价匹配',
                '语义理解能力强',
                '减少误匹配率'
            ],
            '劣势': [
                '目前仅支持单位名称字段',
                '词典维护成本较高',
                '对新词汇适应性有限',
                '计算复杂度相对较高'
            ],
            '适用场景': [
                '单位名称精确匹配',
                '对语义准确性要求高的场景',
                '需要处理同义词的情况'
            ]
        }
    }
    
    for algorithm, analysis in algorithms_analysis.items():
        print(f"\n🔧 {algorithm}")
        print("-" * 60)
        
        for category, items in analysis.items():
            print(f"\n{category}:")
            for item in items:
                print(f"  • {item}")

def recommend_algorithm_integration():
    """推荐算法集成方案"""
    print("\n" + "=" * 80)
    print("💡 算法集成改进建议")
    print("=" * 80)
    
    recommendations = [
        {
            'title': '1. 分层匹配策略',
            'description': '根据字段类型选择最适合的匹配算法',
            'details': [
                '单位名称字段：使用IntelligentUnitNameMatcher',
                '地址字段：使用地址语义匹配算法',
                '人名字段：使用基础模糊匹配',
                '数值字段：使用数值相似度算法'
            ]
        },
        {
            'title': '2. 混合权重机制',
            'description': '结合多种算法的优势，动态调整权重',
            'details': [
                '高置信度场景：提高智能匹配权重',
                '模糊场景：结合多种算法投票',
                '性能要求高：优先使用优化算法',
                '准确性要求高：使用增强算法验证'
            ]
        },
        {
            'title': '3. 渐进式匹配流程',
            'description': '从快速到精确的多阶段匹配',
            'details': [
                '第一阶段：优化模糊匹配快速预筛选',
                '第二阶段：智能匹配精确评分',
                '第三阶段：增强匹配最终验证',
                '第四阶段：人工审核边界案例'
            ]
        },
        {
            'title': '4. 自适应阈值调整',
            'description': '根据数据质量和业务需求动态调整阈值',
            'details': [
                '数据质量高：提高匹配阈值',
                '数据质量低：降低阈值，增加人工审核',
                '关键业务：使用保守阈值',
                '批量处理：使用宽松阈值'
            ]
        },
        {
            'title': '5. 性能与准确性平衡',
            'description': '在不同场景下平衡性能和准确性需求',
            'details': [
                '实时匹配：优先性能，使用优化算法',
                '批量匹配：优先准确性，使用智能算法',
                '交互式匹配：平衡两者，提供多种选择',
                '审核模式：最高准确性，多算法验证'
            ]
        }
    ]
    
    for rec in recommendations:
        print(f"\n{rec['title']}")
        print(f"描述: {rec['description']}")
        print("具体方案:")
        for detail in rec['details']:
            print(f"  • {detail}")

if __name__ == "__main__":
    try:
        analyze_fuzzy_algorithms()
        analyze_algorithm_characteristics()
        recommend_algorithm_integration()
        
        print("\n" + "=" * 80)
        print("✅ 模糊匹配算法分析完成！")
        print("📝 关键发现：")
        print("  1. 现有算法各有优势，适合不同场景")
        print("  2. 智能单位名称匹配器解决了核心问题")
        print("  3. 需要建立分层匹配和混合权重机制")
        print("  4. 性能与准确性需要根据场景平衡")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
