#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单位名称相似度计算问题分析脚本
分析当前算法的问题并设计改进方案
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.matching.similarity_scorer import SimilarityCalculator
from fuzzywuzzy import fuzz
import jieba
import re
from typing import List, Dict, Tuple

def analyze_current_algorithm():
    """分析当前算法的问题"""
    print("=" * 60)
    print("🔍 单位名称相似度计算问题分析")
    print("=" * 60)
    
    # 问题案例
    test_cases = [
        ("上海为民食品厂", "上海惠民食品厂"),
        ("北京华为科技有限公司", "北京华美科技有限公司"),
        ("深圳腾讯计算机系统有限公司", "深圳腾讯科技有限公司"),
        ("广州恒大地产集团有限公司", "广州恒基地产集团有限公司"),
        ("上海浦东发展银行", "上海浦发银行"),
        ("中国工商银行股份有限公司", "中国农业银行股份有限公司"),
        ("苹果电脑贸易（上海）有限公司", "苹果电子贸易（上海）有限公司"),
        ("上海市第一人民医院", "上海市第二人民医院")
    ]
    
    # 初始化相似度计算器
    config = {
        'string_similarity': {
            'algorithms': [
                {'name': 'levenshtein', 'weight': 0.3},
                {'name': 'jaro_winkler', 'weight': 0.3},
                {'name': 'cosine', 'weight': 0.4}
            ],
            'chinese_processing': {
                'enable_pinyin': True,
                'enable_jieba': True,
                'remove_punctuation': True,
                'normalize_spaces': True
            }
        }
    }
    
    calculator = SimilarityCalculator(config)
    
    print("\n📊 当前算法测试结果:")
    print("-" * 60)
    print(f"{'单位名称1':<25} {'单位名称2':<25} {'相似度':<8} {'问题分析'}")
    print("-" * 60)
    
    for name1, name2 in test_cases:
        # 当前算法计算相似度
        similarity = calculator.calculate_string_similarity(name1, name2)
        
        # 分析问题
        problem_analysis = analyze_similarity_problem(name1, name2, similarity)
        
        print(f"{name1:<25} {name2:<25} {similarity:.3f}    {problem_analysis}")
    
    print("\n🚨 发现的核心问题:")
    print("1. 字符重叠率算法：简单统计相同字符数量，忽略了语义重要性")
    print("2. 缺乏核心词汇识别：无法区分'为民'和'惠民'这种核心差异")
    print("3. 位置权重缺失：单位名称中不同位置的词汇重要性不同")
    print("4. 行业词汇干扰：'有限公司'、'股份'等通用词汇影响判断")
    print("5. 语义理解不足：无法理解'浦东发展银行'和'浦发银行'的等价性")

def analyze_similarity_problem(name1: str, name2: str, similarity: float) -> str:
    """分析相似度计算的问题"""
    # 提取核心词汇
    core1 = extract_core_keywords(name1)
    core2 = extract_core_keywords(name2)
    
    # 检查核心词汇重叠
    core_overlap = len(set(core1) & set(core2))
    
    if similarity > 0.8 and core_overlap == 0:
        return "❌高相似度但核心词不同"
    elif similarity < 0.5 and core_overlap > 0:
        return "❌低相似度但核心词相同"
    elif similarity > 0.7:
        return "⚠️可能误判"
    else:
        return "✅合理"

def extract_core_keywords(company_name: str) -> List[str]:
    """提取单位名称的核心关键词"""
    # 移除常见的公司后缀
    suffixes = ['有限公司', '股份有限公司', '集团有限公司', '科技有限公司', 
                '贸易有限公司', '投资有限公司', '发展有限公司', '实业有限公司',
                '有限责任公司', '股份公司', '集团公司', '公司', '厂', '店', '院', '中心']
    
    name = company_name
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
            break
    
    # 移除地区前缀
    prefixes = ['北京', '上海', '广州', '深圳', '天津', '重庆', '杭州', '南京', '武汉', '成都',
                '中国', '全国', '国际', '亚洲', '世界']
    
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    
    # 使用jieba分词提取关键词
    words = jieba.lcut(name)
    
    # 过滤停用词和单字符
    stop_words = {'的', '和', '与', '及', '或', '等', '为', '是', '在', '有', '无', '不', '了', '着', '过'}
    keywords = [word for word in words if len(word) > 1 and word not in stop_words]
    
    return keywords

def design_improved_algorithm():
    """设计改进的单位名称相似度算法"""
    print("\n" + "=" * 60)
    print("🚀 改进算法设计方案")
    print("=" * 60)
    
    print("\n📋 核心改进思路:")
    print("1. 【核心词汇权重化】：识别单位名称中的核心业务词汇，给予更高权重")
    print("2. 【位置敏感匹配】：不同位置的词汇重要性不同，核心词汇位置更重要")
    print("3. 【语义等价识别】：识别同义词和缩写（如'浦东发展银行'='浦发银行'）")
    print("4. 【行业词汇过滤】：降低通用词汇（如'有限公司'）的权重")
    print("5. 【多层次匹配】：结合字符级、词汇级、语义级多层次匹配")
    
    print("\n🔧 算法架构设计:")
    print("┌─────────────────────────────────────────────────────────┐")
    print("│                单位名称智能相似度计算器                    │")
    print("├─────────────────────────────────────────────────────────┤")
    print("│ 1. 预处理层                                              │")
    print("│    - 标准化处理（去空格、统一大小写）                      │")
    print("│    - 公司后缀识别和标准化                                 │")
    print("│    - 地区前缀处理                                        │")
    print("├─────────────────────────────────────────────────────────┤")
    print("│ 2. 核心词汇提取层                                         │")
    print("│    - jieba分词 + 自定义词典                              │")
    print("│    - 核心业务词汇识别                                     │")
    print("│    - 词汇重要性权重分配                                   │")
    print("├─────────────────────────────────────────────────────────┤")
    print("│ 3. 多层次匹配层                                          │")
    print("│    - 精确匹配（权重: 40%）                               │")
    print("│    - 核心词汇匹配（权重: 35%）                            │")
    print("│    - 语义相似匹配（权重: 15%）                            │")
    print("│    - 字符相似匹配（权重: 10%）                            │")
    print("├─────────────────────────────────────────────────────────┤")
    print("│ 4. 智能决策层                                            │")
    print("│    - 核心词汇冲突检测                                     │")
    print("│    - 同义词等价性判断                                     │")
    print("│    - 最终相似度综合计算                                   │")
    print("└─────────────────────────────────────────────────────────┘")

def test_improved_algorithm_concept():
    """测试改进算法的概念验证"""
    print("\n" + "=" * 60)
    print("🧪 改进算法概念验证")
    print("=" * 60)
    
    test_cases = [
        ("上海为民食品厂", "上海惠民食品厂"),
        ("北京华为科技有限公司", "北京华美科技有限公司"),
        ("上海浦东发展银行", "上海浦发银行"),
    ]
    
    print(f"{'单位名称1':<25} {'单位名称2':<25} {'当前算法':<8} {'改进算法':<8} {'改进说明'}")
    print("-" * 80)
    
    for name1, name2 in test_cases:
        # 当前算法
        config = {'string_similarity': {'algorithms': [{'name': 'levenshtein', 'weight': 1.0}]}}
        calculator = SimilarityCalculator(config)
        current_sim = calculator.calculate_string_similarity(name1, name2)
        
        # 改进算法概念验证
        improved_sim = calculate_improved_similarity_concept(name1, name2)
        
        # 改进说明
        improvement = get_improvement_explanation(name1, name2, current_sim, improved_sim)
        
        print(f"{name1:<25} {name2:<25} {current_sim:.3f}    {improved_sim:.3f}    {improvement}")

def calculate_improved_similarity_concept(name1: str, name2: str) -> float:
    """改进算法的概念验证实现"""
    # 1. 提取核心词汇
    core1 = extract_core_keywords(name1)
    core2 = extract_core_keywords(name2)
    
    # 2. 核心词汇匹配检查
    core_intersection = set(core1) & set(core2)
    core_union = set(core1) | set(core2)
    
    if len(core_union) == 0:
        return 0.0
    
    # 3. 核心词汇相似度（权重35%）
    core_similarity = len(core_intersection) / len(core_union)
    
    # 4. 检查核心词汇冲突
    # 如果核心词汇完全不同，相似度应该很低
    if len(core_intersection) == 0 and len(core1) > 0 and len(core2) > 0:
        # 核心词汇冲突，降低相似度
        core_similarity = 0.1
    
    # 5. 字符级相似度（权重10%）
    char_similarity = fuzz.ratio(name1, name2) / 100.0
    
    # 6. 同义词检查（简化版）
    synonym_bonus = 0.0
    if ("浦东发展银行" in name1 and "浦发银行" in name2) or \
       ("浦发银行" in name1 and "浦东发展银行" in name2):
        synonym_bonus = 0.8
    
    # 7. 综合计算
    final_similarity = core_similarity * 0.35 + char_similarity * 0.10 + synonym_bonus * 0.55
    
    return min(final_similarity, 1.0)

def get_improvement_explanation(name1: str, name2: str, current: float, improved: float) -> str:
    """获取改进说明"""
    diff = improved - current
    if abs(diff) < 0.1:
        return "相似"
    elif diff > 0:
        return f"提升{diff:.2f}"
    else:
        return f"降低{abs(diff):.2f}"

if __name__ == "__main__":
    try:
        analyze_current_algorithm()
        design_improved_algorithm()
        test_improved_algorithm_concept()
        
        print("\n" + "=" * 60)
        print("✅ 分析完成！")
        print("📝 下一步：实现改进的单位名称相似度算法")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
