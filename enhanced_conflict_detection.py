#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
增强的冲突检测系统
针对用户反馈的问题进行深度优化
"""

import re
import logging
from typing import Dict, List, Tuple, Set
from fuzzywuzzy import fuzz
import jieba

logger = logging.getLogger(__name__)

class EnhancedConflictDetector:
    """增强的冲突检测器"""
    
    def __init__(self):
        self._init_core_name_conflicts()
        self._init_famous_brands()
        self._init_similar_but_different_patterns()
        
    def _init_core_name_conflicts(self):
        """初始化核心名称冲突检测"""
        # 相似但不同的核心词汇对
        self.core_name_conflicts = [
            # 食品行业
            ('为民', '惠民'),
            ('为民', '利民'),
            ('惠民', '利民'),
            ('民生', '民康'),
            ('民康', '民安'),
            
            # 科技行业 - 著名品牌冲突
            ('华为', '华美'),
            ('华为', '华兴'),
            ('华为', '华泰'),
            ('华美', '华兴'),
            ('腾讯', '腾达'),
            ('腾讯', '腾飞'),
            ('阿里', '阿拉'),
            ('百度', '百达'),
            
            # 金融行业
            ('招商', '招行'),  # 这个可能是同义词，需要特殊处理
            ('工商', '农商'),
            ('建设', '建行'),
            
            # 制造业
            ('精工', '精密'),
            ('精工', '精华'),
            ('机械', '机电'),
            
            # 服务业
            ('物流', '物业'),
            ('贸易', '商贸'),
            ('咨询', '资讯'),
        ]
    
    def _init_famous_brands(self):
        """初始化知名品牌列表"""
        self.famous_brands = {
            # 科技公司
            '华为', '腾讯', '阿里巴巴', '百度', '字节跳动', '小米', 'OPPO', 'VIVO',
            '联想', '海尔', '美的', '格力', '比亚迪', '大疆',
            
            # 金融机构
            '工商银行', '建设银行', '农业银行', '中国银行', '交通银行',
            '招商银行', '浦发银行', '民生银行', '兴业银行',
            
            # 通信运营商
            '中国移动', '中国联通', '中国电信',
            
            # 互联网公司
            '京东', '拼多多', '美团', '滴滴', '网易', '搜狐', '新浪',
        }
    
    def _init_similar_but_different_patterns(self):
        """初始化相似但不同的模式"""
        self.similar_patterns = [
            # 一字之差的模式
            (r'(.+)为民(.+)', r'(.+)惠民(.+)'),
            (r'(.+)华为(.+)', r'(.+)华美(.+)'),
            (r'(.+)精工(.+)', r'(.+)精密(.+)'),
            (r'(.+)物流(.+)', r'(.+)物业(.+)'),
            
            # 同音不同字
            (r'(.+)胜利(.+)', r'(.+)盛利(.+)'),
            (r'(.+)宏达(.+)', r'(.+)弘达(.+)'),
            (r'(.+)创新(.+)', r'(.+)创兴(.+)'),
        ]
    
    def detect_core_name_conflict(self, name1: str, name2: str) -> Tuple[float, str]:
        """
        检测核心名称冲突
        
        Returns:
            Tuple[float, str]: (冲突惩罚分数, 冲突原因)
        """
        # 1. 检查预定义的冲突词汇对
        for word1, word2 in self.core_name_conflicts:
            if (word1 in name1 and word2 in name2) or (word2 in name1 and word1 in name2):
                return 0.95, f"核心名称冲突: {word1} vs {word2}"
        
        # 2. 检查知名品牌冲突
        brand_conflict = self._detect_brand_conflict(name1, name2)
        if brand_conflict[0] > 0:
            return brand_conflict
        
        # 3. 检查相似模式冲突
        pattern_conflict = self._detect_pattern_conflict(name1, name2)
        if pattern_conflict[0] > 0:
            return pattern_conflict
        
        # 4. 检查核心词汇的细微差异
        subtle_conflict = self._detect_subtle_differences(name1, name2)
        if subtle_conflict[0] > 0:
            return subtle_conflict
        
        return 0.0, ""
    
    def _detect_brand_conflict(self, name1: str, name2: str) -> Tuple[float, str]:
        """检测知名品牌冲突"""
        brands_in_name1 = [brand for brand in self.famous_brands if brand in name1]
        brands_in_name2 = [brand for brand in self.famous_brands if brand in name2]
        
        if brands_in_name1 and brands_in_name2:
            # 如果两个名称都包含知名品牌，但品牌不同
            if set(brands_in_name1) != set(brands_in_name2):
                return 0.98, f"知名品牌冲突: {brands_in_name1[0]} vs {brands_in_name2[0]}"
        
        return 0.0, ""
    
    def _detect_pattern_conflict(self, name1: str, name2: str) -> Tuple[float, str]:
        """检测相似模式冲突"""
        for pattern1, pattern2 in self.similar_patterns:
            if re.search(pattern1, name1) and re.search(pattern2, name2):
                return 0.9, f"相似模式冲突: {pattern1} vs {pattern2}"
            elif re.search(pattern2, name1) and re.search(pattern1, name2):
                return 0.9, f"相似模式冲突: {pattern2} vs {pattern1}"
        
        return 0.0, ""
    
    def _detect_subtle_differences(self, name1: str, name2: str) -> Tuple[float, str]:
        """检测细微差异"""
        # 提取核心词汇
        words1 = self._extract_core_words(name1)
        words2 = self._extract_core_words(name2)
        
        if not words1 or not words2:
            return 0.0, ""
        
        # 检查是否存在高相似度但不相等的核心词
        for word1 in words1:
            for word2 in words2:
                if len(word1) >= 2 and len(word2) >= 2:
                    similarity = fuzz.ratio(word1, word2) / 100.0
                    
                    # 相似度在70%-95%之间，可能是不同的词汇
                    if 0.7 <= similarity < 0.95:
                        # 进一步检查是否为明显的不同词汇
                        if self._is_obviously_different(word1, word2):
                            return 0.85, f"核心词汇细微差异: {word1} vs {word2} (相似度: {similarity:.2f})"
        
        return 0.0, ""
    
    def _extract_core_words(self, name: str) -> List[str]:
        """提取核心词汇"""
        # 移除常见的公司后缀
        suffixes = ['有限公司', '股份有限公司', '集团', '公司', '厂', '店', '中心', '部']
        cleaned_name = name
        for suffix in suffixes:
            cleaned_name = cleaned_name.replace(suffix, '')
        
        # 移除地区信息
        regions = ['北京', '上海', '广州', '深圳', '杭州', '南京', '武汉', '成都', '西安', '天津', '重庆']
        for region in regions:
            cleaned_name = cleaned_name.replace(region, '')
        
        # 分词并过滤
        words = jieba.lcut(cleaned_name)
        core_words = []
        
        # 过滤掉单字和常见词汇
        stop_words = {'市', '区', '县', '省', '自治区', '的', '和', '与', '及'}
        
        for word in words:
            if len(word) >= 2 and word not in stop_words:
                core_words.append(word)
        
        return core_words
    
    def _is_obviously_different(self, word1: str, word2: str) -> bool:
        """判断两个词汇是否明显不同"""
        # 检查是否在预定义的冲突列表中
        for conflict_word1, conflict_word2 in self.core_name_conflicts:
            if (word1 == conflict_word1 and word2 == conflict_word2) or \
               (word1 == conflict_word2 and word2 == conflict_word1):
                return True
        
        # 检查字符差异
        if len(word1) == len(word2):
            diff_count = sum(c1 != c2 for c1, c2 in zip(word1, word2))
            # 如果只有1-2个字符不同，可能是不同的词汇
            if 1 <= diff_count <= 2:
                return True
        
        return False

def test_enhanced_conflict_detection():
    """测试增强的冲突检测"""
    detector = EnhancedConflictDetector()
    
    test_cases = [
        ('上海为民食品厂', '上海惠民食品厂'),
        ('北京华为科技有限公司', '北京华美科技有限公司'),
        ('深圳腾讯计算机系统有限公司', '深圳腾达科技有限公司'),
        ('上海浦东发展银行', '上海浦发银行'),  # 这个应该是同义词
        ('中国工商银行', '中国农商银行'),
        ('精工机械有限公司', '精密机械有限公司'),
        ('物流运输公司', '物业管理公司'),
    ]
    
    print("增强冲突检测测试结果:")
    print("=" * 80)
    print(f"{'企业1':<30} {'企业2':<30} {'冲突分数':<10} {'冲突原因'}")
    print("=" * 80)
    
    for name1, name2 in test_cases:
        penalty, reason = detector.detect_core_name_conflict(name1, name2)
        print(f"{name1:<30} {name2:<30} {penalty:<10.3f} {reason}")
    
    print("=" * 80)

if __name__ == "__main__":
    test_enhanced_conflict_detection()
