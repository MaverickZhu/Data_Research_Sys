#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能单位名称相似度匹配器
解决传统字符重叠算法的核心问题，实现基于语义理解的单位名称匹配

核心改进：
1. 核心词汇权重化：识别单位名称中的核心业务词汇，给予更高权重
2. 位置敏感匹配：不同位置的词汇重要性不同
3. 语义等价识别：识别同义词和缩写
4. 行业词汇过滤：降低通用词汇的权重
5. 多层次匹配：结合字符级、词汇级、语义级匹配
"""

import jieba
import re
import logging
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass
from fuzzywuzzy import fuzz
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class CompanyNameComponents:
    """单位名称组件"""
    original: str           # 原始名称
    region: str            # 地区前缀
    core_business: List[str]  # 核心业务词汇
    industry_type: List[str]  # 行业类型词汇
    legal_suffix: str      # 法律后缀
    normalized: str        # 标准化名称


class IntelligentUnitNameMatcher:
    """智能单位名称相似度匹配器"""
    
    def __init__(self):
        """初始化匹配器"""
        self._init_dictionaries()
        self._init_synonym_dict()
        logger.info("智能单位名称匹配器初始化完成")
    
    def _init_dictionaries(self):
        """初始化词典"""
        # 地区前缀词典
        self.region_prefixes = {
            '北京', '上海', '广州', '深圳', '天津', '重庆', '杭州', '南京', '武汉', '成都',
            '西安', '沈阳', '大连', '青岛', '厦门', '苏州', '无锡', '宁波', '温州', '佛山',
            '东莞', '中山', '珠海', '惠州', '江门', '湛江', '茂名', '肇庆', '梅州', '汕头',
            '中国', '全国', '国际', '亚洲', '世界', '华东', '华南', '华北', '华中', '西南',
            '东北', '西北', '长三角', '珠三角', '京津冀'
        }
        
        # 法律后缀词典（按重要性排序）
        self.legal_suffixes = [
            '股份有限公司', '有限责任公司', '集团有限公司', '科技有限公司',
            '贸易有限公司', '投资有限公司', '发展有限公司', '实业有限公司',
            '建设有限公司', '工程有限公司', '咨询有限公司', '服务有限公司',
            '有限公司', '股份公司', '集团公司', '集团', '公司', '厂', '店', '院', 
            '中心', '所', '部', '局', '委', '会', '社', '团', '协会', '基金会'
        ]
        
        # 行业通用词汇（权重较低）
        self.industry_common_words = {
            '科技', '技术', '信息', '网络', '软件', '硬件', '电子', '数码', '智能',
            '贸易', '商贸', '进出口', '国际贸易', '商务', '营销', '销售',
            '投资', '资本', '基金', '证券', '保险', '银行', '金融', '财务',
            '建设', '建筑', '工程', '装饰', '设计', '规划', '咨询',
            '制造', '生产', '加工', '机械', '设备', '工业', '实业',
            '服务', '管理', '咨询', '顾问', '代理', '中介', '物流',
            '医疗', '健康', '生物', '制药', '化工', '材料', '能源',
            '教育', '培训', '文化', '传媒', '广告', '出版', '娱乐',
            '房地产', '物业', '酒店', '餐饮', '旅游', '运输', '航空'
        }
        
        # 停用词
        self.stop_words = {
            '的', '和', '与', '及', '或', '等', '为', '是', '在', '有', '无', '不',
            '了', '着', '过', '将', '被', '把', '从', '向', '到', '于', '对', '按',
            '根据', '通过', '由于', '因为', '所以', '但是', '然而', '虽然', '尽管'
        }
    
    def _init_synonym_dict(self):
        """初始化同义词词典"""
        self.synonym_groups = [
            # 银行类同义词
            {'浦东发展银行', '浦发银行', '上海浦发银行'},
            {'招商银行', '招行'},
            {'中国建设银行', '建设银行', '建行'},
            {'中国工商银行', '工商银行', '工行'},
            {'中国农业银行', '农业银行', '农行'},
            {'中国银行', '中行'},
            {'交通银行', '交行'},
            
            # 科技公司同义词
            {'腾讯', '腾讯科技', '腾讯计算机系统'},
            {'阿里巴巴', '阿里', '阿里巴巴集团'},
            {'百度', '百度在线', '百度网络'},
            {'华为', '华为技术', '华为科技'},
            {'小米', '小米科技', '小米通讯'},
            
            # 通用缩写
            {'有限公司', '有限', '公司'},
            {'股份有限公司', '股份公司', '股份'},
            {'集团有限公司', '集团公司', '集团'},
            {'科技有限公司', '科技公司', '科技'},
        ]
        
        # 构建同义词映射
        self.synonym_map = {}
        for group in self.synonym_groups:
            # 选择最短的作为标准形式
            standard = min(group, key=len)
            for word in group:
                self.synonym_map[word] = standard
    
    def calculate_similarity(self, name1: str, name2: str) -> float:
        """
        计算两个单位名称的智能相似度
        
        Args:
            name1: 单位名称1
            name2: 单位名称2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not name1 or not name2:
            return 0.0
        
        if name1 == name2:
            return 1.0
        
        # 1. 解析单位名称组件
        components1 = self._parse_company_name(name1)
        components2 = self._parse_company_name(name2)
        
        # 2. 多层次相似度计算
        exact_score = self._calculate_exact_match_score(components1, components2)
        core_score = self._calculate_core_business_score(components1, components2)
        semantic_score = self._calculate_semantic_score(components1, components2)
        char_score = self._calculate_character_score(name1, name2)
        
        # 3. 核心词汇冲突检测
        conflict_penalty = self._detect_core_conflict(components1, components2)
        
        # 4. 同义词加成
        synonym_bonus = self._calculate_synonym_bonus(components1, components2)
        
        # 5. 权重化综合计算
        weights = {
            'exact': 0.40,      # 精确匹配权重最高
            'core': 0.35,       # 核心业务词汇权重
            'semantic': 0.15,   # 语义相似权重
            'char': 0.10        # 字符相似权重
        }
        
        final_score = (
            exact_score * weights['exact'] +
            core_score * weights['core'] +
            semantic_score * weights['semantic'] +
            char_score * weights['char'] +
            synonym_bonus * 0.2  # 同义词加成
        ) * (1 - conflict_penalty)  # 应用冲突惩罚
        
        # 确保分数在[0,1]范围内
        final_score = max(0.0, min(1.0, final_score))
        
        logger.debug(f"单位名称相似度详情: {name1} <-> {name2}")
        logger.debug(f"  精确匹配: {exact_score:.3f}, 核心业务: {core_score:.3f}")
        logger.debug(f"  语义相似: {semantic_score:.3f}, 字符相似: {char_score:.3f}")
        logger.debug(f"  同义词加成: {synonym_bonus:.3f}, 冲突惩罚: {conflict_penalty:.3f}")
        logger.debug(f"  最终相似度: {final_score:.3f}")
        
        return final_score
    
    def _parse_company_name(self, name: str) -> CompanyNameComponents:
        """解析单位名称组件"""
        original = name.strip()
        
        # 1. 提取地区前缀
        region = ""
        remaining = original
        for prefix in sorted(self.region_prefixes, key=len, reverse=True):
            if remaining.startswith(prefix):
                region = prefix
                remaining = remaining[len(prefix):].strip()
                break
        
        # 2. 提取法律后缀
        legal_suffix = ""
        for suffix in self.legal_suffixes:
            if remaining.endswith(suffix):
                legal_suffix = suffix
                remaining = remaining[:-len(suffix)].strip()
                break
        
        # 3. 对剩余部分进行分词
        words = jieba.lcut(remaining)
        words = [w for w in words if w.strip() and w not in self.stop_words]
        
        # 4. 分类词汇
        core_business = []
        industry_type = []
        
        for word in words:
            if word in self.industry_common_words:
                industry_type.append(word)
            else:
                # 非通用词汇视为核心业务词汇
                core_business.append(word)
        
        # 5. 标准化处理
        normalized_parts = []
        if region:
            normalized_parts.append(region)
        normalized_parts.extend(core_business)
        normalized_parts.extend(industry_type)
        if legal_suffix:
            normalized_parts.append(legal_suffix)
        
        normalized = "".join(normalized_parts)
        
        return CompanyNameComponents(
            original=original,
            region=region,
            core_business=core_business,
            industry_type=industry_type,
            legal_suffix=legal_suffix,
            normalized=normalized
        )
    
    def _calculate_exact_match_score(self, comp1: CompanyNameComponents, comp2: CompanyNameComponents) -> float:
        """计算精确匹配分数"""
        if comp1.normalized == comp2.normalized:
            return 1.0
        
        # 检查核心业务词汇的精确匹配
        if comp1.core_business and comp2.core_business:
            core1_set = set(comp1.core_business)
            core2_set = set(comp2.core_business)
            
            if core1_set == core2_set:
                return 0.9  # 核心业务完全匹配，但其他部分可能不同
        
        return 0.0
    
    def _calculate_core_business_score(self, comp1: CompanyNameComponents, comp2: CompanyNameComponents) -> float:
        """计算核心业务词汇相似度"""
        if not comp1.core_business or not comp2.core_business:
            return 0.0
        
        core1_set = set(comp1.core_business)
        core2_set = set(comp2.core_business)
        
        # 应用同义词映射
        core1_normalized = {self.synonym_map.get(word, word) for word in core1_set}
        core2_normalized = {self.synonym_map.get(word, word) for word in core2_set}
        
        intersection = len(core1_normalized & core2_normalized)
        union = len(core1_normalized | core2_normalized)
        
        if union == 0:
            return 0.0
        
        return intersection / union
    
    def _calculate_semantic_score(self, comp1: CompanyNameComponents, comp2: CompanyNameComponents) -> float:
        """计算语义相似度"""
        # 结合行业类型词汇的相似度
        industry1_set = set(comp1.industry_type)
        industry2_set = set(comp2.industry_type)
        
        if not industry1_set and not industry2_set:
            return 0.0
        
        if not industry1_set or not industry2_set:
            return 0.3  # 一方有行业词汇，一方没有，给予中等分数
        
        intersection = len(industry1_set & industry2_set)
        union = len(industry1_set | industry2_set)
        
        return intersection / union if union > 0 else 0.0
    
    def _calculate_character_score(self, name1: str, name2: str) -> float:
        """计算字符级相似度"""
        return fuzz.ratio(name1, name2) / 100.0
    
    def _detect_core_conflict(self, comp1: CompanyNameComponents, comp2: CompanyNameComponents) -> float:
        """检测核心词汇冲突"""
        if not comp1.core_business or not comp2.core_business:
            return 0.0
        
        core1_set = set(comp1.core_business)
        core2_set = set(comp2.core_business)
        
        # 应用同义词映射
        core1_normalized = {self.synonym_map.get(word, word) for word in core1_set}
        core2_normalized = {self.synonym_map.get(word, word) for word in core2_set}
        
        intersection = len(core1_normalized & core2_normalized)
        
        # 如果核心词汇完全不重叠，且都有核心词汇，则存在冲突
        if intersection == 0 and len(core1_normalized) > 0 and len(core2_normalized) > 0:
            # 检查是否为明显的不同核心词汇（如"为民"vs"惠民"）
            if self._is_obvious_conflict(core1_normalized, core2_normalized):
                return 0.8  # 高冲突惩罚
            else:
                return 0.3  # 中等冲突惩罚
        
        return 0.0
    
    def _is_obvious_conflict(self, core1: Set[str], core2: Set[str]) -> bool:
        """判断是否为明显的核心词汇冲突"""
        # 检查是否存在相似但不同的核心词汇
        for word1 in core1:
            for word2 in core2:
                if len(word1) >= 2 and len(word2) >= 2:
                    # 字符相似度高但不相等，可能是冲突
                    char_sim = fuzz.ratio(word1, word2) / 100.0
                    if 0.5 <= char_sim < 1.0:
                        return True
        return False
    
    def _calculate_synonym_bonus(self, comp1: CompanyNameComponents, comp2: CompanyNameComponents) -> float:
        """计算同义词加成"""
        # 检查整体名称的同义词关系
        name1_parts = comp1.core_business + comp1.industry_type
        name2_parts = comp2.core_business + comp2.industry_type
        
        for group in self.synonym_groups:
            name1_in_group = any(part in group for part in name1_parts)
            name2_in_group = any(part in group for part in name2_parts)
            
            if name1_in_group and name2_in_group:
                return 0.5  # 同义词组加成
        
        # 检查特殊的同义词关系（如银行缩写）
        if self._check_bank_synonym(comp1.original, comp2.original):
            return 0.8
        
        return 0.0
    
    def _check_bank_synonym(self, name1: str, name2: str) -> bool:
        """检查银行类同义词"""
        bank_patterns = [
            ('浦东发展银行', '浦发银行'),
            ('招商银行', '招行'),
            ('中国建设银行', '建行'),
            ('中国工商银行', '工行'),
            ('中国农业银行', '农行'),
            ('中国银行', '中行'),
            ('交通银行', '交行'),
        ]
        
        for full_name, short_name in bank_patterns:
            if (full_name in name1 and short_name in name2) or \
               (short_name in name1 and full_name in name2):
                return True
        
        return False
    
    def get_detailed_analysis(self, name1: str, name2: str) -> Dict:
        """获取详细的相似度分析"""
        components1 = self._parse_company_name(name1)
        components2 = self._parse_company_name(name2)
        
        return {
            'similarity_score': self.calculate_similarity(name1, name2),
            'components1': {
                'region': components1.region,
                'core_business': components1.core_business,
                'industry_type': components1.industry_type,
                'legal_suffix': components1.legal_suffix,
                'normalized': components1.normalized
            },
            'components2': {
                'region': components2.region,
                'core_business': components2.core_business,
                'industry_type': components2.industry_type,
                'legal_suffix': components2.legal_suffix,
                'normalized': components2.normalized
            },
            'analysis': {
                'exact_match': self._calculate_exact_match_score(components1, components2),
                'core_business_match': self._calculate_core_business_score(components1, components2),
                'semantic_match': self._calculate_semantic_score(components1, components2),
                'character_match': self._calculate_character_score(name1, name2),
                'conflict_penalty': self._detect_core_conflict(components1, components2),
                'synonym_bonus': self._calculate_synonym_bonus(components1, components2)
            }
        }


# 测试函数
def test_intelligent_matcher():
    """测试智能匹配器"""
    matcher = IntelligentUnitNameMatcher()
    
    test_cases = [
        ("上海为民食品厂", "上海惠民食品厂"),
        ("北京华为科技有限公司", "北京华美科技有限公司"),
        ("深圳腾讯计算机系统有限公司", "深圳腾讯科技有限公司"),
        ("上海浦东发展银行", "上海浦发银行"),
        ("中国工商银行股份有限公司", "中国农业银行股份有限公司"),
    ]
    
    print("智能单位名称匹配器测试结果:")
    print("-" * 80)
    print(f"{'单位名称1':<30} {'单位名称2':<30} {'相似度':<8} {'分析'}")
    print("-" * 80)
    
    for name1, name2 in test_cases:
        similarity = matcher.calculate_similarity(name1, name2)
        analysis = "智能匹配" if similarity > 0.7 else "正确识别差异" if similarity < 0.3 else "需要人工确认"
        print(f"{name1:<30} {name2:<30} {similarity:.3f}    {analysis}")


if __name__ == "__main__":
    test_intelligent_matcher()
