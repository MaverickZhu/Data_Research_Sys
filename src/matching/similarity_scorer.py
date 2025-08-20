"""
相似度计算器模块
实现多种相似度计算算法
"""

import logging
import re
from typing import Dict, List, Optional, Tuple, Any
import jieba
import pypinyin
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from src.utils.helpers import normalize_string, normalize_phone, safe_float_convert, calculate_percentage_diff

logger = logging.getLogger(__name__)


class SimilarityCalculator:
    """相似度计算器"""
    
    def __init__(self, config: Dict):
        """
        初始化相似度计算器
        
        Args:
            config: 配置参数
        """
        self.config = config
        self.string_config = config.get('string_similarity', {})
        self.chinese_config = self.string_config.get('chinese_processing', {})
        self.numeric_config = config.get('numeric_similarity', {})
        self.phone_config = config.get('phone_similarity', {})
        self.address_config = config.get('address_similarity', {})
        
        # 初始化TF-IDF向量化器
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words=None,
            ngram_range=(1, 2)
        )
        
    def calculate_string_similarity(self, str1: str, str2: str) -> float:
        """
        计算字符串相似度
        
        Args:
            str1: 字符串1
            str2: 字符串2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not str1 or not str2:
            return 0.0
            
        # 标准化字符串
        str1 = normalize_string(str1)
        str2 = normalize_string(str2)
        
        if str1 == str2:
            return 1.0
            
        # 获取算法配置
        algorithms = self.string_config.get('algorithms', [])
        if not algorithms:
            algorithms = [
                {'name': 'levenshtein', 'weight': 0.3},
                {'name': 'jaro_winkler', 'weight': 0.3},
                {'name': 'cosine', 'weight': 0.4}
            ]
        
        total_score = 0.0
        total_weight = 0.0
        
        for algorithm in algorithms:
            name = algorithm['name']
            weight = algorithm['weight']
            
            if name == 'levenshtein':
                score = self._levenshtein_similarity(str1, str2)
            elif name == 'jaro_winkler':
                score = self._jaro_winkler_similarity(str1, str2)
            elif name == 'cosine':
                score = self._cosine_similarity(str1, str2)
            else:
                continue
                
            total_score += score * weight
            total_weight += weight
        
        # 中文处理增强
        if self.chinese_config.get('enable_pinyin', True):
            pinyin_score = self._pinyin_similarity(str1, str2)
            total_score += pinyin_score * 0.2
            total_weight += 0.2
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def _levenshtein_similarity(self, str1: str, str2: str) -> float:
        """Levenshtein距离相似度"""
        return fuzz.ratio(str1, str2) / 100.0
    
    def _jaro_winkler_similarity(self, str1: str, str2: str) -> float:
        """Jaro-Winkler相似度"""
        return fuzz.token_sort_ratio(str1, str2) / 100.0
    
    def _cosine_similarity(self, str1: str, str2: str) -> float:
        """余弦相似度"""
        try:
            # 使用TF-IDF向量化
            corpus = [str1, str2]
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(corpus)
            similarity_matrix = cosine_similarity(tfidf_matrix)
            
            return similarity_matrix[0, 1]
        except Exception as e:
            logger.debug(f"余弦相似度计算失败: {str(e)}")
            return 0.0
    
    def _pinyin_similarity(self, str1: str, str2: str) -> float:
        """拼音相似度"""
        try:
            # 转换为拼音
            pinyin1 = ''.join([item[0] for item in pypinyin.pinyin(str1, style=pypinyin.NORMAL)])
            pinyin2 = ''.join([item[0] for item in pypinyin.pinyin(str2, style=pypinyin.NORMAL)])
            
            if not pinyin1 or not pinyin2:
                return 0.0
            
            return fuzz.ratio(pinyin1, pinyin2) / 100.0
        except Exception as e:
            logger.debug(f"拼音相似度计算失败: {str(e)}")
            return 0.0
    
    def calculate_numeric_similarity(self, num1: Any, num2: Any, field_config: Dict) -> float:
        """
        计算数值相似度
        
        Args:
            num1: 数值1
            num2: 数值2
            field_config: 字段配置
            
        Returns:
            float: 相似度分数 (0-1)
        """
        # 转换为浮点数
        val1 = safe_float_convert(num1)
        val2 = safe_float_convert(num2)
        
        if val1 == 0 and val2 == 0:
            return 1.0
        
        # 获取容差配置
        tolerance = field_config.get('tolerance', 0.1)
        
        # 计算百分比差异
        diff = calculate_percentage_diff(val1, val2)
        
        # 如果差异在容差范围内，返回高相似度
        if diff <= tolerance:
            return 1.0 - (diff / tolerance) * 0.2  # 最高0.8-1.0
        else:
            # 超出容差范围，相似度快速下降
            return max(0.0, 1.0 - diff)
    
    def calculate_phone_similarity(self, phone1: str, phone2: str) -> float:
        """
        计算电话号码相似度
        
        Args:
            phone1: 电话号码1
            phone2: 电话号码2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not phone1 or not phone2:
            return 0.0
        
        # 标准化电话号码
        norm_phone1 = normalize_phone(phone1)
        norm_phone2 = normalize_phone(phone2)
        
        if not norm_phone1 or not norm_phone2:
            return 0.0
        
        # 完全匹配
        if norm_phone1 == norm_phone2:
            return 1.0
        
        # 部分匹配（后7位）
        if len(norm_phone1) >= 7 and len(norm_phone2) >= 7:
            suffix1 = norm_phone1[-7:]
            suffix2 = norm_phone2[-7:]
            if suffix1 == suffix2:
                return 0.8
        
        # 使用编辑距离
        return fuzz.ratio(norm_phone1, norm_phone2) / 100.0 * 0.6
    
    def calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """
        计算地址相似度（增强版 - 支持模糊地址与标准地址匹配）
        
        Args:
            addr1: 地址1（可能是模糊描述地址）
            addr2: 地址2（可能是标准地址）
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not addr1 or not addr2:
            return 0.0
        
        # 地址标准化预处理
        from .address_normalizer import normalize_address_for_matching
        normalized_addr1 = normalize_address_for_matching(addr1)
        normalized_addr2 = normalize_address_for_matching(addr2)
        
        logger.debug(f"地址标准化: '{addr1}' -> '{normalized_addr1}'")
        logger.debug(f"地址标准化: '{addr2}' -> '{normalized_addr2}'")
        
        if normalized_addr1 == normalized_addr2:
            return 1.0
        
        # 使用标准化后的地址进行语义分析
        return self._enhanced_address_semantic_similarity(normalized_addr1, normalized_addr2)
    
    def _enhanced_address_semantic_similarity(self, addr1: str, addr2: str) -> float:
        """
        增强的地址语义相似度分析
        专门处理模糊地址描述与标准地址的匹配
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        try:
            # 1. 提取地址核心组件
            components1 = self._extract_enhanced_address_components(addr1)
            components2 = self._extract_enhanced_address_components(addr2)
            
            # 2. 计算组件权重化相似度
            return self._calculate_weighted_address_similarity(components1, components2)
            
        except Exception as e:
            logger.debug(f"增强地址语义分析失败，回退到基础方法: {str(e)}")
            # 回退到原有的分段匹配
            if self.address_config.get('enable_segmentation', True):
                return self._segmented_address_similarity(addr1, addr2)
            else:
                return self.calculate_string_similarity(addr1, addr2)
    
    def _extract_enhanced_address_components(self, address: str) -> Dict[str, str]:
        """
        提取增强的地址组件
        支持更复杂的地址解析，包括建筑物名称、门牌号等
        """
        components = {
            'province': '',      # 省份
            'city': '',         # 城市
            'district': '',     # 区县
            'street': '',       # 街道/路
            'number': '',       # 门牌号
            'building': '',     # 建筑物名称
            'detail': ''        # 其他详细信息
        }
        
        # 地址解析正则表达式（优化版）
        patterns = {
            'province': r'([^省市区县]{2,8}(?:省|市|自治区))',
            'city': r'([^省市区县]{2,8}(?:市|州|县))',
            'district': r'([^省市区县]{2,8}(?:区|县|镇|开发区|高新区|经济区))',
            'street': r'([^路街道巷弄里]{1,20}(?:路|街|道|巷|弄|里|大街|大道|街道))',
            'number': r'(\d+(?:号|栋|幢|座|楼|室|层))',
            'building': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:大厦|大楼|广场|中心|院|园|村|小区|公司|厂|店|馆|所|站|场|生态园|科技园|工业园|产业园))'
        }
        
        # 预处理：移除行政层级词汇
        remaining_address = address
        remaining_address = re.sub(r'市辖区', '', remaining_address)  # 移除"市辖区"
        remaining_address = re.sub(r'县辖区', '', remaining_address)  # 移除"县辖区"
        
        # 按顺序提取各组件
        for component, pattern in patterns.items():
            matches = re.findall(pattern, remaining_address)
            if matches:
                # 取最长的匹配（通常更准确）
                components[component] = max(matches, key=len)
                # 从剩余地址中移除已匹配的部分
                remaining_address = remaining_address.replace(components[component], '', 1)
        
        # 剩余部分作为详细信息
        components['detail'] = remaining_address.strip()
        
        return components
    
    def _calculate_weighted_address_similarity(self, comp1: Dict[str, str], comp2: Dict[str, str]) -> float:
        """
        计算权重化的地址组件相似度
        根据地址组件的重要性分配不同权重
        """
        # 地址组件权重配置（优化版 - 基于地址精确度重要性）
        weights = {
            'number': 0.50,      # 门牌号最重要（881号是精确定位）
            'street': 0.40,      # 街道次重要（天宝路是具体路名）
            'district': 0.15,    # 区县（虹口区是大概念）
            'city': 0.10,        # 城市（上海市是大概念）
            'province': 0.05,    # 省份（最大概念）
            'building': 0.05,    # 建筑物名称（天宝养老院是附加信息，权重最低）
            'detail': 0.02       # 其他详细信息
        }
        
        total_score = 0.0
        total_weight = 0.0
        
        for component, weight in weights.items():
            val1 = comp1.get(component, '').strip()
            val2 = comp2.get(component, '').strip()
            
            # 只有当至少一个组件有值时才计算
            if val1 or val2:
                if val1 and val2:
                    # 两个都有值，计算相似度
                    similarity = self._calculate_component_similarity(val1, val2, component)
                elif val1 == val2:  # 都为空
                    similarity = 1.0
                else:
                    # 一个有值一个没有，相似度为0
                    similarity = 0.0
                
                total_score += similarity * weight
                total_weight += weight
        
        # 计算最终得分
        if total_weight == 0:
            return 0.0
        
        final_score = total_score / total_weight
        
        # 应用组件匹配奖励机制
        final_score = self._apply_address_matching_bonus(final_score, comp1, comp2)
        
        return min(1.0, final_score)  # 确保不超过1.0
    
    def _calculate_component_similarity(self, val1: str, val2: str, component_type: str) -> float:
        """
        计算特定组件的相似度
        根据组件类型使用不同的匹配策略
        """
        if val1 == val2:
            return 1.0
        
        if component_type == 'number':
            # 门牌号：数字部分完全匹配得分更高
            num1 = re.findall(r'\d+', val1)
            num2 = re.findall(r'\d+', val2)
            if num1 and num2 and num1[0] == num2[0]:
                return 0.95  # 数字相同，但可能单位不同
            else:
                return fuzz.ratio(val1, val2) / 100.0 * 0.6
        
        elif component_type in ['street', 'building']:
            # 街道和建筑物：使用多种算法综合评估
            scores = [
                fuzz.ratio(val1, val2) / 100.0,
                fuzz.partial_ratio(val1, val2) / 100.0,
                fuzz.token_set_ratio(val1, val2) / 100.0
            ]
            return max(scores)  # 取最高分
        
        else:
            # 其他组件：使用标准字符串相似度
            return self.calculate_string_similarity(val1, val2)
    
    def _apply_address_matching_bonus(self, base_score: float, comp1: Dict[str, str], comp2: Dict[str, str]) -> float:
        """
        应用地址匹配奖励机制
        当多个关键组件匹配时给予额外奖励
        """
        bonus = 0.0
        
        # 关键组件完全匹配奖励
        key_components = ['number', 'street', 'district']
        matched_key_components = 0
        
        for component in key_components:
            val1 = comp1.get(component, '').strip()
            val2 = comp2.get(component, '').strip()
            if val1 and val2 and val1 == val2:
                matched_key_components += 1
        
        # 根据匹配的关键组件数量给予奖励
        if matched_key_components >= 2:
            bonus += 0.1  # 两个或以上关键组件匹配
        elif matched_key_components == 1:
            bonus += 0.05  # 一个关键组件匹配
        
        # 建筑物名称匹配奖励
        building1 = comp1.get('building', '').strip()
        building2 = comp2.get('building', '').strip()
        if building1 and building2:
            building_sim = self.calculate_string_similarity(building1, building2)
            if building_sim > 0.8:
                bonus += 0.05
        
        return base_score + bonus
    
    def _segmented_address_similarity(self, addr1: str, addr2: str) -> float:
        """分段地址相似度"""
        try:
            # 提取地址组件
            components1 = self._extract_address_components(addr1)
            components2 = self._extract_address_components(addr2)
            
            # 权重配置
            weights = {
                'province': self.address_config.get('province_weight', 0.2),
                'city': self.address_config.get('city_weight', 0.3),
                'district': self.address_config.get('district_weight', 0.3),
                'detail': self.address_config.get('detail_weight', 0.2)
            }
            
            total_score = 0.0
            total_weight = 0.0
            
            for component, weight in weights.items():
                comp1 = components1.get(component, '')
                comp2 = components2.get(component, '')
                
                if comp1 or comp2:
                    score = self.calculate_string_similarity(comp1, comp2)
                    total_score += score * weight
                    total_weight += weight
            
            return total_score / total_weight if total_weight > 0 else 0.0
            
        except Exception as e:
            logger.debug(f"分段地址相似度计算失败: {str(e)}")
            return self.calculate_string_similarity(addr1, addr2)
    
    def _extract_address_components(self, address: str) -> Dict[str, str]:
        """提取地址组件"""
        components = {
            'province': '',
            'city': '',
            'district': '',
            'street': '',
            'number': '',
            'building': '',
            'detail': ''
        }
        
        # 预处理：移除行政层级词汇
        remaining_address = address
        remaining_address = re.sub(r'市辖区', '', remaining_address)  # 移除"市辖区"
        remaining_address = re.sub(r'县辖区', '', remaining_address)  # 移除"县辖区"
        
        # 地址解析正则表达式（优化版）
        patterns = {
            'province': r'([^省市区县]{2,8}(?:省|市|自治区))',
            'city': r'([^省市区县]{2,8}(?:市|州|县))',
            'district': r'([^省市区县]{2,8}(?:区|县|镇|开发区|高新区|经济区))',
            'street': r'([^路街道巷弄里]{1,20}(?:路|街|道|巷|弄|里|大街|大道|街道))',
            'number': r'(\d+(?:号|栋|幢|座|楼|室|层))',
            'building': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:大厦|大楼|广场|中心|院|园|村|小区|公司|厂|店|馆|所|站|场|生态园|科技园|工业园|产业园))'
        }
        
        # 按顺序提取各组件
        for component, pattern in patterns.items():
            matches = re.findall(pattern, remaining_address)
            if matches:
                components[component] = matches[0]
                remaining_address = remaining_address.replace(matches[0], '', 1)
        
        # 剩余部分作为详细地址
        components['detail'] = remaining_address.strip()
        
        return components
    
    def calculate_comprehensive_similarity(self, source_record: Dict, target_record: Dict, 
                                        field_configs: Dict) -> Tuple[float, Dict]:
        """
        计算综合相似度
        
        Args:
            source_record: 源记录
            target_record: 目标记录
            field_configs: 字段配置
            
        Returns:
            Tuple[float, Dict]: (综合相似度, 字段相似度详情)
        """
        
        field_similarities = {}
        weighted_score = 0.0
        total_weight = 0.0
        
        for field_name, field_config in field_configs.items():
            source_field = field_config['source_field']
            target_field = field_config.get('target_field')
            weight = field_config['weight']
            match_type = field_config['match_type']
            
            # 跳过目标字段为空的情况
            if not target_field:
                continue
            
            source_value = source_record.get(source_field)
            target_value = target_record.get(target_field)
            
            # 计算字段相似度
            if match_type == 'string':
                similarity = self.calculate_string_similarity(
                    str(source_value) if source_value else '',
                    str(target_value) if target_value else ''
                )
            elif match_type == 'numeric':
                similarity = self.calculate_numeric_similarity(
                    source_value, target_value, field_config
                )
            elif match_type == 'phone':
                similarity = self.calculate_phone_similarity(
                    str(source_value) if source_value else '',
                    str(target_value) if target_value else ''
                )
            elif match_type == 'address':
                similarity = self.calculate_address_similarity(
                    str(source_value) if source_value else '',
                    str(target_value) if target_value else ''
                )
            else:
                similarity = 0.0
            
            field_similarities[field_name] = {
                'similarity': similarity,
                'weight': weight,
                'source_value': source_value,
                'target_value': target_value
            }
            
            weighted_score += similarity * weight
            total_weight += weight
        
        # 计算综合相似度
        comprehensive_score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        return comprehensive_score, field_similarities 