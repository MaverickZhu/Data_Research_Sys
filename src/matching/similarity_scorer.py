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
        计算地址相似度
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not addr1 or not addr2:
            return 0.0
        
        # 标准化地址
        addr1 = normalize_string(addr1)
        addr2 = normalize_string(addr2)
        
        if addr1 == addr2:
            return 1.0
        
        # 地址分段匹配
        if self.address_config.get('enable_segmentation', True):
            return self._segmented_address_similarity(addr1, addr2)
        else:
            # 简单字符串匹配
            return self.calculate_string_similarity(addr1, addr2)
    
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
            'detail': ''
        }
        
        # 简单的地址分解（可以根据实际情况优化）
        # 匹配省份
        province_pattern = r'(.*?省|.*?市|.*?区|.*?自治区)'
        province_match = re.search(province_pattern, address)
        if province_match:
            components['province'] = province_match.group(1)
            address = address.replace(components['province'], '', 1)
        
        # 匹配城市
        city_pattern = r'(.*?市|.*?州|.*?县)'
        city_match = re.search(city_pattern, address)
        if city_match:
            components['city'] = city_match.group(1)
            address = address.replace(components['city'], '', 1)
        
        # 匹配区县
        district_pattern = r'(.*?区|.*?县|.*?镇)'
        district_match = re.search(district_pattern, address)
        if district_match:
            components['district'] = district_match.group(1)
            address = address.replace(components['district'], '', 1)
        
        # 剩余部分作为详细地址
        components['detail'] = address.strip()
        
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