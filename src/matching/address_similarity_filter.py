#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
地址相似度过滤器
用于过滤地址不匹配的错误匹配结果
"""

import logging
import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class AddressFilterConfig:
    """地址过滤器配置"""
    min_address_similarity: float = 0.3  # 最小地址相似度阈值
    address_weight: float = 0.4  # 地址在总相似度中的权重
    enable_geographic_validation: bool = False  # 是否启用地理位置验证
    strict_mode: bool = True  # 严格模式：地址不匹配直接拒绝

class AddressSimilarityFilter:
    """地址相似度过滤器"""
    
    def __init__(self, config: Optional[AddressFilterConfig] = None):
        self.config = config or AddressFilterConfig()
        
        # 地址关键词权重
        self.keyword_weights = {
            '市': 0.3,
            '区': 0.4, 
            '县': 0.4,
            '镇': 0.3,
            '街道': 0.3,
            '路': 0.6,
            '街': 0.6,
            '道': 0.6,
            '巷': 0.5,
            '弄': 0.5,
            '大道': 0.6
        }
        
        # 统计信息
        self.stats = {
            'total_processed': 0,
            'filtered_out': 0,
            'address_mismatch_filtered': 0,
            'geographic_filtered': 0
        }
        
    def filter_matches(self, matches: List[Dict], source_record: Dict, 
                      mappings: List[Dict]) -> List[Dict]:
        """
        过滤匹配结果，移除地址不匹配的结果
        
        Args:
            matches: 匹配结果列表
            source_record: 源记录
            mappings: 字段映射配置
            
        Returns:
            List[Dict]: 过滤后的匹配结果
        """
        if not matches:
            return matches
            
        filtered_matches = []
        
        # 获取源记录的地址字段
        source_address = self._extract_address_from_record(source_record, mappings, 'source')
        
        for match in matches:
            self.stats['total_processed'] += 1
            
            target_record = match.get('target_record', {})
            target_address = self._extract_address_from_record(target_record, mappings, 'target')
            
            # 计算地址相似度
            address_similarity = self.calculate_address_similarity(source_address, target_address)
            
            # 应用过滤规则
            if self._should_keep_match(match, address_similarity, source_address, target_address):
                # 更新匹配结果，添加地址相似度信息
                match['address_similarity'] = address_similarity
                match['address_filter_passed'] = True
                filtered_matches.append(match)
            else:
                self.stats['filtered_out'] += 1
                self.stats['address_mismatch_filtered'] += 1
                
                logger.debug(f"地址不匹配过滤: 总相似度={match.get('similarity', 0):.3f}, "
                           f"地址相似度={address_similarity:.3f}")
                logger.debug(f"  源地址: {source_address}")
                logger.debug(f"  目标地址: {target_address}")
        
        filter_rate = self.stats['filtered_out'] / self.stats['total_processed'] * 100
        logger.info(f"地址过滤器统计: 处理{self.stats['total_processed']}条, "
                   f"过滤{self.stats['filtered_out']}条 ({filter_rate:.1f}%)")
        
        return filtered_matches
    
    def calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """
        计算两个地址的相似度
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            float: 相似度 (0-1)
        """
        if not addr1 or not addr2:
            return 0.0
            
        # 提取地址关键词
        keywords1 = self._extract_address_keywords(addr1)
        keywords2 = self._extract_address_keywords(addr2)
        
        if not keywords1 and not keywords2:
            return 0.0
        
        if not keywords1 or not keywords2:
            return 0.0
        
        # 计算加权相似度
        return self._calculate_weighted_similarity(keywords1, keywords2)
    
    def _extract_address_keywords(self, address: str) -> Dict[str, float]:
        """
        提取地址关键词及其权重
        
        Args:
            address: 地址字符串
            
        Returns:
            Dict[str, float]: 关键词及其权重
        """
        if not address:
            return {}
        
        keywords = {}
        
        # 清理地址：移除门牌号等具体信息
        cleaned_addr = re.sub(r'\d+号.*$', '', address)
        cleaned_addr = re.sub(r'[层楼室栋座幢]\d*.*$', '', cleaned_addr)
        
        # 提取市
        city_pattern = r'([^省]*?市)'
        cities = re.findall(city_pattern, cleaned_addr)
        for city in cities:
            if len(city) >= 3:
                keywords[city] = self.keyword_weights.get('市', 0.3)
        
        # 提取区县
        district_pattern = r'([^市]*?)(区|县)'
        districts = re.findall(district_pattern, cleaned_addr)
        for district, suffix in districts:
            if len(district) >= 2:
                full_district = district + suffix
                keywords[full_district] = self.keyword_weights.get(suffix, 0.4)
        
        # 提取镇街道
        town_pattern = r'([^区县]*?)(镇|街道)'
        towns = re.findall(town_pattern, cleaned_addr)
        for town, suffix in towns:
            if len(town) >= 2:
                full_town = town + suffix
                keywords[full_town] = self.keyword_weights.get(suffix, 0.3)
        
        # 提取路街
        road_pattern = r'([^镇街道区县市]*?)(路|街|道|巷|弄|大道)'
        roads = re.findall(road_pattern, cleaned_addr)
        for road, suffix in roads:
            if len(road) >= 2:
                full_road = road + suffix
                keywords[full_road] = self.keyword_weights.get(suffix, 0.6)
        
        return keywords
    
    def _calculate_weighted_similarity(self, keywords1: Dict[str, float], 
                                     keywords2: Dict[str, float]) -> float:
        """
        计算加权相似度
        
        Args:
            keywords1: 地址1的关键词及权重
            keywords2: 地址2的关键词及权重
            
        Returns:
            float: 加权相似度
        """
        if not keywords1 or not keywords2:
            return 0.0
        
        # 计算交集权重
        intersection_weight = 0.0
        for keyword in keywords1:
            if keyword in keywords2:
                # 使用较小的权重（更保守）
                intersection_weight += min(keywords1[keyword], keywords2[keyword])
        
        # 计算并集权重
        union_weight = 0.0
        all_keywords = set(keywords1.keys()) | set(keywords2.keys())
        for keyword in all_keywords:
            weight1 = keywords1.get(keyword, 0)
            weight2 = keywords2.get(keyword, 0)
            union_weight += max(weight1, weight2)
        
        return intersection_weight / union_weight if union_weight > 0 else 0.0
    
    def _extract_address_from_record(self, record: Dict, mappings: List[Dict], 
                                   field_type: str) -> str:
        """
        从记录中提取地址字段
        
        Args:
            record: 记录数据
            mappings: 字段映射
            field_type: 字段类型 ('source' 或 'target')
            
        Returns:
            str: 地址字符串
        """
        for mapping in mappings:
            field_name = mapping.get(f'{field_type}_field', '')
            
            # 检查是否为地址字段
            if self._is_address_field(field_name):
                address = record.get(field_name, '')
                if address:
                    return str(address)
        
        return ''
    
    def _is_address_field(self, field_name: str) -> bool:
        """
        判断字段是否为地址字段
        
        Args:
            field_name: 字段名
            
        Returns:
            bool: 是否为地址字段
        """
        address_keywords = ['地址', 'address', 'addr', '住址', '所在地', 'COMPANY_ADDRESS']
        field_lower = field_name.lower()
        
        return any(keyword.lower() in field_lower for keyword in address_keywords)
    
    def _should_keep_match(self, match: Dict, address_similarity: float,
                          source_address: str, target_address: str) -> bool:
        """
        判断是否应该保留匹配结果
        
        Args:
            match: 匹配结果
            address_similarity: 地址相似度
            source_address: 源地址
            target_address: 目标地址
            
        Returns:
            bool: 是否保留
        """
        overall_similarity = match.get('similarity', 0)
        
        # 如果没有地址信息，按原逻辑处理
        if not source_address or not target_address:
            return True
        
        # 严格模式：地址相似度必须达到阈值
        if self.config.strict_mode:
            if address_similarity < self.config.min_address_similarity:
                return False
        
        # 综合评估：考虑总相似度和地址相似度
        weighted_score = (overall_similarity * (1 - self.config.address_weight) + 
                         address_similarity * self.config.address_weight)
        
        # 如果加权后的分数仍然较高，保留
        return weighted_score >= 0.4  # 可调整的综合阈值
    
    def get_filter_stats(self) -> Dict:
        """获取过滤器统计信息"""
        return self.stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total_processed': 0,
            'filtered_out': 0,
            'address_mismatch_filtered': 0,
            'geographic_filtered': 0
        }
