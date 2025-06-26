"""
预筛选系统
用于快速筛选候选匹配记录
"""

import pymongo
from typing import Dict, List
import logging
from rapidfuzz import process, fuzz

logger = logging.getLogger(__name__)


class PrefilterSystem:
    """预筛选系统"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.db = db_manager.db
        
        # 预筛选配置
        self.config = {
            'name_similarity_threshold': 0.6,
            'max_candidates_per_method': 50,
            'enable_address_filter': True,
            'enable_legal_person_filter': True
        }
    
    def get_candidates(self, source_record: Dict) -> List[Dict]:
        """
        获取候选匹配记录
        
        Args:
            source_record: 源记录
            
        Returns:
            List[Dict]: 候选记录列表
        """
        candidates = set()
        
        # 1. 基于单位名称的快速筛选
        name_candidates = self._filter_by_unit_name(source_record)
        candidates.update(self._to_id_set(name_candidates))
        
        # 2. 基于地址的筛选
        if self.config['enable_address_filter']:
            addr_candidates = self._filter_by_address(source_record)
            candidates.update(self._to_id_set(addr_candidates))
        
        # 3. 基于法定代表人的筛选
        if self.config['enable_legal_person_filter']:
            legal_candidates = self._filter_by_legal_person(source_record)
            candidates.update(self._to_id_set(legal_candidates))
        
        # 转换回完整记录
        if candidates:
            return list(self.db['xxj_shdwjbxx'].find({'_id': {'$in': list(candidates)}}))
        else:
            return []
    
    def _filter_by_unit_name(self, source_record: Dict) -> List[Dict]:
        """基于单位名称筛选"""
        source_name = source_record.get('UNIT_NAME', '')
        if not source_name:
            return []
        
        # 使用文本搜索索引进行快速筛选
        try:
            # MongoDB文本搜索
            query = {'$text': {'$search': source_name}}
            candidates = list(self.db['xxj_shdwjbxx'].find(
                query, 
                {'score': {'$meta': 'textScore'}}
            ).sort([('score', {'$meta': 'textScore'})]).limit(self.config['max_candidates_per_method']))
            
            return candidates
            
        except Exception as e:
            logger.debug(f"文本搜索失败，使用正则表达式: {e}")
            
            # 备用方案：正则表达式搜索
            # 提取关键词
            keywords = self._extract_keywords(source_name)
            if keywords:
                regex_pattern = '|'.join(keywords)
                query = {'dwmc': {'$regex': regex_pattern, '$options': 'i'}}
                candidates = list(self.db['xxj_shdwjbxx'].find(query).limit(self.config['max_candidates_per_method']))
                return candidates
            
            return []
    
    def _filter_by_address(self, source_record: Dict) -> List[Dict]:
        """基于地址筛选"""
        source_address = source_record.get('ADDRESS', '')
        if not source_address:
            return []
        
        # 提取地址关键词
        keywords = self._extract_address_keywords(source_address)
        if not keywords:
            return []
        
        # 构建查询
        regex_pattern = '|'.join(keywords)
        query = {'dwdz': {'$regex': regex_pattern, '$options': 'i'}}
        
        candidates = list(self.db['xxj_shdwjbxx'].find(query).limit(self.config['max_candidates_per_method']))
        return candidates
    
    def _filter_by_legal_person(self, source_record: Dict) -> List[Dict]:
        """基于法定代表人筛选"""
        source_legal = source_record.get('LEGAL_PEOPLE', '')
        if not source_legal:
            return []
        
        # 精确匹配法定代表人
        query = {'fddbr': source_legal}
        candidates = list(self.db['xxj_shdwjbxx'].find(query).limit(self.config['max_candidates_per_method']))
        return candidates
    
    def _extract_keywords(self, text: str) -> List[str]:
        """提取关键词"""
        if not text:
            return []
        
        # 移除常见的公司后缀
        suffixes = ['有限公司', '股份有限公司', '有限责任公司', '公司', '厂', '店', '部', '中心', '所']
        clean_text = text
        for suffix in suffixes:
            clean_text = clean_text.replace(suffix, '')
        
        # 如果清理后的文本太短，使用原文本
        if len(clean_text) < 3:
            clean_text = text
        
        # 提取长度大于1的子字符串作为关键词
        keywords = []
        if len(clean_text) >= 2:
            keywords.append(clean_text)
        
        # 如果文本较长，提取前半部分作为关键词
        if len(clean_text) > 6:
            keywords.append(clean_text[:len(clean_text)//2])
        
        return keywords
    
    def _extract_address_keywords(self, address: str) -> List[str]:
        """提取地址关键词"""
        if not address:
            return []
        
        keywords = []
        
        # 提取区县信息
        import re
        district_match = re.search(r'(\w+[区县市])', address)
        if district_match:
            keywords.append(district_match.group(1))
        
        # 提取街道信息
        street_match = re.search(r'(\w+[街道路弄巷])', address)
        if street_match:
            keywords.append(street_match.group(1))
        
        # 如果没有提取到关键词，使用前半部分地址
        if not keywords and len(address) > 6:
            keywords.append(address[:len(address)//2])
        
        return keywords
    
    def _to_id_set(self, records: List[Dict]) -> set:
        """转换记录列表为ID集合"""
        return {record['_id'] for record in records if '_id' in record}
