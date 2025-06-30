"""
预筛选系统
用于快速筛选候选匹配记录
"""

import pymongo
from typing import Dict, List
import logging
import jieba
import json
import re

logger = logging.getLogger(__name__)


class PrefilterSystem:
    """预筛选系统"""
    
    def __init__(self, db: pymongo.database.Database):
        if db is None:
            raise ValueError("Database instance 'db' is required.")
        self.db = db
        
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
            # 增加查询候选项数量的日志
            logger.info(f"为 {source_record.get('UNIT_NAME', 'Unknown')} 找到 {len(candidates)} 个候选。")
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
            logger.debug(f"文本搜索失败，回退到正则表达式搜索: {e}")
            
            # 备用方案：基于jieba分词的OR查询
            keywords = self._extract_keywords(source_name)
            if not keywords:
                return []

            # 为每个关键词构建一个正则表达式查询，并对特殊字符进行转义
            regex_queries = [{'dwmc': {'$regex': re.escape(keyword), '$options': 'i'}} for keyword in keywords]
            
            # 使用$or操作符组合查询
            query = {'$or': regex_queries}
            
            # 在执行前记录查询
            logger.info(f"正在执行名称预过滤查询: {json.dumps(query, ensure_ascii=False)}")
            
            candidates = list(self.db['xxj_shdwjbxx'].find(query).limit(self.config['max_candidates_per_method']))
            return candidates

    def _filter_by_address(self, source_record: Dict) -> List[Dict]:
        """基于地址筛选，现在使用文本索引"""
        source_address = source_record.get('ADDRESS', '')
        if not isinstance(source_address, str) or not source_address.strip():
            return []
        
        # 使用与名称筛选相同的文本搜索逻辑
        try:
            query = {'$text': {'$search': source_address}}
            candidates = list(self.db['xxj_shdwjbxx'].find(
                query, 
                {'score': {'$meta': 'textScore'}}
            ).sort([('score', {'$meta': 'textScore'})]).limit(self.config['max_candidates_per_method']))
            return candidates
        except Exception as e:
            logger.debug(f"地址文本搜索失败: {e}")
            # 作为备用，可以返回空列表或执行更简单的查询
            return []
    
    def _filter_by_legal_person(self, source_record: Dict) -> List[Dict]:
        """基于法定代表人筛选"""
        source_legal = source_record.get('LEGAL_PEOPLE', '')
        if not source_legal:
            return []
        
        # 精确匹配法定代表人，对特殊字符进行转义
        query = {'fddbr': re.escape(source_legal)}
        
        # 在执行前记录查询
        logger.info(f"正在执行法人预过滤查询: {json.dumps(query, ensure_ascii=False)}")
        
        candidates = list(self.db['xxj_shdwjbxx'].find(query).limit(self.config['max_candidates_per_method']))
        return candidates
    
    def _extract_keywords(self, text: str) -> List[str]:
        """
        使用jieba分词提取关键词，并过滤掉停用词和通用词。
        """
        # 防御性编程：在处理前，确保输入值是字符串类型，以处理数字等异常数据。
        text = str(text) if text is not None else ''
        if not text:
            return []

        # 定义停用词和通用后缀
        stop_words = {'公司', '有限', '责任', '股份', '集团', '发展', '实业', '科技'}
        suffixes = ['有限公司', '股份有限公司', '有限责任公司', '公司', '厂', '店', '部', '中心', '所']

        # 1. 移除常见后缀，以帮助jieba更好地识别核心名称
        for suffix in suffixes:
            if text.endswith(suffix):
                text = text[:-len(suffix)]
                break
        
        # 2. 使用jieba进行分词
        words = jieba.lcut(text, cut_all=False)
        
        # 3. 过滤关键词
        keywords = []
        for word in words:
            # 过滤掉单字、数字、停用词和过短的词
            if len(word) > 1 and not word.isdigit() and word not in stop_words:
                keywords.append(word)

        # 4. 如果没有提取到关键词，则使用原始文本中最长的连续非后缀部分作为最后的尝试
        if not keywords:
            clean_text = text
            # 再次尝试移除后缀
            for suffix in suffixes:
                clean_text = clean_text.replace(suffix, '')
            
            if len(clean_text) > 1:
                keywords.append(clean_text.strip())

        logger.debug(f"为 '{text}' 提取的关键词: {keywords}")
        return list(set(keywords)) # 返回去重后的关键词列表
    
    def _extract_address_keywords(self, address: str) -> List[str]:
        """
        使用jieba分词提取地址中的关键词。
        """
        # 防御性编程：在处理前，确保输入值是字符串类型。
        address = str(address) if address is not None else ''
        if not address:
            return []

        # 定义地址停用词
        stop_words = {'市', '区', '县', '镇', '乡', '村', '街道', '路', '号', '弄', '室', '栋', '座'}
        
        # 使用jieba进行分词
        words = jieba.lcut(address, cut_all=False)
        
        # 过滤关键词
        keywords = []
        for word in words:
            # 过滤掉单字、数字、停用词
            if len(word) > 1 and not word.isdigit() and word not in stop_words:
                # 进一步移除末尾的常见单位词，如'路', '号'
                if word.endswith(('路', '号', '弄', '街', '巷')):
                    word = word[:-1]

                if len(word) > 1: # 再次检查长度
                    keywords.append(word)

        logger.debug(f"为地址 '{address}' 提取的关键词: {keywords}")
        return list(set(keywords))
    
    def _to_id_set(self, records: List[Dict]) -> set:
        """转换记录列表为ID集合"""
        return {record['_id'] for record in records if '_id' in record}
