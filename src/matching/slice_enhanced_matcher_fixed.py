#!/usr/bin/env python3
"""
修复的切片增强匹配器
解决calculate_similarity方法调用问题
"""

import re
import time
import jieba
import pymongo
from typing import Dict, List, Set, Optional
from fuzzywuzzy import fuzz
from src.utils.logger import logger

class SliceEnhancedMatcher:
    """修复版的切片增强匹配器"""
    
    def __init__(self, db_manager=None):
        """初始化切片增强匹配器"""
        if db_manager:
            # 使用get_db()方法获取数据库实例
            if hasattr(db_manager, 'get_db'):
                self.db = db_manager.get_db()
            elif hasattr(db_manager, 'db'):
                self.db = db_manager.db
            else:
                # 降级到默认连接
                client = pymongo.MongoClient('mongodb://localhost:27017/')
                self.db = client['Unit_Info']
        else:
            client = pymongo.MongoClient('mongodb://localhost:27017/')
            self.db = client['Unit_Info']
        
        # 缓存集合引用
        self.source_slice_collection = self.db['xfaqpc_jzdwxx_name_slices']
        self.source_keyword_collection = self.db['xfaqpc_jzdwxx_name_keywords']
        self.target_slice_collection = self.db['xxj_shdwjbxx_name_slices']
        self.target_keyword_collection = self.db['xxj_shdwjbxx_name_keywords']
        self.target_collection = self.db['xxj_shdwjbxx']
        self.cache_collection = self.db['unit_name_similarity_cache']
        
        # 配置参数
        self.config = {
            'slice_match_threshold': 0.3,      # 切片匹配阈值
            'keyword_match_threshold': 0.5,    # 关键词匹配阈值
            'final_similarity_threshold': 0.75, # 最终相似度阈值
            'max_candidates': 50,               # 最大候选数量
            'use_cache': True,                  # 启用缓存
            'cache_ttl': 7 * 24 * 3600         # 缓存过期时间（7天）
        }
        
        # 性能统计
        self.stats = {
            'slice_queries': 0,
            'keyword_queries': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'total_matches': 0
        }
        
        logger.info("🚀 修复版切片增强匹配器初始化完成")
    
    def calculate_similarity(self, source_name: str, target_name: str) -> float:
        """
        计算两个字符串的相似度（修复版本）
        
        Args:
            source_name: 源字符串
            target_name: 目标字符串
            
        Returns:
            float: 相似度分数 (0.0-1.0)
        """
        try:
            if not source_name or not target_name:
                return 0.0
            
            # 转换为字符串确保类型正确
            source_str = str(source_name).strip()
            target_str = str(target_name).strip()
            
            if not source_str or not target_str:
                return 0.0
            
            # 使用多种算法计算相似度
            basic_similarity = fuzz.ratio(source_str, target_str) / 100.0
            token_similarity = fuzz.token_sort_ratio(source_str, target_str) / 100.0
            partial_similarity = fuzz.partial_ratio(source_str, target_str) / 100.0
            
            # 加权平均
            weighted_score = (
                basic_similarity * 0.4 +
                token_similarity * 0.4 +
                partial_similarity * 0.2
            )
            
            return min(max(weighted_score, 0.0), 1.0)  # 确保结果在0-1之间
            
        except Exception as e:
            logger.warning(f"相似度计算失败: {source_name} vs {target_name}, 错误: {e}")
            return 0.0
    
    def create_ngram_slices(self, text: str, n: int = 3) -> Set[str]:
        """创建N-gram切片"""
        if not text or len(text) < n:
            return set()
        
        # 清理文本
        clean_text = re.sub(r'[^\u4e00-\u9fff\w]', '', text)
        
        # 生成N-gram切片
        slices = set()
        for i in range(len(clean_text) - n + 1):
            slice_text = clean_text[i:i+n]
            if len(slice_text) == n:
                slices.add(slice_text)
        
        return slices
    
    def extract_keywords(self, text: str) -> Set[str]:
        """提取关键词"""
        if not text:
            return set()
        
        # 使用jieba分词
        words = jieba.cut(text)
        keywords = set()
        
        for word in words:
            word = word.strip()
            # 过滤掉长度小于2的词和常见停用词
            if len(word) >= 2 and word not in {'有限', '公司', '企业', '集团', '工厂', '商店', '中心'}:
                keywords.add(word)
        
        return keywords
    
    def find_candidates_by_slices(self, source_name: str, limit: int = 30) -> List[Dict]:
        """通过切片查找候选记录（简化版本）"""
        try:
            # 生成切片
            slices_3 = self.create_ngram_slices(source_name, 3)
            slices_2 = self.create_ngram_slices(source_name, 2)
            all_slices = slices_3.union(slices_2)
            
            if not all_slices:
                return []
            
            # 简化查询：直接从目标集合中查找包含关键词的记录
            keywords = self.extract_keywords(source_name)
            if not keywords:
                return []
            
            # 使用正则表达式查找包含关键词的记录
            query_conditions = []
            for keyword in list(keywords)[:5]:  # 限制关键词数量
                query_conditions.append({'dwmc': {'$regex': keyword, '$options': 'i'}})
            
            if query_conditions:
                query = {'$or': query_conditions}
                candidates = list(self.target_collection.find(
                    query,
                    {'dwmc': 1, 'dwdz': 1, 'tyshxydm': 1, 'fddbr': 1}
                ).limit(limit))
                
                # 计算相似度分数
                for candidate in candidates:
                    target_name = candidate.get('dwmc', '')
                    if target_name:
                        candidate['slice_score'] = self.calculate_similarity(source_name, target_name)
                
                # 按相似度排序
                candidates.sort(key=lambda x: x.get('slice_score', 0), reverse=True)
                return candidates[:limit]
            
            return []
            
        except Exception as e:
            logger.error(f"切片查询失败: {e}")
            return []
    
    def find_best_matches(self, source_name: str, limit: int = 10) -> List[Dict]:
        """查找最佳匹配（主要接口）"""
        try:
            candidates = self.find_candidates_by_slices(source_name, limit * 3)
            
            if not candidates:
                return []
            
            # 重新计算相似度并排序
            results = []
            for candidate in candidates:
                target_name = candidate.get('dwmc', '')
                if target_name:
                    similarity = self.calculate_similarity(source_name, target_name)
                    if similarity >= 0.5:  # 只保留相似度较高的结果
                        candidate['similarity'] = similarity
                        results.append(candidate)
            
            # 按相似度排序
            results.sort(key=lambda x: x.get('similarity', 0), reverse=True)
            return results[:limit]
            
        except Exception as e:
            logger.error(f"查找最佳匹配失败: {e}")
            return []
    
    def get_stats(self) -> Dict:
        """获取性能统计"""
        return self.stats.copy()
