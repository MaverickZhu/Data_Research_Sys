#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于切片索引的增强模糊匹配器
利用N-gram切片和关键词索引实现超高速模糊匹配
"""

import pymongo
import re
import jieba
import time
from typing import Dict, List, Set, Tuple, Optional
from rapidfuzz import fuzz, process
from collections import defaultdict, Counter
import logging

logger = logging.getLogger(__name__)

class SliceEnhancedMatcher:
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
        
        logger.info("🚀 切片增强匹配器初始化完成")
    
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
    
    def find_candidates_by_slices(self, source_name: str) -> List[Dict]:
        """通过切片查找候选记录"""
        try:
            # 生成3-gram和2-gram切片
            slices_3 = self.create_ngram_slices(source_name, 3)
            slices_2 = self.create_ngram_slices(source_name, 2)
            all_slices = slices_3.union(slices_2)
            
            if not all_slices:
                return []
            
            # 查询切片索引
            self.stats['slice_queries'] += 1
            
            # 使用聚合管道统计切片匹配度
            pipeline = [
                {'$match': {'slice': {'$in': list(all_slices)}}},
                {'$group': {
                    '_id': '$doc_id',
                    'unit_name': {'$first': '$unit_name'},
                    'match_count': {'$sum': 1},
                    'matched_slices': {'$push': '$slice'}
                }},
                {'$addFields': {
                    'slice_score': {'$divide': ['$match_count', len(all_slices)]}
                }},
                {'$match': {'slice_score': {'$gte': self.config['slice_match_threshold']}}},
                {'$sort': {'slice_score': -1}},
                {'$limit': self.config['max_candidates']}
            ]
            
            candidates = list(self.target_slice_collection.aggregate(pipeline))
            
            # 获取完整记录
            if candidates:
                doc_ids = [candidate['_id'] for candidate in candidates]
                full_records = list(self.target_collection.find(
                    {'_id': {'$in': doc_ids}},
                    {'dwmc': 1, 'dwdz': 1, 'tyshxydm': 1, 'fddbr': 1, 'xfaqglr': 1}
                ))
                
                # 合并切片信息和完整记录
                record_map = {record['_id']: record for record in full_records}
                result = []
                
                for candidate in candidates:
                    doc_id = candidate['_id']
                    if doc_id in record_map:
                        record = record_map[doc_id].copy()
                        record['slice_score'] = candidate['slice_score']
                        record['matched_slices'] = candidate['matched_slices']
                        result.append(record)
                
                return result
            
            return []
            
        except Exception as e:
            logger.error(f"切片查询失败: {e}")
            return []
    
    def find_candidates_by_keywords(self, source_name: str) -> List[Dict]:
        """通过关键词查找候选记录"""
        try:
            keywords = self.extract_keywords(source_name)
            if not keywords:
                return []
            
            self.stats['keyword_queries'] += 1
            
            # 使用聚合管道统计关键词匹配度
            pipeline = [
                {'$match': {'keyword': {'$in': list(keywords)}}},
                {'$group': {
                    '_id': '$doc_id',
                    'unit_name': {'$first': '$unit_name'},
                    'match_count': {'$sum': 1},
                    'matched_keywords': {'$push': '$keyword'}
                }},
                {'$addFields': {
                    'keyword_score': {'$divide': ['$match_count', len(keywords)]}
                }},
                {'$match': {'keyword_score': {'$gte': self.config['keyword_match_threshold']}}},
                {'$sort': {'keyword_score': -1}},
                {'$limit': self.config['max_candidates']}
            ]
            
            candidates = list(self.target_keyword_collection.aggregate(pipeline))
            
            # 获取完整记录
            if candidates:
                doc_ids = [candidate['_id'] for candidate in candidates]
                full_records = list(self.target_collection.find(
                    {'_id': {'$in': doc_ids}},
                    {'dwmc': 1, 'dwdz': 1, 'tyshxydm': 1, 'fddbr': 1, 'xfaqglr': 1}
                ))
                
                # 合并关键词信息和完整记录
                record_map = {record['_id']: record for record in full_records}
                result = []
                
                for candidate in candidates:
                    doc_id = candidate['_id']
                    if doc_id in record_map:
                        record = record_map[doc_id].copy()
                        record['keyword_score'] = candidate['keyword_score']
                        record['matched_keywords'] = candidate['matched_keywords']
                        result.append(record)
                
                return result
            
            return []
            
        except Exception as e:
            logger.error(f"关键词查询失败: {e}")
            return []
    
    def calculate_similarity(self, source_name: str, target_name: str) -> float:
        """
        计算两个字符串的相似度（简化版本）
        
        Args:
            source_name: 源字符串
            target_name: 目标字符串
            
        Returns:
            float: 相似度分数 (0.0-1.0)
        """
        if not source_name or not target_name:
            return 0.0
        
        # 使用多种算法计算相似度
        basic_similarity = fuzz.ratio(source_name, target_name) / 100.0
        token_similarity = fuzz.token_sort_ratio(source_name, target_name) / 100.0
        partial_similarity = fuzz.partial_ratio(source_name, target_name) / 100.0
        
        # 加权平均
        weighted_score = (
            basic_similarity * 0.4 +
            token_similarity * 0.4 +
            partial_similarity * 0.2
        )
        
        return weighted_score
    
    def calculate_enhanced_similarity(self, source_name: str, target_record: Dict) -> float:
        """计算增强相似度"""
        target_name = target_record.get('dwmc', '')
        if not target_name:
            return 0.0
        
        # 检查缓存
        if self.config['use_cache']:
            cache_key = f"{source_name}||{target_name}"
            cached_result = self.cache_collection.find_one({'source_name': source_name, 'target_name': target_name})
            
            if cached_result:
                self.stats['cache_hits'] += 1
                return cached_result.get('similarity_score', 0.0)
            else:
                self.stats['cache_misses'] += 1
        
        # 计算多维度相似度
        similarities = {}
        weights = {}
        
        # 1. 基础字符串相似度 (权重: 0.4)
        basic_similarity = fuzz.ratio(source_name, target_name) / 100.0
        similarities['basic'] = basic_similarity
        weights['basic'] = 0.4
        
        # 2. Token相似度 (权重: 0.3)
        token_similarity = fuzz.token_sort_ratio(source_name, target_name) / 100.0
        similarities['token'] = token_similarity
        weights['token'] = 0.3
        
        # 3. 切片相似度 (权重: 0.2)
        if 'slice_score' in target_record:
            similarities['slice'] = target_record['slice_score']
            weights['slice'] = 0.2
        else:
            # 实时计算切片相似度
            source_slices = self.create_ngram_slices(source_name, 3)
            target_slices = self.create_ngram_slices(target_name, 3)
            
            if source_slices and target_slices:
                intersection = source_slices.intersection(target_slices)
                union = source_slices.union(target_slices)
                slice_similarity = len(intersection) / len(union) if union else 0.0
                similarities['slice'] = slice_similarity
                weights['slice'] = 0.2
        
        # 4. 关键词相似度 (权重: 0.1)
        if 'keyword_score' in target_record:
            similarities['keyword'] = target_record['keyword_score']
            weights['keyword'] = 0.1
        else:
            # 实时计算关键词相似度
            source_keywords = self.extract_keywords(source_name)
            target_keywords = self.extract_keywords(target_name)
            
            if source_keywords and target_keywords:
                intersection = source_keywords.intersection(target_keywords)
                union = source_keywords.union(target_keywords)
                keyword_similarity = len(intersection) / len(union) if union else 0.0
                similarities['keyword'] = keyword_similarity
                weights['keyword'] = 0.1
        
        # 计算加权平均分数
        total_weight = sum(weights.values())
        if total_weight > 0:
            final_score = sum(similarities[key] * weights[key] for key in similarities) / total_weight
        else:
            final_score = basic_similarity
        
        # 缓存结果
        if self.config['use_cache']:
            try:
                self.cache_collection.insert_one({
                    'source_name': source_name,
                    'target_name': target_name,
                    'similarity_score': final_score,
                    'created_time': time.time()
                })
            except Exception:
                pass  # 忽略缓存插入错误
        
        return final_score
    
    def find_best_matches(self, source_record: Dict) -> List[Dict]:
        """查找最佳匹配"""
        source_name = source_record.get('UNIT_NAME', '')
        if not source_name:
            return []
        
        start_time = time.time()
        
        # 1. 通过切片查找候选
        slice_candidates = self.find_candidates_by_slices(source_name)
        
        # 2. 通过关键词查找候选
        keyword_candidates = self.find_candidates_by_keywords(source_name)
        
        # 3. 合并候选记录（去重）
        all_candidates = {}
        
        for candidate in slice_candidates:
            doc_id = candidate['_id']
            all_candidates[doc_id] = candidate
        
        for candidate in keyword_candidates:
            doc_id = candidate['_id']
            if doc_id in all_candidates:
                # 合并分数信息
                all_candidates[doc_id]['keyword_score'] = candidate.get('keyword_score', 0)
                all_candidates[doc_id]['matched_keywords'] = candidate.get('matched_keywords', [])
            else:
                all_candidates[doc_id] = candidate
        
        # 4. 计算相似度并排序
        scored_matches = []
        for candidate in all_candidates.values():
            similarity = self.calculate_enhanced_similarity(source_name, candidate)
            
            if similarity >= self.config['final_similarity_threshold']:
                candidate['similarity_score'] = similarity
                scored_matches.append(candidate)
        
        # 按相似度降序排序
        scored_matches.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        # 统计
        self.stats['total_matches'] += len(scored_matches)
        
        processing_time = time.time() - start_time
        logger.debug(f"切片增强匹配耗时: {processing_time:.4f}秒, 候选: {len(all_candidates)}, 匹配: {len(scored_matches)}")
        
        return scored_matches[:10]  # 返回前10个最佳匹配
    
    def get_performance_stats(self) -> Dict:
        """获取性能统计"""
        return {
            'slice_queries': self.stats['slice_queries'],
            'keyword_queries': self.stats['keyword_queries'],
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'cache_hit_rate': self.stats['cache_hits'] / (self.stats['cache_hits'] + self.stats['cache_misses']) if (self.stats['cache_hits'] + self.stats['cache_misses']) > 0 else 0,
            'total_matches': self.stats['total_matches']
        }
    
    def clear_cache(self):
        """清空缓存"""
        try:
            self.cache_collection.drop()
            logger.info("相似度缓存已清空")
        except Exception as e:
            logger.error(f"清空缓存失败: {e}") 