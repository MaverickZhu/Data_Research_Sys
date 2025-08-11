"""
优化的预过滤系统
专门用于用户数据匹配的高性能预筛选
"""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple
import re
import jieba
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from collections import defaultdict
import pymongo

logger = logging.getLogger(__name__)


class OptimizedPrefilter:
    """优化的预过滤系统"""
    
    def __init__(self, db_manager, mappings: List[Dict]):
        """
        初始化优化预过滤系统
        
        Args:
            db_manager: 数据库管理器
            mappings: 字段映射配置
        """
        self.db_manager = db_manager
        self.db = db_manager.mongo_client.get_database() if db_manager and db_manager.mongo_client else None
        self.mappings = mappings
        
        # 构建字段映射索引
        self.field_mapping_index = self._build_field_mapping_index(mappings)
        
        # 预过滤配置
        self.config = {
            'max_candidates_per_field': 100,  # 每个字段最多返回的候选数
            'max_total_candidates': 200,      # 总候选数上限
            'similarity_threshold': 0.3,      # 预过滤相似度阈值
            'enable_parallel': True,          # 启用并行处理
            'thread_count': 4,               # 线程数
            'enable_cache': True,            # 启用缓存
            'cache_size': 1000,              # 缓存大小
            'text_search_limit': 50,         # 文本搜索限制
            'enable_fuzzy_prefilter': True,  # 启用模糊预过滤
        }
        
        # 缓存系统
        self.candidate_cache = {}
        self.cache_lock = threading.Lock()
        
        # 性能统计
        self.stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'avg_candidates': 0,
            'avg_query_time': 0,
            'field_performance': defaultdict(list)
        }
        
        logger.info(f"优化预过滤系统初始化完成，字段映射数量: {len(mappings)}")
    
    def _build_field_mapping_index(self, mappings: List[Dict]) -> Dict[str, Dict]:
        """构建字段映射索引"""
        index = {}
        
        for mapping in mappings:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            target_table = mapping['target_table']
            
            index[source_field] = {
                'target_field': target_field,
                'target_table': target_table,
                'similarity_score': mapping.get('similarity_score', 1.0),
                'data_type': mapping.get('data_type', 'auto')
            }
        
        return index
    
    def get_optimized_candidates(self, source_record: Dict, source_table: str) -> List[Dict]:
        """
        获取优化的候选记录
        
        Args:
            source_record: 源记录
            source_table: 源表名
            
        Returns:
            List[Dict]: 候选记录列表
        """
        if self.db is None:
            logger.error("数据库连接未初始化")
            return []
        
        start_time = time.time()
        self.stats['total_queries'] += 1
        
        # 生成缓存键
        cache_key = self._generate_cache_key(source_record)
        
        # 检查缓存
        if self.config['enable_cache'] and cache_key in self.candidate_cache:
            with self.cache_lock:
                if cache_key in self.candidate_cache:
                    self.stats['cache_hits'] += 1
                    logger.debug(f"缓存命中: {cache_key}")
                    return self.candidate_cache[cache_key]
        
        try:
            # 获取候选记录
            candidates = self._fetch_candidates_parallel(source_record) if self.config['enable_parallel'] else self._fetch_candidates_serial(source_record)
            
            # 去重和排序
            unique_candidates = self._deduplicate_and_rank_candidates(candidates, source_record)
            
            # 限制候选数量
            final_candidates = unique_candidates[:self.config['max_total_candidates']]
            
            # 更新缓存
            if self.config['enable_cache'] and len(self.candidate_cache) < self.config['cache_size']:
                with self.cache_lock:
                    self.candidate_cache[cache_key] = final_candidates
            
            # 更新统计
            query_time = time.time() - start_time
            self.stats['avg_candidates'] = (self.stats['avg_candidates'] * (self.stats['total_queries'] - 1) + len(final_candidates)) / self.stats['total_queries']
            self.stats['avg_query_time'] = (self.stats['avg_query_time'] * (self.stats['total_queries'] - 1) + query_time) / self.stats['total_queries']
            
            logger.info(f"预过滤完成: 源记录={source_record.get('_id', 'unknown')}, "
                       f"候选数={len(final_candidates)}, 耗时={query_time:.3f}s")
            
            return final_candidates
            
        except Exception as e:
            logger.error(f"预过滤失败: {str(e)}")
            return []
    
    def _fetch_candidates_parallel(self, source_record: Dict) -> List[Dict]:
        """并行获取候选记录"""
        all_candidates = []
        
        with ThreadPoolExecutor(max_workers=self.config['thread_count']) as executor:
            # 为每个映射字段提交任务
            future_to_field = {}
            
            for source_field, mapping_info in self.field_mapping_index.items():
                if source_field in source_record:
                    future = executor.submit(
                        self._fetch_candidates_for_field,
                        source_record,
                        source_field,
                        mapping_info
                    )
                    future_to_field[future] = source_field
            
            # 收集结果
            for future in as_completed(future_to_field):
                source_field = future_to_field[future]
                try:
                    candidates = future.result()
                    all_candidates.extend(candidates)
                    
                    # 记录字段性能
                    self.stats['field_performance'][source_field].append(len(candidates))
                    
                except Exception as e:
                    logger.warning(f"字段 {source_field} 候选获取失败: {str(e)}")
        
        return all_candidates
    
    def _fetch_candidates_serial(self, source_record: Dict) -> List[Dict]:
        """串行获取候选记录"""
        all_candidates = []
        
        for source_field, mapping_info in self.field_mapping_index.items():
            if source_field in source_record:
                try:
                    candidates = self._fetch_candidates_for_field(
                        source_record, source_field, mapping_info
                    )
                    all_candidates.extend(candidates)
                    
                    # 记录字段性能
                    self.stats['field_performance'][source_field].append(len(candidates))
                    
                except Exception as e:
                    logger.warning(f"字段 {source_field} 候选获取失败: {str(e)}")
        
        return all_candidates
    
    def _fetch_candidates_for_field(self, source_record: Dict, source_field: str, 
                                  mapping_info: Dict) -> List[Dict]:
        """为单个字段获取候选记录"""
        source_value = source_record.get(source_field)
        if not source_value or not str(source_value).strip():
            return []
        
        target_table = mapping_info['target_table']
        target_field = mapping_info['target_field']
        
        collection = self.db[target_table]
        candidates = []
        
        try:
            # 1. 精确匹配
            exact_candidates = self._get_exact_match_candidates(
                collection, target_field, source_value
            )
            candidates.extend(exact_candidates)
            
            # 2. 文本搜索匹配
            if len(candidates) < self.config['max_candidates_per_field']:
                text_candidates = self._get_text_search_candidates(
                    collection, target_field, source_value
                )
                candidates.extend(text_candidates)
            
            # 3. 模糊匹配（如果启用且候选数量不足）
            if (self.config['enable_fuzzy_prefilter'] and 
                len(candidates) < self.config['max_candidates_per_field'] // 2):
                fuzzy_candidates = self._get_fuzzy_match_candidates(
                    collection, target_field, source_value
                )
                candidates.extend(fuzzy_candidates)
            
        except Exception as e:
            logger.warning(f"字段 {source_field} 候选获取异常: {str(e)}")
        
        return candidates[:self.config['max_candidates_per_field']]
    
    def _get_exact_match_candidates(self, collection, target_field: str, 
                                  source_value: Any) -> List[Dict]:
        """获取精确匹配候选"""
        try:
            # 精确匹配查询
            query = {target_field: source_value}
            candidates = list(collection.find(query).limit(20))
            
            logger.debug(f"精确匹配 {target_field}='{source_value}': {len(candidates)} 个候选")
            return candidates
            
        except Exception as e:
            logger.debug(f"精确匹配查询失败: {str(e)}")
            return []
    
    def _get_text_search_candidates(self, collection, target_field: str, 
                                  source_value: Any) -> List[Dict]:
        """获取文本搜索候选"""
        try:
            source_str = str(source_value).strip()
            if len(source_str) < 2:
                return []
            
            # 文本搜索查询
            query = {'$text': {'$search': source_str}}
            candidates = list(
                collection.find(query, {'score': {'$meta': 'textScore'}})
                .sort([('score', {'$meta': 'textScore'})])
                .limit(self.config['text_search_limit'])
            )
            
            logger.debug(f"文本搜索 '{source_str}': {len(candidates)} 个候选")
            return candidates
            
        except Exception as e:
            logger.debug(f"文本搜索查询失败: {str(e)}")
            return []
    
    def _get_fuzzy_match_candidates(self, collection, target_field: str, 
                                  source_value: Any) -> List[Dict]:
        """获取模糊匹配候选"""
        try:
            source_str = str(source_value).strip()
            if len(source_str) < 3:
                return []
            
            # 使用正则表达式进行模糊匹配
            # 提取关键词
            keywords = self._extract_keywords(source_str)
            if not keywords:
                return []
            
            # 构建正则查询
            regex_patterns = []
            for keyword in keywords[:3]:  # 限制关键词数量
                if len(keyword) >= 2:
                    escaped_keyword = re.escape(keyword)
                    regex_patterns.append(escaped_keyword)
            
            if regex_patterns:
                regex_query = '|'.join(regex_patterns)
                query = {target_field: {'$regex': regex_query, '$options': 'i'}}
                
                candidates = list(collection.find(query).limit(30))
                
                logger.debug(f"模糊匹配 '{source_str}': {len(candidates)} 个候选")
                return candidates
            
        except Exception as e:
            logger.debug(f"模糊匹配查询失败: {str(e)}")
        
        return []
    
    def _extract_keywords(self, text: str) -> List[str]:
        """提取关键词"""
        try:
            # 使用jieba分词
            words = jieba.lcut(text)
            
            # 过滤短词和停用词
            keywords = []
            for word in words:
                word = word.strip()
                if (len(word) >= 2 and 
                    word not in ['有限', '公司', '责任', '股份', '集团', '企业', '商贸', '贸易'] and
                    not word.isdigit()):
                    keywords.append(word)
            
            return keywords[:5]  # 返回前5个关键词
            
        except Exception as e:
            logger.debug(f"关键词提取失败: {str(e)}")
            return []
    
    def _deduplicate_and_rank_candidates(self, candidates: List[Dict], 
                                       source_record: Dict) -> List[Dict]:
        """去重并排序候选记录"""
        # 使用_id去重
        seen_ids = set()
        unique_candidates = []
        
        for candidate in candidates:
            candidate_id = candidate.get('_id')
            if candidate_id and candidate_id not in seen_ids:
                seen_ids.add(candidate_id)
                unique_candidates.append(candidate)
        
        # 简单排序：优先考虑有文本搜索分数的记录
        def sort_key(candidate):
            score = candidate.get('score', 0)
            return score if score > 0 else 0
        
        unique_candidates.sort(key=sort_key, reverse=True)
        
        return unique_candidates
    
    def _generate_cache_key(self, source_record: Dict) -> str:
        """生成缓存键"""
        key_parts = []
        
        for source_field in self.field_mapping_index.keys():
            value = source_record.get(source_field, '')
            if value:
                key_parts.append(f"{source_field}:{str(value)[:50]}")
        
        return '|'.join(key_parts)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        stats = dict(self.stats)
        
        # 计算字段平均性能
        field_avg_performance = {}
        for field, performances in self.stats['field_performance'].items():
            if performances:
                field_avg_performance[field] = {
                    'avg_candidates': sum(performances) / len(performances),
                    'query_count': len(performances),
                    'max_candidates': max(performances),
                    'min_candidates': min(performances)
                }
        
        stats['field_avg_performance'] = field_avg_performance
        stats['cache_hit_rate'] = (self.stats['cache_hits'] / max(self.stats['total_queries'], 1)) * 100
        
        return stats
    
    def clear_cache(self):
        """清空缓存"""
        with self.cache_lock:
            self.candidate_cache.clear()
            logger.info("预过滤缓存已清空")
    
    def update_config(self, new_config: Dict[str, Any]):
        """更新配置"""
        self.config.update(new_config)
        logger.info(f"预过滤配置已更新: {new_config}")


class CandidateRanker:
    """候选记录排序器"""
    
    def __init__(self, mappings: List[Dict]):
        self.mappings = mappings
        self.field_weights = self._calculate_field_weights(mappings)
    
    def _calculate_field_weights(self, mappings: List[Dict]) -> Dict[str, float]:
        """计算字段权重"""
        weights = {}
        
        for mapping in mappings:
            source_field = mapping['source_field']
            similarity_score = mapping.get('similarity_score', 0.5)
            
            # 根据相似度分数设置权重
            if similarity_score >= 0.8:
                weight = 1.0
            elif similarity_score >= 0.6:
                weight = 0.8
            elif similarity_score >= 0.4:
                weight = 0.6
            else:
                weight = 0.4
            
            weights[source_field] = weight
        
        return weights
    
    def rank_candidates(self, candidates: List[Dict], source_record: Dict) -> List[Dict]:
        """对候选记录进行排序"""
        if not candidates:
            return []
        
        # 计算每个候选记录的得分
        scored_candidates = []
        
        for candidate in candidates:
            score = self._calculate_candidate_score(candidate, source_record)
            scored_candidates.append((score, candidate))
        
        # 按得分排序
        scored_candidates.sort(key=lambda x: x[0], reverse=True)
        
        # 返回排序后的候选记录
        return [candidate for score, candidate in scored_candidates]
    
    def _calculate_candidate_score(self, candidate: Dict, source_record: Dict) -> float:
        """计算候选记录得分"""
        total_score = 0.0
        total_weight = 0.0
        
        for mapping in self.mappings:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            
            source_value = source_record.get(source_field, '')
            target_value = candidate.get(target_field, '')
            
            if source_value and target_value:
                field_score = self._calculate_field_similarity(
                    str(source_value), str(target_value)
                )
                weight = self.field_weights.get(source_field, 0.5)
                
                total_score += field_score * weight
                total_weight += weight
        
        # 返回加权平均得分
        return total_score / max(total_weight, 0.1)
    
    def _calculate_field_similarity(self, source_value: str, target_value: str) -> float:
        """计算字段相似度（简化版）"""
        if source_value == target_value:
            return 1.0
        
        if not source_value or not target_value:
            return 0.0
        
        # 简单的字符串相似度计算
        source_set = set(source_value)
        target_set = set(target_value)
        
        intersection = len(source_set.intersection(target_set))
        union = len(source_set.union(target_set))
        
        return intersection / max(union, 1)
