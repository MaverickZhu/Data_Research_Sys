#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用聚合查询引擎
使用统一的聚合管道模板进行高速查询，替换复杂的$or查询
支持批量查询、并行处理和智能缓存
"""

import logging
import time
from typing import Dict, List, Set, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import pymongo
from .universal_text_matcher import UniversalTextMatcher, FieldType
from .universal_index_builder import UniversalIndexBuilder
from .similarity_scorer import SimilarityCalculator

logger = logging.getLogger(__name__)


class QueryResult:
    """查询结果封装"""
    
    def __init__(self, candidates: List[Dict], query_time: float, 
                 source_record: Dict, field_info: Dict):
        self.candidates = candidates
        self.query_time = query_time
        self.source_record = source_record
        self.field_info = field_info
        self.candidate_count = len(candidates)


class UniversalQueryEngine:
    """通用聚合查询引擎"""
    
    def __init__(self, db_manager):
        """
        初始化通用查询引擎
        
        Args:
            db_manager: 数据库管理器
        """
        self.db_manager = db_manager
        self.db = db_manager.get_db() if db_manager else None
        self.text_matcher = UniversalTextMatcher(db_manager)
        self.index_builder = UniversalIndexBuilder(db_manager)
        
        # 初始化地址相似度计算器
        try:
            # 创建默认配置
            config = {
                'address_similarity': {
                    'enable_segmentation': True,
                    'province_weight': 0.05,
                    'city_weight': 0.10,
                    'district_weight': 0.15,
                    'detail_weight': 0.02
                },
                'string_similarity': {
                    'algorithms': [
                        {'name': 'levenshtein', 'weight': 0.3},
                        {'name': 'jaro_winkler', 'weight': 0.3},
                        {'name': 'cosine', 'weight': 0.4}
                    ]
                }
            }
            self.similarity_calculator = SimilarityCalculator(config)
            logger.info("地址相似度计算器初始化成功")
        except Exception as e:
            logger.warning(f"初始化地址相似度计算器失败: {e}")
            self.similarity_calculator = None
        
        # 查询配置（高性能优化）
        self.query_config = {
            'enable_batch_query': True,
            'batch_size': 500,  # 增加批量大小，减少数据库查询次数
            'max_workers': 32,  # 增加并发数
            'enable_parallel': True,
            'enable_cache': True,
            'cache_ttl': 3600,
            'default_similarity_threshold': 0.6,
            'max_candidates_per_field': 30,  # 减少候选数量，提升速度
            'query_timeout': 60.0,  # 增加超时时间
            'enable_auto_index_creation': True,
            'enable_fast_mode': True  # 启用快速模式
        }
        
        # 缓存系统
        self.query_cache = {}
        self.pipeline_cache = {}
        
        # 性能统计
        self.query_stats = {
            'total_queries': 0,
            'batch_queries': 0,
            'cache_hits': 0,
            'avg_query_time': 0.0,
            'avg_candidates_per_query': 0.0,
            'total_candidates_found': 0,
            'auto_indexes_created': 0
        }
        
        logger.info("🚀 通用聚合查询引擎初始化完成")
    
    def query_single_field(self, source_value: str, target_table: str, 
                          target_field: str, field_type: FieldType = None,
                          similarity_threshold: float = None) -> QueryResult:
        """
        单字段查询
        
        Args:
            source_value: 源值
            target_table: 目标表名
            target_field: 目标字段名
            field_type: 字段类型
            similarity_threshold: 相似度阈值
            
        Returns:
            QueryResult: 查询结果
        """
        start_time = time.time()
        self.query_stats['total_queries'] += 1
        
        try:
            # 自动检测字段类型
            if field_type is None:
                field_type = self.text_matcher.detect_field_type(target_field, [source_value])
            
            # 获取配置
            config = self.text_matcher.field_configs.get(field_type, 
                                                       self.text_matcher.field_configs[FieldType.TEXT])
            
            # 使用配置的阈值或默认阈值
            if similarity_threshold is None:
                similarity_threshold = config.similarity_threshold
            
            # 预处理和关键词提取
            preprocessed_value = self.text_matcher._apply_preprocessing(source_value, config)
            if not preprocessed_value:
                return QueryResult([], time.time() - start_time, 
                                 {'original_value': source_value}, 
                                 {'field_type': field_type.value, 'reason': 'empty_after_preprocessing'})
            
            keywords = self.text_matcher._apply_keyword_extraction(preprocessed_value, config)
            if not keywords:
                return QueryResult([], time.time() - start_time,
                                 {'original_value': source_value, 'preprocessed_value': preprocessed_value},
                                 {'field_type': field_type.value, 'reason': 'no_keywords_extracted'})
            
            # 执行查询
            candidates = self._execute_single_field_query(
                target_table, target_field, keywords, config, similarity_threshold
            )
            
            # 更新统计
            query_time = time.time() - start_time
            self._update_query_stats(query_time, len(candidates))
            
            return QueryResult(
                candidates, query_time,
                {'original_value': source_value, 'preprocessed_value': preprocessed_value, 'keywords': keywords},
                {'field_type': field_type.value, 'similarity_threshold': similarity_threshold}
            )
            
        except Exception as e:
            logger.error(f"单字段查询失败: {target_table}.{target_field} - {str(e)}")
            return QueryResult([], time.time() - start_time,
                             {'original_value': source_value},
                             {'field_type': field_type.value if field_type else 'unknown', 'error': str(e)})
    
    def query_batch_records(self, batch_records: List[Dict], mappings: List[Dict]) -> Dict[str, QueryResult]:
        """
        批量记录查询 - 这是替换58秒瓶颈的核心方法
        
        Args:
            batch_records: 批量源记录
            mappings: 字段映射配置
            
        Returns:
            Dict[str, QueryResult]: 记录ID到查询结果的映射
        """
        start_time = time.time()
        self.query_stats['batch_queries'] += 1
        
        try:
            logger.info(f"🚀 开始批量查询: {len(batch_records)} 条记录, {len(mappings)} 个映射")
            
            # 按目标表和字段分组映射
            grouped_mappings = self._group_mappings_by_target(mappings)
            
            # 构建批量查询计划
            query_plan = self._build_batch_query_plan(batch_records, grouped_mappings)
            
            # 执行批量查询
            if self.query_config['enable_parallel'] and len(query_plan) > 1:
                batch_results = self._execute_batch_queries_parallel(query_plan)
            else:
                batch_results = self._execute_batch_queries_sequential(query_plan)
            
            # 合并结果
            final_results = self._merge_batch_results(batch_records, batch_results, mappings)
            
            # 更新统计
            batch_time = time.time() - start_time
            total_candidates = sum(len(result.candidates) for result in final_results.values())
            
            logger.info(f"✅ 批量查询完成: {len(final_results)} 个结果, "
                       f"总候选数: {total_candidates}, 耗时: {batch_time:.3f}s, "
                       f"平均速度: {len(batch_records) / max(batch_time, 0.001):.1f} 条/秒")
            
            return final_results
            
        except Exception as e:
            logger.error(f"批量查询失败: {str(e)}")
            return {}
    
    def _execute_single_field_query(self, target_table: str, target_field: str, 
                                   keywords: List[str], config, similarity_threshold: float) -> List[Dict]:
        """执行单字段聚合查询"""
        try:
            # 构建索引表名（根据字段名动态生成）
            index_table_name = f"{target_table}_{target_field}_keywords"
            
            # 检查索引表是否存在
            if index_table_name not in self.db.list_collection_names():
                if self.query_config['enable_auto_index_creation']:
                    logger.info(f"索引表不存在，自动创建: {index_table_name}")
                    self.index_builder.build_field_index(target_table, target_field)
                    self.query_stats['auto_indexes_created'] += 1
                else:
                    logger.warning(f"索引表不存在: {index_table_name}")
                    return []
            
            # 获取聚合管道
            pipeline = self._build_aggregation_pipeline(
                target_field, keywords, similarity_threshold, config.max_candidates
            )
            
            # 执行聚合查询
            index_collection = self.db[index_table_name]
            aggregation_results = list(index_collection.aggregate(pipeline))
            
            if not aggregation_results:
                return []
            
            # 获取完整记录
            return self._fetch_full_records(target_table, aggregation_results)
            
        except Exception as e:
            logger.error(f"单字段查询执行失败: {target_table}.{target_field} - {str(e)}")
            return []
    
    def _build_aggregation_pipeline(self, target_field: str, keywords: List[str], 
                                   similarity_threshold: float, max_candidates: int) -> List[Dict]:
        """构建聚合管道"""
        # 缓存检查
        cache_key = f"{target_field}_{len(keywords)}_{similarity_threshold}_{max_candidates}"
        if cache_key in self.pipeline_cache:
            pipeline_template = self.pipeline_cache[cache_key]
        else:
            pipeline_template = [
                # 第1阶段：匹配关键词
                {'$match': {
                    'keyword': {'$in': keywords},
                    'field_name': target_field
                }},
                # 第2阶段：按文档ID分组统计
                {'$group': {
                    '_id': '$doc_id',
                    'original_value': {'$first': '$original_value'},
                    'match_count': {'$sum': 1},
                    'matched_keywords': {'$push': '$keyword'}
                }},
                # 第3阶段：计算相似度得分
                {'$addFields': {
                    'similarity_score': {'$divide': ['$match_count', len(keywords)]}
                }},
                # 第4阶段：过滤低分候选
                {'$match': {
                    'similarity_score': {'$gte': similarity_threshold}
                }},
                # 第5阶段：按得分排序
                {'$sort': {'similarity_score': -1}},
                # 第6阶段：限制候选数量
                {'$limit': max_candidates}
            ]
            self.pipeline_cache[cache_key] = pipeline_template
        
        # 动态替换关键词（避免缓存污染）
        pipeline = []
        for stage in pipeline_template:
            if '$match' in stage and 'keyword' in stage['$match']:
                stage_copy = stage.copy()
                stage_copy['$match'] = stage['$match'].copy()
                stage_copy['$match']['keyword'] = {'$in': keywords}
                pipeline.append(stage_copy)
            elif '$addFields' in stage:
                stage_copy = stage.copy()
                stage_copy['$addFields'] = stage['$addFields'].copy()
                stage_copy['$addFields']['similarity_score'] = {'$divide': ['$match_count', len(keywords)]}
                pipeline.append(stage_copy)
            else:
                pipeline.append(stage)
        
        return pipeline
    
    def _fetch_full_records(self, target_table: str, aggregation_results: List[Dict]) -> List[Dict]:
        """获取完整记录"""
        try:
            if not aggregation_results:
                return []
            
            # 提取文档ID并转换为ObjectId
            from bson import ObjectId
            doc_ids = []
            for result in aggregation_results:
                doc_id = result['_id']
                # 如果是字符串，转换为ObjectId
                if isinstance(doc_id, str):
                    try:
                        doc_ids.append(ObjectId(doc_id))
                    except Exception as e:
                        logger.warning(f"无效的ObjectId字符串: {doc_id} - {e}")
                        continue
                else:
                    doc_ids.append(doc_id)
            
            # 批量查询完整记录
            target_collection = self.db[target_table]
            full_records = list(target_collection.find({'_id': {'$in': doc_ids}}))
            
            # 合并聚合结果和完整记录
            record_map = {record['_id']: record for record in full_records}
            candidates = []
            
            for agg_result in aggregation_results:
                doc_id = agg_result['_id']
                
                # 统一ID格式进行匹配
                if isinstance(doc_id, str):
                    try:
                        lookup_id = ObjectId(doc_id)
                    except:
                        continue
                else:
                    lookup_id = doc_id
                
                if lookup_id in record_map:
                    record = record_map[lookup_id].copy()
                    record['similarity_score'] = agg_result['similarity_score']
                    record['_matched_keywords'] = agg_result['matched_keywords']
                    record['_original_value'] = agg_result['original_value']
                    candidates.append(record)
            
            return candidates
            
        except Exception as e:
            logger.error(f"获取完整记录失败: {target_table} - {str(e)}")
            return []
    
    def _fetch_full_records_batch(self, target_table: str, aggregation_results: List[Dict]) -> List[Dict]:
        """获取完整记录（批量查询版本）"""
        try:
            if not aggregation_results:
                return []
            
            # 提取文档ID并转换为ObjectId
            from bson import ObjectId
            doc_ids = []
            for result in aggregation_results:
                doc_id = result['_id']
                # 如果是字符串，转换为ObjectId
                if isinstance(doc_id, str):
                    try:
                        doc_ids.append(ObjectId(doc_id))
                    except Exception as e:
                        logger.warning(f"无效的ObjectId字符串: {doc_id} - {e}")
                        continue
                else:
                    doc_ids.append(doc_id)
            
            # 批量查询完整记录
            target_collection = self.db[target_table]
            full_records = list(target_collection.find({'_id': {'$in': doc_ids}}))
            
            # 合并聚合结果和完整记录
            record_map = {record['_id']: record for record in full_records}
            candidates = []
            
            for agg_result in aggregation_results:
                doc_id = agg_result['_id']
                
                # 统一ID格式进行匹配
                if isinstance(doc_id, str):
                    try:
                        lookup_id = ObjectId(doc_id)
                    except:
                        continue
                else:
                    lookup_id = doc_id
                
                if lookup_id in record_map:
                    record = record_map[lookup_id].copy()
                    # 【关键修复】使用聚合结果中的实际字段
                    record['_keyword_count'] = agg_result.get('match_count', 0)
                    record['_matched_keywords'] = agg_result.get('matched_keywords', [])
                    record['_original_value'] = agg_result.get('original_value', '')
                    # 【关键修复】不在此处计算相似度，留给后续的_match_candidates_to_records方法处理
                    # 相似度计算需要源记录的关键词信息，这里无法获取
                    # record['similarity_score'] 将在_match_candidates_to_records中正确计算
                    candidates.append(record)
            
            return candidates
            
        except Exception as e:
            logger.error(f"批量获取完整记录失败: {target_table} - {str(e)}")
            return []
    
    def _group_mappings_by_target(self, mappings: List[Dict]) -> Dict[Tuple[str, str], List[Dict]]:
        """按目标表和字段分组映射"""
        grouped = defaultdict(list)
        
        for mapping in mappings:
            target_table = mapping.get('target_table')
            target_field = mapping.get('target_field')
            if target_table and target_field:
                key = (target_table, target_field)
                grouped[key].append(mapping)
        
        return dict(grouped)
    
    def _build_batch_query_plan(self, batch_records: List[Dict], 
                               grouped_mappings: Dict[Tuple[str, str], List[Dict]]) -> List[Dict]:
        """构建批量查询计划"""
        query_plan = []
        
        for (target_table, target_field), mapping_list in grouped_mappings.items():
            # 收集所有源值
            source_values = []
            record_mapping = {}
            
            for record in batch_records:
                record_id = str(record.get('_id', ''))
                for mapping in mapping_list:
                    source_field = mapping.get('source_field')
                    source_value = record.get(source_field)
                    
                    if source_value:
                        source_values.append({
                            'record_id': record_id,
                            'source_value': str(source_value),
                            'source_field': source_field,
                            'mapping': mapping
                        })
                        record_mapping[record_id] = record
            
            if source_values:
                query_plan.append({
                    'target_table': target_table,
                    'target_field': target_field,
                    'source_values': source_values,
                    'record_mapping': record_mapping
                })
        
        return query_plan
    
    def _execute_batch_queries_parallel(self, query_plan: List[Dict]) -> List[Dict]:
        """并行执行批量查询"""
        results = []
        
        try:
            with ThreadPoolExecutor(max_workers=self.query_config['max_workers']) as executor:
                # 检查解释器状态
                import sys
                if hasattr(sys, '_getframe') and sys.is_finalizing():
                    logger.warning("解释器正在关闭，降级到顺序执行")
                    return self._execute_batch_queries_sequential(query_plan)
                
                future_to_plan = {}
                
                # 安全地提交任务
                for plan in query_plan:
                    try:
                        future = executor.submit(self._execute_single_batch_query, plan)
                        future_to_plan[future] = plan
                    except RuntimeError as e:
                        if "cannot schedule new futures after interpreter shutdown" in str(e):
                            logger.warning("检测到解释器关闭，停止提交新任务")
                            break
                        else:
                            raise
                
                # 处理已提交的任务
                for future in as_completed(future_to_plan):
                    plan = future_to_plan[future]
                    try:
                        result = future.result(timeout=self.query_config['query_timeout'])
                        results.append(result)
                    except Exception as e:
                        error_msg = str(e)
                        if "cannot schedule new futures after interpreter shutdown" in error_msg:
                            logger.warning(f"任务执行中断（解释器关闭）: {plan['target_table']}.{plan['target_field']}")
                        else:
                            logger.error(f"并行批量查询失败: {plan['target_table']}.{plan['target_field']} - {error_msg}")
                        
                        results.append({
                            'target_table': plan['target_table'],
                            'target_field': plan['target_field'],
                            'status': 'error',
                            'error': error_msg,
                            'record_results': {}
                        })
        
        except RuntimeError as e:
            if "cannot schedule new futures after interpreter shutdown" in str(e):
                logger.warning("ThreadPoolExecutor创建失败（解释器关闭），降级到顺序执行")
                return self._execute_batch_queries_sequential(query_plan)
            else:
                raise
        except Exception as e:
            logger.error(f"并行执行框架异常: {str(e)}")
            # 降级到顺序执行
            return self._execute_batch_queries_sequential(query_plan)
        
        return results
    
    def _execute_batch_queries_sequential(self, query_plan: List[Dict]) -> List[Dict]:
        """顺序执行批量查询"""
        results = []
        
        for plan in query_plan:
            try:
                result = self._execute_single_batch_query(plan)
                results.append(result)
            except Exception as e:
                logger.error(f"顺序批量查询失败: {plan['target_table']}.{plan['target_field']} - {str(e)}")
                results.append({
                    'target_table': plan['target_table'],
                    'target_field': plan['target_field'],
                    'status': 'error',
                    'error': str(e),
                    'record_results': {}
                })
        
        return results
    
    def _execute_single_batch_query(self, plan: Dict) -> Dict:
        """执行单个批量查询"""
        try:
            target_table = plan['target_table']
            target_field = plan['target_field']
            source_values = plan['source_values']
            
            # 自动检测字段类型
            sample_values = [sv['source_value'] for sv in source_values[:10]]
            field_type = self.text_matcher.detect_field_type(target_field, sample_values)
            config = self.text_matcher.field_configs.get(field_type, 
                                                       self.text_matcher.field_configs[FieldType.TEXT])
            
            # 收集所有关键词
            all_keywords = set()
            record_keywords = {}
            
            for sv in source_values:
                record_id = sv['record_id']
                source_value = sv['source_value']
                
                # 预处理和关键词提取
                preprocessed_value = self.text_matcher._apply_preprocessing(source_value, config)
                if preprocessed_value:
                    keywords = self.text_matcher._apply_keyword_extraction(preprocessed_value, config)
                    if keywords:
                        all_keywords.update(keywords)
                        record_keywords[record_id] = {
                            'keywords': keywords,
                            'preprocessed_value': preprocessed_value,
                            'original_value': source_value
                        }
            
            if not all_keywords:
                return {
                    'target_table': target_table,
                    'target_field': target_field,
                    'status': 'no_keywords',
                    'record_results': {}
                }
            
            # 执行批量聚合查询
            candidates = self._execute_batch_aggregation_query(
                target_table, target_field, list(all_keywords), config, record_keywords
            )
            
            # 为每个记录匹配候选
            record_results = self._match_candidates_to_records(
                candidates, record_keywords, config.similarity_threshold
            )
            
            return {
                'target_table': target_table,
                'target_field': target_field,
                'field_type': field_type.value,
                'status': 'success',
                'total_candidates': len(candidates),
                'record_results': record_results
            }
            
        except Exception as e:
            logger.error(f"单个批量查询执行失败: {str(e)}")
            raise
    
    def _execute_batch_aggregation_query(self, target_table: str, target_field: str, 
                                        all_keywords: List[str], config, record_keywords: Dict) -> List[Dict]:
        """执行批量聚合查询"""
        try:
            # 构建索引表名（根据字段名动态生成）
            index_table_name = f"{target_table}_{target_field}_keywords"
            
            # 检查索引表
            if index_table_name not in self.db.list_collection_names():
                if self.query_config['enable_auto_index_creation']:
                    logger.info(f"批量查询时自动创建索引: {index_table_name}")
                    self.index_builder.build_field_index(target_table, target_field)
                    self.query_stats['auto_indexes_created'] += 1
                else:
                    return []
            
            # 【关键修复】构建批量聚合管道 - 移除错误的相似度计算，由后续方法正确处理
            pipeline = [
                # 第1阶段：匹配关键词
                {'$match': {
                    'keyword': {'$in': all_keywords},
                    'field_name': target_field
                }},
                # 第2阶段：按文档ID分组统计
                {'$group': {
                    '_id': '$doc_id',
                    'original_value': {'$first': '$original_value'},
                    'match_count': {'$sum': 1},
                    'matched_keywords': {'$push': '$keyword'}
                }},
                # 第3阶段：按匹配关键词数量排序（更多匹配的优先）
                {'$sort': {'match_count': -1}},
                # 第4阶段：限制候选数量（在相似度计算前先限制，提高性能）
                {'$limit': self.query_config['max_candidates_per_field'] * 2}  # 多取一些，后续再精确过滤
            ]
            
            # 执行查询
            index_collection = self.db[index_table_name]
            
            aggregation_results = list(index_collection.aggregate(pipeline))
            
            # 获取完整记录（批量查询版本）
            return self._fetch_full_records_batch(target_table, aggregation_results)
            
        except Exception as e:
            logger.error(f"批量聚合查询失败: {target_table}.{target_field} - {str(e)}")
            return []
    
    def _match_candidates_to_records(self, candidates: List[Dict], record_keywords: Dict[str, Dict], 
                                   similarity_threshold: float) -> Dict[str, List[Dict]]:
        """将候选记录匹配到源记录"""
        record_results = {}
        
        for record_id, keyword_info in record_keywords.items():
            record_keywords_set = set(keyword_info['keywords'])
            record_candidates = []
            
            for candidate in candidates:
                # 【关键修复】使用正确的字段名 _matched_keywords（来自_fetch_full_records_batch）
                candidate_keywords = set(candidate.get('_matched_keywords', []))
                
                # 计算关键词交集相似度（与单查询保持一致）
                intersection = len(record_keywords_set.intersection(candidate_keywords))
                similarity = intersection / max(len(record_keywords_set), 1)
                
                if similarity >= similarity_threshold:
                    candidate_copy = candidate.copy()
                    candidate_copy['similarity_score'] = similarity
                    record_candidates.append(candidate_copy)
            
            if record_candidates:
                # 按相似度排序
                record_candidates.sort(key=lambda x: x['similarity_score'], reverse=True)
                record_results[record_id] = record_candidates
        
        return record_results
    
    def _calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """
        计算地址相似度（高性能优化版）
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        # 快速预检查
        if not addr1 or not addr2:
            return 0.0
        if addr1 == addr2:
            return 1.0
            
        # 使用简单高效的字符重叠比例，避免复杂的地址标准化
        addr1_chars = set(addr1)
        addr2_chars = set(addr2)
        intersection = len(addr1_chars.intersection(addr2_chars))
        union = len(addr1_chars.union(addr2_chars))
        
        # 基础相似度
        base_similarity = intersection / max(union, 1)
        
        # 快速关键词匹配加权
        if any(keyword in addr1 and keyword in addr2 for keyword in ['区', '市', '路', '街', '号']):
            base_similarity *= 1.2  # 提升有共同地址关键词的相似度
            
        return min(base_similarity, 1.0)
    
    def _merge_batch_results(self, batch_records: List[Dict], batch_results: List[Dict], 
                           mappings: List[Dict]) -> Dict[str, QueryResult]:
        """合并批量查询结果"""
        final_results = {}
        
        # 为每个源记录创建结果
        for record in batch_records:
            record_id = str(record.get('_id', ''))
            all_candidates = []
            field_info = {'mappings': []}
            
            # 收集所有字段的候选
            for batch_result in batch_results:
                if batch_result.get('status') == 'success':
                    record_candidates = batch_result.get('record_results', {}).get(record_id, [])
                    all_candidates.extend(record_candidates)
                    
                    if record_candidates:
                        field_info['mappings'].append({
                            'target_table': batch_result['target_table'],
                            'target_field': batch_result['target_field'],
                            'field_type': batch_result.get('field_type', 'unknown'),
                            'candidate_count': len(record_candidates)
                        })
            
            # 去重和排序
            unique_candidates = self._deduplicate_candidates(all_candidates)
            
            final_results[record_id] = QueryResult(
                unique_candidates, 0.0,  # 批量查询时间在外层统计
                record, field_info
            )
        
        return final_results
    
    def _deduplicate_candidates(self, candidates: List[Dict]) -> List[Dict]:
        """去重候选记录"""
        seen_ids = set()
        unique_candidates = []
        
        # 按相似度排序
        candidates.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
        
        for candidate in candidates:
            candidate_id = candidate.get('_id')
            if candidate_id not in seen_ids:
                seen_ids.add(candidate_id)
                unique_candidates.append(candidate)
        
        return unique_candidates
    
    def _update_query_stats(self, query_time: float, candidate_count: int):
        """更新查询统计"""
        self.query_stats['avg_query_time'] = (
            (self.query_stats['avg_query_time'] * (self.query_stats['total_queries'] - 1) + query_time) /
            self.query_stats['total_queries']
        )
        
        self.query_stats['total_candidates_found'] += candidate_count
        self.query_stats['avg_candidates_per_query'] = (
            self.query_stats['total_candidates_found'] / self.query_stats['total_queries']
        )
    
    def get_query_stats(self) -> Dict[str, Any]:
        """获取查询统计信息"""
        return {
            'total_queries': self.query_stats['total_queries'],
            'batch_queries': self.query_stats['batch_queries'],
            'cache_hits': self.query_stats['cache_hits'],
            'cache_hit_rate': self.query_stats['cache_hits'] / max(self.query_stats['total_queries'], 1),
            'avg_query_time': self.query_stats['avg_query_time'],
            'avg_candidates_per_query': self.query_stats['avg_candidates_per_query'],
            'total_candidates_found': self.query_stats['total_candidates_found'],
            'auto_indexes_created': self.query_stats['auto_indexes_created']
        }
    
    def clear_cache(self):
        """清空缓存"""
        self.query_cache.clear()
        self.pipeline_cache.clear()
        logger.info("查询缓存已清空")
