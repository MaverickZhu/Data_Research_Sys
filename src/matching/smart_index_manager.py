"""
智能索引管理器
根据字段映射自动创建和优化数据库索引
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pymongo
from pymongo import ASCENDING, DESCENDING, TEXT, HASHED
import threading
import time

logger = logging.getLogger(__name__)


class SmartIndexManager:
    """智能索引管理器"""
    
    def __init__(self, db_manager):
        """
        初始化智能索引管理器
        
        Args:
            db_manager: 数据库管理器
        """
        self.db_manager = db_manager
        self.db = db_manager.mongo_client.get_database() if db_manager and db_manager.mongo_client else None
        self.index_cache = {}
        self.cache_lock = threading.Lock()
        
        # 索引配置
        self.index_config = {
            'text_search': {
                'default_language': 'none',  # 关闭语言特定处理，适合中文
                'background': True,
                'sparse': True
            },
            'compound': {
                'background': True,
                'sparse': False
            },
            'single': {
                'background': True,
                'sparse': True
            },
            'performance': {
                'batch_size': 1000,
                'max_time_ms': 30000,  # 30秒超时
                'enable_profiling': True
            }
        }
        
        logger.info("智能索引管理器初始化完成")
    
    def create_mapping_optimized_indexes(self, mappings: List[Dict], source_table: str, 
                                       target_tables: List[str]) -> Dict[str, Any]:
        """
        根据字段映射创建优化索引
        
        Args:
            mappings: 字段映射配置列表
            source_table: 源表名
            target_tables: 目标表名列表
            
        Returns:
            Dict: 索引创建结果报告
        """
        if self.db is None:
            logger.error("数据库连接未初始化")
            return {'success': False, 'error': '数据库连接未初始化'}
        
        start_time = time.time()
        results = {
            'success': True,
            'source_table_indexes': {},
            'target_table_indexes': {},
            'performance_indexes': {},
            'total_time': 0,
            'created_count': 0,
            'skipped_count': 0,
            'error_count': 0,
            'errors': []
        }
        
        try:
            logger.info(f"开始为映射配置创建优化索引")
            logger.info(f"源表: {source_table}, 目标表: {target_tables}")
            logger.info(f"字段映射数量: {len(mappings)}")
            
            # 1. 为源表创建索引
            source_result = self._create_source_table_indexes(source_table, mappings)
            results['source_table_indexes'] = source_result
            results['created_count'] += source_result.get('created_count', 0)
            results['skipped_count'] += source_result.get('skipped_count', 0)
            results['error_count'] += source_result.get('error_count', 0)
            
            # 2. 为目标表创建索引
            for target_table in target_tables:
                target_result = self._create_target_table_indexes(target_table, mappings)
                results['target_table_indexes'][target_table] = target_result
                results['created_count'] += target_result.get('created_count', 0)
                results['skipped_count'] += target_result.get('skipped_count', 0)
                results['error_count'] += target_result.get('error_count', 0)
            
            # 3. 创建性能优化索引
            perf_result = self._create_performance_indexes(source_table, target_tables, mappings)
            results['performance_indexes'] = perf_result
            results['created_count'] += perf_result.get('created_count', 0)
            results['skipped_count'] += perf_result.get('skipped_count', 0)
            results['error_count'] += perf_result.get('error_count', 0)
            
            # 4. 创建匹配结果表索引
            match_result = self._create_match_result_indexes(source_table, target_tables)
            results['match_result_indexes'] = match_result
            results['created_count'] += match_result.get('created_count', 0)
            results['skipped_count'] += match_result.get('skipped_count', 0)
            results['error_count'] += match_result.get('error_count', 0)
            
        except Exception as e:
            logger.error(f"创建优化索引失败: {str(e)}")
            results['success'] = False
            results['errors'].append(str(e))
        
        results['total_time'] = time.time() - start_time
        
        logger.info(f"索引创建完成 - 创建: {results['created_count']}, "
                   f"跳过: {results['skipped_count']}, 错误: {results['error_count']}, "
                   f"耗时: {results['total_time']:.2f}秒")
        
        return results
    
    def _create_source_table_indexes(self, source_table: str, mappings: List[Dict]) -> Dict[str, Any]:
        """为源表创建索引"""
        result = {'created_count': 0, 'skipped_count': 0, 'error_count': 0, 'indexes': []}
        
        try:
            collection = self.db[source_table]
            existing_indexes = set(collection.index_information().keys())
            
            # 提取源字段
            source_fields = [mapping['source_field'] for mapping in mappings]
            
            # 1. 单字段索引（用于快速查找）
            for field in source_fields:
                indexes_to_create = [
                    (f"idx_{field}_asc", [(field, ASCENDING)]),
                    (f"idx_{field}_text", [(field, TEXT)]),
                ]
                
                for index_name, index_spec in indexes_to_create:
                    if index_name not in existing_indexes:
                        try:
                            if index_spec[0][1] == TEXT:
                                collection.create_index(
                                    index_spec, 
                                    name=index_name,
                                    **self.index_config['text_search']
                                )
                            else:
                                collection.create_index(
                                    index_spec, 
                                    name=index_name,
                                    **self.index_config['single']
                                )
                            result['created_count'] += 1
                            result['indexes'].append(index_name)
                            logger.info(f"✅ 源表索引创建: {source_table}.{index_name}")
                        except Exception as e:
                            result['error_count'] += 1
                            logger.warning(f"⚠️ 源表索引创建失败: {index_name} - {str(e)}")
                    else:
                        result['skipped_count'] += 1
            
            # 2. 复合索引（用于多字段查询优化）
            if len(source_fields) > 1:
                # 创建前两个字段的复合索引
                compound_fields = source_fields[:2]
                compound_name = f"idx_compound_{'_'.join(compound_fields)}"
                
                if compound_name not in existing_indexes:
                    try:
                        compound_spec = [(field, ASCENDING) for field in compound_fields]
                        collection.create_index(
                            compound_spec,
                            name=compound_name,
                            **self.index_config['compound']
                        )
                        result['created_count'] += 1
                        result['indexes'].append(compound_name)
                        logger.info(f"✅ 源表复合索引创建: {source_table}.{compound_name}")
                    except Exception as e:
                        result['error_count'] += 1
                        logger.warning(f"⚠️ 源表复合索引创建失败: {compound_name} - {str(e)}")
                else:
                    result['skipped_count'] += 1
            
            # 3. 基础性能索引
            basic_indexes = [
                ("idx_id_asc", [("_id", ASCENDING)]),  # MongoDB默认有，但确保存在
                ("idx_created_desc", [("created_at", DESCENDING)]),
                ("idx_updated_desc", [("updated_at", DESCENDING)]),
            ]
            
            for index_name, index_spec in basic_indexes:
                if index_name not in existing_indexes:
                    try:
                        collection.create_index(
                            index_spec,
                            name=index_name,
                            **self.index_config['single']
                        )
                        result['created_count'] += 1
                        result['indexes'].append(index_name)
                        logger.info(f"✅ 源表基础索引创建: {source_table}.{index_name}")
                    except Exception as e:
                        if "_id" not in index_name:  # _id索引默认存在，忽略错误
                            result['error_count'] += 1
                            logger.warning(f"⚠️ 源表基础索引创建失败: {index_name} - {str(e)}")
                else:
                    result['skipped_count'] += 1
                    
        except Exception as e:
            logger.error(f"源表索引创建过程失败: {str(e)}")
            result['error_count'] += 1
        
        return result
    
    def _create_target_table_indexes(self, target_table: str, mappings: List[Dict]) -> Dict[str, Any]:
        """为目标表创建索引"""
        result = {'created_count': 0, 'skipped_count': 0, 'error_count': 0, 'indexes': []}
        
        try:
            collection = self.db[target_table]
            existing_indexes = set(collection.index_information().keys())
            
            # 提取目标字段
            target_fields = [mapping['target_field'] for mapping in mappings 
                           if mapping['target_table'] == target_table]
            
            if not target_fields:
                return result
            
            # 1. 单字段索引
            for field in target_fields:
                indexes_to_create = [
                    (f"idx_{field}_asc", [(field, ASCENDING)]),
                    (f"idx_{field}_text", [(field, TEXT)]),
                    (f"idx_{field}_hash", [(field, HASHED)]),  # 用于分片和快速等值查询
                ]
                
                for index_name, index_spec in indexes_to_create:
                    if index_name not in existing_indexes:
                        try:
                            if index_spec[0][1] == TEXT:
                                collection.create_index(
                                    index_spec, 
                                    name=index_name,
                                    **self.index_config['text_search']
                                )
                            elif index_spec[0][1] == HASHED:
                                collection.create_index(
                                    index_spec, 
                                    name=index_name,
                                    background=True
                                )
                            else:
                                collection.create_index(
                                    index_spec, 
                                    name=index_name,
                                    **self.index_config['single']
                                )
                            result['created_count'] += 1
                            result['indexes'].append(index_name)
                            logger.info(f"✅ 目标表索引创建: {target_table}.{index_name}")
                        except Exception as e:
                            result['error_count'] += 1
                            logger.warning(f"⚠️ 目标表索引创建失败: {index_name} - {str(e)}")
                    else:
                        result['skipped_count'] += 1
            
            # 2. 复合索引
            if len(target_fields) > 1:
                compound_fields = target_fields[:2]
                compound_name = f"idx_compound_{'_'.join(compound_fields)}"
                
                if compound_name not in existing_indexes:
                    try:
                        compound_spec = [(field, ASCENDING) for field in compound_fields]
                        collection.create_index(
                            compound_spec,
                            name=compound_name,
                            **self.index_config['compound']
                        )
                        result['created_count'] += 1
                        result['indexes'].append(compound_name)
                        logger.info(f"✅ 目标表复合索引创建: {target_table}.{compound_name}")
                    except Exception as e:
                        result['error_count'] += 1
                        logger.warning(f"⚠️ 目标表复合索引创建失败: {compound_name} - {str(e)}")
                else:
                    result['skipped_count'] += 1
                    
        except Exception as e:
            logger.error(f"目标表索引创建过程失败: {str(e)}")
            result['error_count'] += 1
        
        return result
    
    def _create_performance_indexes(self, source_table: str, target_tables: List[str], 
                                  mappings: List[Dict]) -> Dict[str, Any]:
        """创建性能优化索引"""
        result = {'created_count': 0, 'skipped_count': 0, 'error_count': 0, 'indexes': []}
        
        try:
            # 为所有相关表创建性能优化索引
            all_tables = [source_table] + target_tables
            
            for table_name in all_tables:
                collection = self.db[table_name]
                existing_indexes = set(collection.index_information().keys())
                
                # 性能优化索引定义
                perf_indexes = [
                    # 匹配状态索引（用于快速筛选已匹配/未匹配记录）
                    ("idx_is_matched", [("is_matched", ASCENDING)]),
                    ("idx_match_status", [("match_status", ASCENDING)]),
                    
                    # 时间范围索引（用于增量匹配）
                    ("idx_created_time_desc", [("created_at", DESCENDING)]),
                    ("idx_updated_time_desc", [("updated_at", DESCENDING)]),
                    
                    # 复合性能索引
                    ("idx_match_time_compound", [("is_matched", ASCENDING), ("updated_at", DESCENDING)]),
                    ("idx_status_time_compound", [("match_status", ASCENDING), ("created_at", DESCENDING)]),
                ]
                
                for index_name, index_spec in perf_indexes:
                    full_index_name = f"{table_name}_{index_name}"
                    
                    if index_name not in existing_indexes:
                        try:
                            collection.create_index(
                                index_spec,
                                name=index_name,
                                **self.index_config['single']
                            )
                            result['created_count'] += 1
                            result['indexes'].append(full_index_name)
                            logger.info(f"✅ 性能索引创建: {table_name}.{index_name}")
                        except Exception as e:
                            result['error_count'] += 1
                            logger.warning(f"⚠️ 性能索引创建失败: {full_index_name} - {str(e)}")
                    else:
                        result['skipped_count'] += 1
                        
        except Exception as e:
            logger.error(f"性能索引创建过程失败: {str(e)}")
            result['error_count'] += 1
        
        return result
    
    def _create_match_result_indexes(self, source_table: str, target_tables: List[str]) -> Dict[str, Any]:
        """为匹配结果表创建索引"""
        result = {'created_count': 0, 'skipped_count': 0, 'error_count': 0, 'indexes': []}
        
        try:
            # 匹配结果集合名称
            result_collection_name = f'user_match_results_{source_table}'
            collection = self.db[result_collection_name]
            existing_indexes = set(collection.index_information().keys())
            
            # 匹配结果索引定义
            match_result_indexes = [
                # 基础查询索引
                ("idx_source_id", [("source_id", ASCENDING)]),
                ("idx_target_id", [("target_id", ASCENDING)]),
                ("idx_source_table", [("source_table", ASCENDING)]),
                ("idx_target_table", [("target_table", ASCENDING)]),
                ("idx_task_id", [("task_id", ASCENDING)]),
                ("idx_config_id", [("config_id", ASCENDING)]),
                
                # 相似度和质量索引
                ("idx_similarity_desc", [("similarity_score", DESCENDING)]),
                ("idx_confidence", [("match_details.confidence_level", ASCENDING)]),
                ("idx_match_type", [("match_details.match_type", ASCENDING)]),
                
                # 时间索引
                ("idx_created_desc", [("created_at", DESCENDING)]),
                
                # 复合查询索引
                ("idx_task_similarity", [("task_id", ASCENDING), ("similarity_score", DESCENDING)]),
                ("idx_source_target", [("source_table", ASCENDING), ("target_table", ASCENDING)]),
                ("idx_config_created", [("config_id", ASCENDING), ("created_at", DESCENDING)]),
                
                # 统计分析索引
                ("idx_similarity_confidence", [("similarity_score", DESCENDING), ("match_details.confidence_level", ASCENDING)]),
            ]
            
            for index_name, index_spec in match_result_indexes:
                if index_name not in existing_indexes:
                    try:
                        collection.create_index(
                            index_spec,
                            name=index_name,
                            **self.index_config['single']
                        )
                        result['created_count'] += 1
                        result['indexes'].append(f"{result_collection_name}.{index_name}")
                        logger.info(f"✅ 匹配结果索引创建: {result_collection_name}.{index_name}")
                    except Exception as e:
                        result['error_count'] += 1
                        logger.warning(f"⚠️ 匹配结果索引创建失败: {index_name} - {str(e)}")
                else:
                    result['skipped_count'] += 1
                    
        except Exception as e:
            logger.error(f"匹配结果索引创建过程失败: {str(e)}")
            result['error_count'] += 1
        
        return result
    
    def get_index_statistics(self, table_names: List[str]) -> Dict[str, Any]:
        """获取索引统计信息"""
        if not self.db:
            return {'error': '数据库连接未初始化'}
        
        stats = {}
        
        for table_name in table_names:
            try:
                collection = self.db[table_name]
                
                # 获取索引信息
                indexes = collection.index_information()
                
                # 获取集合统计
                collection_stats = self.db.command("collStats", table_name)
                
                stats[table_name] = {
                    'index_count': len(indexes),
                    'index_names': list(indexes.keys()),
                    'total_index_size': collection_stats.get('totalIndexSize', 0),
                    'document_count': collection_stats.get('count', 0),
                    'avg_obj_size': collection_stats.get('avgObjSize', 0),
                    'storage_size': collection_stats.get('storageSize', 0)
                }
                
            except Exception as e:
                stats[table_name] = {'error': str(e)}
        
        return stats
    
    def optimize_existing_indexes(self, table_names: List[str]) -> Dict[str, Any]:
        """优化现有索引"""
        if not self.db:
            return {'error': '数据库连接未初始化'}
        
        results = {}
        
        for table_name in table_names:
            try:
                collection = self.db[table_name]
                
                # 重建索引以优化性能
                result = collection.reindex()
                
                results[table_name] = {
                    'success': True,
                    'reindex_result': result
                }
                
                logger.info(f"✅ 索引优化完成: {table_name}")
                
            except Exception as e:
                results[table_name] = {
                    'success': False,
                    'error': str(e)
                }
                logger.error(f"❌ 索引优化失败: {table_name} - {str(e)}")
        
        return results
