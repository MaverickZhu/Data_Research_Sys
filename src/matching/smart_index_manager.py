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
    
    def _index_function_exists(self, collection, field: str, direction: int) -> bool:
        """
        检查是否已存在相同功能的索引（忽略索引名称）
        
        Args:
            collection: MongoDB集合
            field: 字段名
            direction: 索引方向 (ASCENDING, DESCENDING, TEXT, HASHED)
            
        Returns:
            bool: 是否存在相同功能的索引
        """
        try:
            index_info_dict = collection.index_information()
            
            for index_name, index_info in index_info_dict.items():
                if isinstance(index_info, dict):
                    key_info = index_info.get('key', {})
                    if isinstance(key_info, dict):
                        # 检查单字段索引
                        if len(key_info) == 1 and field in key_info:
                            existing_direction = key_info[field]
                            if existing_direction == direction:
                                logger.debug(f"找到相同功能索引: {index_name} ({field}: {direction})")
                                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"检查索引功能时出错: {e}")
            return False
    
    def _compound_index_exists(self, collection, fields: List[str], directions: List[int] = None) -> bool:
        """
        检查是否已存在相同功能的复合索引
        
        Args:
            collection: MongoDB集合
            fields: 字段列表
            directions: 方向列表，默认为ASCENDING
            
        Returns:
            bool: 是否存在相同功能的复合索引
        """
        try:
            if directions is None:
                directions = [ASCENDING] * len(fields)
            
            if len(fields) != len(directions):
                return False
            
            target_spec = dict(zip(fields, directions))
            index_info_dict = collection.index_information()
            
            for index_name, index_info in index_info_dict.items():
                if isinstance(index_info, dict):
                    key_info = index_info.get('key', {})
                    if isinstance(key_info, dict):
                        # 检查字段和方向是否完全匹配
                        if key_info == target_spec:
                            logger.debug(f"找到相同功能复合索引: {index_name} ({fields})")
                            return True
            
            return False
            
        except Exception as e:
            logger.warning(f"检查复合索引功能时出错: {e}")
            return False
    
    def _create_smart_text_index(self, collection, target_fields, existing_indexes, result, target_table):
        """
        智能文本索引管理 - 解决MongoDB每个集合只能有一个文本索引的限制
        """
        try:
            # 检查是否已存在文本索引
            existing_text_indexes = []
            existing_text_fields = set()
            
            # 获取详细的索引信息
            index_info_dict = collection.index_information()
            for index_name, index_info in index_info_dict.items():
                # 处理不同格式的索引信息
                if isinstance(index_info, dict):
                    key_info = index_info.get('key', {})
                    if isinstance(key_info, dict) and key_info.get('_fts') == 'text':
                        existing_text_indexes.append(index_name)
                        # 收集现有文本索引包含的字段
                        existing_text_fields.update(index_info.get('weights', {}).keys())
                elif isinstance(index_info, list):
                    # 某些情况下索引信息可能是列表格式
                    for item in index_info:
                        if isinstance(item, dict) and item.get('_fts') == 'text':
                            existing_text_indexes.append(index_name)
                            existing_text_fields.update(item.get('weights', {}).keys())
                            break
            
            # 检查目标字段是否已被现有文本索引覆盖
            target_fields_set = set(target_fields)
            
            if existing_text_indexes:
                if target_fields_set.issubset(existing_text_fields):
                    # 现有文本索引已包含所有目标字段
                    logger.info(f"✅ 文本索引已包含所有必需字段: {existing_text_indexes[0]} (字段: {list(existing_text_fields)})")
                    result['skipped_count'] += 1
                    return
                else:
                    # 需要更新文本索引以包含新字段
                    missing_fields = target_fields_set - existing_text_fields
                    logger.info(f"📝 文本索引需要更新，缺少字段: {list(missing_fields)}")
                    
                    # 合并字段列表
                    all_fields = list(existing_text_fields.union(target_fields_set))
                    
                    # 删除旧的文本索引
                    for old_index in existing_text_indexes:
                        try:
                            collection.drop_index(old_index)
                            logger.info(f"🗑️ 删除旧文本索引: {old_index}")
                        except Exception as e:
                            logger.warning(f"⚠️ 删除旧文本索引失败: {old_index} - {e}")
                    
                    # 创建包含所有字段的新文本索引
                    self._create_compound_text_index(collection, all_fields, result, target_table)
            else:
                # 不存在文本索引，创建新的复合文本索引
                self._create_compound_text_index(collection, target_fields, result, target_table)
                
        except Exception as e:
            result['error_count'] += 1
            logger.warning(f"⚠️ 智能文本索引管理失败: {e}")
    
    def _create_compound_text_index(self, collection, target_fields, result, target_table):
        """创建复合文本索引（简化版，调用前已检查冲突）"""
        try:
            # 直接创建复合文本索引
            text_fields = [(field, TEXT) for field in target_fields]
            text_weights = {field: 1 for field in target_fields}
            
            collection.create_index(
                text_fields,
                name="idx_compound_text",
                weights=text_weights,
                **self.index_config['text_search']
            )
            
            result['created_count'] += 1
            result['indexes'].append("idx_compound_text")
            logger.info(f"✅ 复合文本索引创建成功: {target_table}.idx_compound_text (字段: {', '.join(target_fields)})")
            
        except Exception as e:
            result['error_count'] += 1
            logger.info(f"📋 复合文本索引跳过（索引冲突）: {target_table}.idx_compound_text - {e}")
    
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
                # 检查升序索引
                asc_index_name = f"idx_{field}_asc"
                if not self._index_function_exists(collection, field, ASCENDING):
                    try:
                        collection.create_index(
                            [(field, ASCENDING)], 
                            name=asc_index_name,
                            **self.index_config['single']
                        )
                        result['created_count'] += 1
                        result['indexes'].append(asc_index_name)
                        logger.info(f"✅ 源表索引创建: {source_table}.{asc_index_name}")
                    except Exception as e:
                        result['error_count'] += 1
                        logger.warning(f"⚠️ 源表索引创建失败: {asc_index_name} - {str(e)}")
                else:
                    result['skipped_count'] += 1
                    logger.info(f"📋 源表索引跳过（功能已存在）: {field} 升序索引")
                
                # 跳过单字段文本索引创建，因为会与复合文本索引冲突
                # 文本搜索功能由复合文本索引提供
            
            # 2. 复合索引（用于多字段查询优化）
            if len(source_fields) > 1:
                # 创建前两个字段的复合索引
                compound_fields = source_fields[:2]
                compound_name = f"idx_compound_{'_'.join(compound_fields)}"
                
                if not self._compound_index_exists(collection, compound_fields):
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
                    logger.info(f"📋 源表复合索引跳过（功能已存在）: {compound_fields}")
            
            # 3. 基础性能索引
            basic_indexes = [
                ("created_at", DESCENDING),
                ("updated_at", DESCENDING),
            ]
            
            for field, direction in basic_indexes:
                if not self._index_function_exists(collection, field, direction):
                    index_name = f"idx_{field}_{'desc' if direction == DESCENDING else 'asc'}"
                    try:
                        collection.create_index(
                            [(field, direction)],
                            name=index_name,
                            **self.index_config['single']
                        )
                        result['created_count'] += 1
                        result['indexes'].append(index_name)
                        logger.info(f"✅ 源表基础索引创建: {source_table}.{index_name}")
                    except Exception as e:
                        # 检查是否是索引名称冲突
                        if 'Index already exists with a different name' in str(e):
                            logger.info(f"📋 源表基础索引跳过（已存在相同字段索引）: {field} - {str(e)}")
                            result['skipped_count'] += 1
                        else:
                            result['error_count'] += 1
                            logger.warning(f"⚠️ 源表基础索引创建失败: {index_name} - {str(e)}, full error: {e}")
                else:
                    result['skipped_count'] += 1
                    logger.info(f"📋 源表基础索引跳过（功能已存在）: {field} {'降序' if direction == DESCENDING else '升序'}索引")
                    
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
            
            # 1. 单字段索引（不包含文本索引）
            for field in target_fields:
                indexes_to_create = [
                    (f"idx_{field}_asc", [(field, ASCENDING)]),
                    (f"idx_{field}_hash", [(field, HASHED)]),  # 用于分片和快速等值查询
                ]
                
                for index_name, index_spec in indexes_to_create:
                    if index_name not in existing_indexes:
                        try:
                            if index_spec[0][1] == HASHED:
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
            
            # 2. 智能文本索引管理（解决MongoDB文本索引冲突）
            # 注意：地址匹配功能不依赖文本索引，使用地址关键词索引实现高效匹配
            # 文本索引冲突警告可以忽略，不影响地址匹配功能
            self._create_smart_text_index(collection, target_fields, existing_indexes, result, target_table)
            
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
                    ("idx_created_at_desc", [("created_at", DESCENDING)]),
                    ("idx_updated_at_desc", [("updated_at", DESCENDING)]),
                    
                    # 复合性能索引
                    ("idx_match_time_compound", [("is_matched", ASCENDING), ("updated_at", DESCENDING)]),
                    ("idx_status_time_compound", [("match_status", ASCENDING), ("created_at", DESCENDING)]),
                ]
                
                for index_name, index_spec in perf_indexes:
                    full_index_name = f"{table_name}_{index_name}"
                    
                    # 检查是否存在相同字段的索引（避免冲突）
                    index_exists = False
                    target_fields = [field[0] for field in index_spec]
                    
                    for existing_name, existing_info in collection.index_information().items():
                        if isinstance(existing_info, dict):
                            existing_key = existing_info.get('key', {})
                            if isinstance(existing_key, dict):
                                existing_fields = list(existing_key.keys())
                                if existing_fields == target_fields:
                                    index_exists = True
                                    logger.info(f"📋 跳过性能索引创建（已存在相同字段索引）: {table_name}.{existing_name} -> {target_fields}")
                                    break
                    
                    if not index_exists and index_name not in existing_indexes:
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
                            # 如果仍然冲突，记录警告但不影响系统运行
                            result['error_count'] += 1
                            logger.info(f"📋 性能索引跳过（索引冲突）: {full_index_name} - {str(e)}")
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
