#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用索引构建器
为任意字段类型自动创建预建索引表，支持高性能文本语义分析查询
"""

import logging
import time
from typing import Dict, List, Set, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo
from pymongo import ASCENDING, TEXT
from .universal_text_matcher import UniversalTextMatcher, FieldType

logger = logging.getLogger(__name__)


class UniversalIndexBuilder:
    """通用索引构建器"""
    
    def __init__(self, db_manager):
        """
        初始化通用索引构建器
        
        Args:
            db_manager: 数据库管理器
        """
        self.db_manager = db_manager
        self.db = db_manager.get_db() if db_manager else None
        self.text_matcher = UniversalTextMatcher(db_manager)
        
        # 构建配置
        self.build_config = {
            'batch_size': 1000,
            'max_workers': 8,
            'enable_parallel': True,
            'clear_existing': True,  # 是否清空现有索引
            'create_compound_indexes': True,  # 是否创建复合索引
            'enable_progress_logging': True
        }
        
        # 性能统计
        self.build_stats = {
            'total_records_processed': 0,
            'total_keywords_created': 0,
            'total_time_spent': 0.0,
            'tables_processed': 0,
            'fields_processed': 0
        }
        
        logger.info("🏗️ 通用索引构建器初始化完成")
    
    def build_field_index(self, table_name: str, field_name: str, 
                         field_type: FieldType = None, force_rebuild: bool = False) -> Dict[str, Any]:
        """
        为指定字段构建索引
        
        Args:
            table_name: 表名
            field_name: 字段名
            field_type: 字段类型（可选，会自动检测）
            force_rebuild: 是否强制重建
            
        Returns:
            Dict: 构建结果统计
        """
        start_time = time.time()
        
        try:
            logger.info(f"🏗️ 开始为字段构建索引: {table_name}.{field_name}")
            
            # 检查源表是否存在
            if table_name not in self.db.list_collection_names():
                raise ValueError(f"源表不存在: {table_name}")
            
            source_collection = self.db[table_name]
            
            # 自动检测字段类型
            if field_type is None:
                sample_values = self._get_sample_values(source_collection, field_name, 20)
                field_type = self.text_matcher.detect_field_type(field_name, sample_values)
            
            # 构建索引表名
            index_table_name = f"{table_name}_{field_name}_keywords"
            
            # 检查是否需要重建
            if not force_rebuild and self._index_exists_and_valid(index_table_name, table_name, field_name):
                logger.info(f"索引已存在且有效，跳过构建: {index_table_name}")
                return {'status': 'skipped', 'reason': 'index_exists'}
            
            # 清空现有索引（如果存在）
            if self.build_config['clear_existing']:
                self._clear_existing_index(index_table_name, table_name, field_name)
            
            # 获取字段处理配置
            config = self.text_matcher.field_configs.get(field_type, 
                                                       self.text_matcher.field_configs[FieldType.TEXT])
            
            # 构建索引
            build_result = self._build_index_for_field(
                source_collection, index_table_name, field_name, field_type, config
            )
            
            # 创建索引表的数据库索引
            self._create_database_indexes(index_table_name)
            
            # 更新统计
            build_time = time.time() - start_time
            self.build_stats['total_time_spent'] += build_time
            self.build_stats['fields_processed'] += 1
            
            result = {
                'status': 'success',
                'table_name': table_name,
                'field_name': field_name,
                'field_type': field_type.value,
                'index_table_name': index_table_name,
                'records_processed': build_result['records_processed'],
                'keywords_created': build_result['keywords_created'],
                'build_time': build_time,
                'processing_rate': build_result['records_processed'] / max(build_time, 0.001)
            }
            
            logger.info(f"✅ 索引构建完成: {index_table_name}, "
                       f"处理 {build_result['records_processed']} 条记录, "
                       f"生成 {build_result['keywords_created']} 个关键词, "
                       f"耗时 {build_time:.2f}s, "
                       f"速度 {result['processing_rate']:.1f} 条/秒")
            
            return result
            
        except Exception as e:
            logger.error(f"索引构建失败: {table_name}.{field_name} - {str(e)}")
            return {
                'status': 'error',
                'table_name': table_name,
                'field_name': field_name,
                'error': str(e),
                'build_time': time.time() - start_time
            }
    
    def build_table_indexes(self, table_name: str, field_mappings: List[Dict], 
                           force_rebuild: bool = False) -> Dict[str, Any]:
        """
        为表的多个字段批量构建索引
        
        Args:
            table_name: 表名
            field_mappings: 字段映射列表
            force_rebuild: 是否强制重建
            
        Returns:
            Dict: 批量构建结果
        """
        start_time = time.time()
        
        try:
            logger.info(f"🏗️ 开始为表构建索引: {table_name}, 字段数量: {len(field_mappings)}")
            
            results = {
                'table_name': table_name,
                'total_fields': len(field_mappings),
                'field_results': [],
                'success_count': 0,
                'error_count': 0,
                'skipped_count': 0,
                'total_build_time': 0.0
            }
            
            # 提取需要构建索引的字段
            target_fields = set()
            for mapping in field_mappings:
                if mapping.get('target_table') == table_name:
                    target_fields.add(mapping.get('target_field'))
            
            if not target_fields:
                logger.warning(f"表 {table_name} 没有需要构建索引的字段")
                return results
            
            # 并行构建索引
            if self.build_config['enable_parallel'] and len(target_fields) > 1:
                field_results = self._build_indexes_parallel(table_name, target_fields, force_rebuild)
            else:
                field_results = self._build_indexes_sequential(table_name, target_fields, force_rebuild)
            
            # 汇总结果
            for field_result in field_results:
                results['field_results'].append(field_result)
                if field_result['status'] == 'success':
                    results['success_count'] += 1
                elif field_result['status'] == 'error':
                    results['error_count'] += 1
                else:
                    results['skipped_count'] += 1
            
            results['total_build_time'] = time.time() - start_time
            self.build_stats['tables_processed'] += 1
            
            logger.info(f"✅ 表索引构建完成: {table_name}, "
                       f"成功: {results['success_count']}, "
                       f"错误: {results['error_count']}, "
                       f"跳过: {results['skipped_count']}, "
                       f"总耗时: {results['total_build_time']:.2f}s")
            
            return results
            
        except Exception as e:
            logger.error(f"表索引构建失败: {table_name} - {str(e)}")
            return {
                'table_name': table_name,
                'status': 'error',
                'error': str(e),
                'total_build_time': time.time() - start_time
            }
    
    def _get_sample_values(self, collection, field_name: str, sample_size: int = 20) -> List[str]:
        """获取字段样本值"""
        try:
            pipeline = [
                {'$match': {field_name: {'$exists': True, '$ne': None, '$ne': ''}}},
                {'$sample': {'size': sample_size}},
                {'$project': {field_name: 1}}
            ]
            
            samples = list(collection.aggregate(pipeline))
            return [str(doc.get(field_name, '')) for doc in samples if doc.get(field_name)]
            
        except Exception as e:
            logger.warning(f"获取样本值失败: {field_name} - {str(e)}")
            return []
    
    def _index_exists_and_valid(self, index_table_name: str, source_table: str, field_name: str) -> bool:
        """检查索引是否存在且有效"""
        try:
            if index_table_name not in self.db.list_collection_names():
                return False
            
            index_collection = self.db[index_table_name]
            
            # 检查索引表是否有数据
            count = index_collection.count_documents({'field_name': field_name})
            if count == 0:
                return False
            
            # 检查索引表的数据是否比源表新
            # 这里可以添加更复杂的版本检查逻辑
            return True
            
        except Exception as e:
            logger.warning(f"检查索引有效性失败: {index_table_name} - {str(e)}")
            return False
    
    def _clear_existing_index(self, index_table_name: str, source_table: str, field_name: str):
        """清空现有索引"""
        try:
            if index_table_name in self.db.list_collection_names():
                # 只清空特定字段的索引数据
                self.db[index_table_name].delete_many({
                    'source_table': source_table,
                    'field_name': field_name
                })
                logger.info(f"已清空现有索引数据: {index_table_name}.{field_name}")
        except Exception as e:
            logger.warning(f"清空索引失败: {index_table_name} - {str(e)}")
    
    def _build_index_for_field(self, source_collection, index_table_name: str, 
                              field_name: str, field_type: FieldType, config) -> Dict[str, int]:
        """为单个字段构建索引"""
        index_collection = self.db[index_table_name]
        
        records_processed = 0
        keywords_created = 0
        batch_keywords = []
        
        try:
            # 获取所有有效记录
            cursor = source_collection.find(
                {field_name: {'$exists': True, '$ne': None, '$ne': ''}},
                {'_id': 1, field_name: 1}
            )
            
            for doc in cursor:
                try:
                    doc_id = doc['_id']
                    field_value = doc.get(field_name)
                    
                    if not field_value:
                        continue
                    
                    # 预处理字段值
                    preprocessed_value = self.text_matcher._apply_preprocessing(field_value, config)
                    if not preprocessed_value:
                        continue
                    
                    # 提取关键词
                    keywords = self.text_matcher._apply_keyword_extraction(preprocessed_value, config)
                    if not keywords:
                        continue
                    
                    # 为每个关键词创建索引记录
                    for keyword in keywords:
                        batch_keywords.append({
                            'doc_id': doc_id,
                            'source_table': source_collection.name,
                            'field_name': field_name,
                            'field_type': field_type.value,
                            'keyword': keyword,
                            'original_value': str(field_value),
                            'preprocessed_value': preprocessed_value,
                            'created_at': time.time()
                        })
                        keywords_created += 1
                    
                    records_processed += 1
                    
                    # 批量插入
                    if len(batch_keywords) >= self.build_config['batch_size']:
                        index_collection.insert_many(batch_keywords)
                        batch_keywords = []
                        
                        if self.build_config['enable_progress_logging'] and records_processed % 5000 == 0:
                            logger.info(f"索引构建进度: {field_name} - {records_processed} 条记录")
                
                except Exception as e:
                    logger.warning(f"处理记录失败: {doc.get('_id')} - {str(e)}")
                    continue
            
            # 插入剩余的关键词
            if batch_keywords:
                index_collection.insert_many(batch_keywords)
            
            # 更新全局统计
            self.build_stats['total_records_processed'] += records_processed
            self.build_stats['total_keywords_created'] += keywords_created
            
            return {
                'records_processed': records_processed,
                'keywords_created': keywords_created
            }
            
        except Exception as e:
            logger.error(f"构建字段索引失败: {field_name} - {str(e)}")
            raise
    
    def _create_database_indexes(self, index_table_name: str):
        """为索引表创建数据库索引"""
        try:
            index_collection = self.db[index_table_name]
            
            # 创建关键词索引（最重要）
            index_collection.create_index([
                ('keyword', ASCENDING),
                ('field_name', ASCENDING)
            ], name='idx_keyword_field', background=True)
            
            # 创建文档ID索引
            index_collection.create_index('doc_id', name='idx_doc_id', background=True)
            
            # 创建复合索引
            if self.build_config['create_compound_indexes']:
                index_collection.create_index([
                    ('source_table', ASCENDING),
                    ('field_name', ASCENDING),
                    ('keyword', ASCENDING)
                ], name='idx_compound', background=True)
            
            logger.debug(f"已创建数据库索引: {index_table_name}")
            
        except Exception as e:
            logger.warning(f"创建数据库索引失败: {index_table_name} - {str(e)}")
    
    def _build_indexes_parallel(self, table_name: str, target_fields: Set[str], 
                               force_rebuild: bool) -> List[Dict]:
        """并行构建索引"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.build_config['max_workers']) as executor:
            future_to_field = {
                executor.submit(self.build_field_index, table_name, field, None, force_rebuild): field
                for field in target_fields
            }
            
            for future in as_completed(future_to_field):
                field = future_to_field[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"并行构建索引失败: {table_name}.{field} - {str(e)}")
                    results.append({
                        'status': 'error',
                        'table_name': table_name,
                        'field_name': field,
                        'error': str(e)
                    })
        
        return results
    
    def _build_indexes_sequential(self, table_name: str, target_fields: Set[str], 
                                 force_rebuild: bool) -> List[Dict]:
        """顺序构建索引"""
        results = []
        
        for field in target_fields:
            try:
                result = self.build_field_index(table_name, field, None, force_rebuild)
                results.append(result)
            except Exception as e:
                logger.error(f"顺序构建索引失败: {table_name}.{field} - {str(e)}")
                results.append({
                    'status': 'error',
                    'table_name': table_name,
                    'field_name': field,
                    'error': str(e)
                })
        
        return results
    
    def build_mapping_indexes(self, mappings: List[Dict], force_rebuild: bool = False) -> Dict[str, Any]:
        """
        根据字段映射批量构建索引
        
        Args:
            mappings: 字段映射列表
            force_rebuild: 是否强制重建
            
        Returns:
            Dict: 批量构建结果
        """
        start_time = time.time()
        
        try:
            logger.info(f"🏗️ 开始根据映射构建索引, 映射数量: {len(mappings)}")
            
            # 按目标表分组
            table_mappings = defaultdict(list)
            for mapping in mappings:
                target_table = mapping.get('target_table')
                if target_table:
                    table_mappings[target_table].append(mapping)
            
            results = {
                'total_tables': len(table_mappings),
                'total_mappings': len(mappings),
                'table_results': [],
                'overall_success_count': 0,
                'overall_error_count': 0,
                'overall_skipped_count': 0,
                'total_build_time': 0.0
            }
            
            # 为每个表构建索引
            for table_name, table_mapping_list in table_mappings.items():
                table_result = self.build_table_indexes(table_name, table_mapping_list, force_rebuild)
                results['table_results'].append(table_result)
                
                results['overall_success_count'] += table_result.get('success_count', 0)
                results['overall_error_count'] += table_result.get('error_count', 0)
                results['overall_skipped_count'] += table_result.get('skipped_count', 0)
            
            results['total_build_time'] = time.time() - start_time
            
            logger.info(f"✅ 映射索引构建完成, "
                       f"表数量: {results['total_tables']}, "
                       f"成功: {results['overall_success_count']}, "
                       f"错误: {results['overall_error_count']}, "
                       f"跳过: {results['overall_skipped_count']}, "
                       f"总耗时: {results['total_build_time']:.2f}s")
            
            return results
            
        except Exception as e:
            logger.error(f"映射索引构建失败: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total_build_time': time.time() - start_time
            }
    
    def get_build_stats(self) -> Dict[str, Any]:
        """获取构建统计信息"""
        return {
            'total_records_processed': self.build_stats['total_records_processed'],
            'total_keywords_created': self.build_stats['total_keywords_created'],
            'total_time_spent': self.build_stats['total_time_spent'],
            'tables_processed': self.build_stats['tables_processed'],
            'fields_processed': self.build_stats['fields_processed'],
            'avg_processing_rate': (
                self.build_stats['total_records_processed'] / 
                max(self.build_stats['total_time_spent'], 0.001)
            )
        }


# 导入defaultdict
from collections import defaultdict
