"""
知识图谱存储引擎
负责知识图谱的持久化存储、索引和查询
"""

import logging
from typing import Dict, List, Any, Optional, Iterator, Tuple
from datetime import datetime
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT, HASHED
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, DuplicateKeyError
from .kg_models import Entity, Relation, KnowledgeTriple, Ontology, EntityType, RelationType

logger = logging.getLogger(__name__)

class KnowledgeGraphStore:
    """知识图谱存储引擎"""
    
    def __init__(self, db_manager=None, database_name: str = "Unit_Info", config: Dict[str, Any] = None):
        """
        初始化存储引擎
        
        Args:
            db_manager: 数据库管理器
            database_name: 数据库名称
            config: 配置参数
        """
        self.config = config or {}
        
        if db_manager:
            self.db = db_manager.get_db()
        else:
            # 优化连接配置
            client_config = {
                'maxPoolSize': self.config.get('max_pool_size', 100),
                'minPoolSize': self.config.get('min_pool_size', 10),
                'maxIdleTimeMS': self.config.get('max_idle_time_ms', 30000),
                'waitQueueTimeoutMS': self.config.get('wait_queue_timeout_ms', 5000),
                'serverSelectionTimeoutMS': self.config.get('server_selection_timeout_ms', 5000)
            }
            self.client = MongoClient('mongodb://localhost:27017/', **client_config)
            self.db = self.client[database_name]
        
        # 知识图谱相关集合
        self.entities_collection = self.db['kg_entities']
        self.relations_collection = self.db['kg_relations']
        self.triples_collection = self.db['kg_triples']
        self.ontologies_collection = self.db['kg_ontologies']
        
        # 性能优化配置
        self.batch_size = self.config.get('batch_size', 1000)
        self.max_workers = self.config.get('max_workers', 4)
        self.enable_compression = self.config.get('enable_compression', True)
        self.cache_size = self.config.get('cache_size', 10000)
        
        # 内存缓存
        self._entity_cache = {}
        self._relation_cache = {}
        self._cache_stats = {'hits': 0, 'misses': 0}
        
        # 创建优化索引
        self._create_optimized_indexes()
        
        # 性能监控
        self._performance_stats = {
            'operations': 0,
            'total_time': 0.0,
            'avg_time': 0.0
        }
        
        logger.info("知识图谱存储引擎初始化完成（性能优化版）")
    
    def _create_optimized_indexes(self) -> None:
        """创建优化的索引系统"""
        try:
            start_time = time.time()
            
            # 实体索引 - 优化版
            self.entities_collection.create_index([('id', ASCENDING)], unique=True, background=True)
            self.entities_collection.create_index([('type', ASCENDING)], background=True)
            self.entities_collection.create_index([('label', ASCENDING)], background=True)
            self.entities_collection.create_index([('source_table', ASCENDING)], background=True)
            
            # 新增：实体全文搜索索引
            self.entities_collection.create_index([('label', TEXT), ('aliases', TEXT)], 
                                                background=True, name='entity_text_search')
            
            # 新增：实体置信度和时间索引
            self.entities_collection.create_index([('confidence', DESCENDING)], background=True)
            self.entities_collection.create_index([('created_time', DESCENDING)], background=True)
            self.entities_collection.create_index([('updated_time', DESCENDING)], background=True)
            
            # 新增：复合索引优化查询
            self.entities_collection.create_index([
                ('type', ASCENDING), 
                ('source_table', ASCENDING)
            ], background=True, name='entity_type_table')
            
            self.entities_collection.create_index([
                ('type', ASCENDING), 
                ('confidence', DESCENDING)
            ], background=True, name='entity_type_confidence')
            
            # 关系索引 - 优化版
            self.relations_collection.create_index([('id', ASCENDING)], unique=True, background=True)
            self.relations_collection.create_index([('type', ASCENDING)], background=True)
            self.relations_collection.create_index([('confidence', DESCENDING)], background=True)
            
            # 三元组索引 - 高性能优化
            self.triples_collection.create_index([('id', ASCENDING)], unique=True, background=True)
            
            # 核心查询索引
            self.triples_collection.create_index([('subject.id', ASCENDING)], background=True)
            self.triples_collection.create_index([('object.id', ASCENDING)], background=True)
            self.triples_collection.create_index([('predicate.id', ASCENDING)], background=True)
            
            # 三元组唯一性索引
            self.triples_collection.create_index([
                ('subject.id', ASCENDING),
                ('predicate.id', ASCENDING),
                ('object.id', ASCENDING)
            ], unique=True, background=True, name='triple_uniqueness')
            
            # 新增：图查询优化索引
            self.triples_collection.create_index([
                ('subject.id', ASCENDING),
                ('predicate.type', ASCENDING)
            ], background=True, name='subject_relation_type')
            
            self.triples_collection.create_index([
                ('object.id', ASCENDING),
                ('predicate.type', ASCENDING)
            ], background=True, name='object_relation_type')
            
            # 新增：置信度和来源索引
            self.triples_collection.create_index([('confidence', DESCENDING)], background=True)
            self.triples_collection.create_index([('source', ASCENDING)], background=True)
            self.triples_collection.create_index([('created_time', DESCENDING)], background=True)
            
            # 新增：图遍历优化索引
            self.triples_collection.create_index([
                ('subject.type', ASCENDING),
                ('predicate.type', ASCENDING),
                ('object.type', ASCENDING)
            ], background=True, name='triple_types_pattern')
            
            # 本体索引
            self.ontologies_collection.create_index([('id', ASCENDING)], unique=True, background=True)
            self.ontologies_collection.create_index([('name', ASCENDING)], background=True)
            self.ontologies_collection.create_index([('version', ASCENDING)], background=True)
            
            # 新增：分片键索引（为分布式扩展准备）
            if self.config.get('enable_sharding', False):
                self.triples_collection.create_index([('subject.id', HASHED)], background=True)
                self.entities_collection.create_index([('id', HASHED)], background=True)
            
            elapsed_time = time.time() - start_time
            logger.info(f"优化索引创建完成，耗时: {elapsed_time:.2f}秒")
            
        except Exception as e:
            logger.error(f"创建优化索引失败: {str(e)}")
    
    def _create_indexes(self) -> None:
        """兼容性方法，调用优化版本"""
        self._create_optimized_indexes()
    
    # ====== 实体操作 ======
    
    # ====== 性能监控和缓存 ======
    
    def _track_performance(self, operation_time: float) -> None:
        """跟踪性能统计"""
        self._performance_stats['operations'] += 1
        self._performance_stats['total_time'] += operation_time
        self._performance_stats['avg_time'] = (
            self._performance_stats['total_time'] / self._performance_stats['operations']
        )
    
    def _get_from_cache(self, cache_dict: Dict, key: str) -> Optional[Any]:
        """从缓存获取数据"""
        if key in cache_dict:
            self._cache_stats['hits'] += 1
            return cache_dict[key]
        else:
            self._cache_stats['misses'] += 1
            return None
    
    def _set_to_cache(self, cache_dict: Dict, key: str, value: Any) -> None:
        """设置缓存数据"""
        if len(cache_dict) >= self.cache_size:
            # 简单的LRU：删除最旧的项
            oldest_key = next(iter(cache_dict))
            del cache_dict[oldest_key]
        
        cache_dict[key] = value
    
    def clear_cache(self) -> None:
        """清空缓存"""
        self._entity_cache.clear()
        self._relation_cache.clear()
        self._cache_stats = {'hits': 0, 'misses': 0}
        logger.info("缓存已清空")
    
    def save_entity(self, entity: Entity) -> bool:
        """
        保存实体（优化版）
        
        Args:
            entity: 实体对象
            
        Returns:
            bool: 保存是否成功
        """
        start_time = time.time()
        
        try:
            entity.updated_time = datetime.now()
            
            # 数据压缩（如果启用）
            entity_dict = entity.to_dict()
            if self.enable_compression:
                entity_dict = self._compress_entity_data(entity_dict)
            
            result = self.entities_collection.replace_one(
                {'id': entity.id},
                entity_dict,
                upsert=True
            )
            
            # 更新缓存
            if result.acknowledged:
                self._set_to_cache(self._entity_cache, entity.id, entity)
            
            operation_time = time.time() - start_time
            self._track_performance(operation_time)
            
            logger.debug(f"保存实体: {entity.id} - {entity.label} (耗时: {operation_time:.3f}s)")
            return result.acknowledged
            
        except Exception as e:
            logger.error(f"保存实体失败: {str(e)}")
            return False
    
    def get_entity(self, entity_id: str) -> Optional[Entity]:
        """
        根据ID获取实体
        
        Args:
            entity_id: 实体ID
            
        Returns:
            Optional[Entity]: 实体对象
        """
        try:
            doc = self.entities_collection.find_one({'id': entity_id})
            if doc:
                return Entity.from_dict(doc)
            return None
            
        except Exception as e:
            logger.error(f"获取实体失败: {str(e)}")
            return None
    
    def find_entities(self, entity_type: EntityType = None,
                     label_pattern: str = None,
                     source_table: str = None,
                     limit: int = 100) -> List[Entity]:
        """
        查找实体
        
        Args:
            entity_type: 实体类型
            label_pattern: 标签模式（支持正则）
            source_table: 源表名
            limit: 限制数量
            
        Returns:
            List[Entity]: 实体列表
        """
        try:
            query = {}
            
            if entity_type:
                query['type'] = entity_type.value
            
            if label_pattern:
                query['label'] = {'$regex': label_pattern, '$options': 'i'}
            
            if source_table:
                query['source_table'] = source_table
            
            docs = self.entities_collection.find(query).limit(limit)
            return [Entity.from_dict(doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"查找实体失败: {str(e)}")
            return []
    
    def delete_entity(self, entity_id: str) -> bool:
        """
        删除实体
        
        Args:
            entity_id: 实体ID
            
        Returns:
            bool: 删除是否成功
        """
        try:
            # 删除相关的三元组
            self.triples_collection.delete_many({
                '$or': [
                    {'subject.id': entity_id},
                    {'object.id': entity_id}
                ]
            })
            
            # 删除实体
            result = self.entities_collection.delete_one({'id': entity_id})
            
            logger.debug(f"删除实体: {entity_id}")
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"删除实体失败: {str(e)}")
            return False
    
    # ====== 关系操作 ======
    
    def save_relation(self, relation: Relation) -> bool:
        """
        保存关系
        
        Args:
            relation: 关系对象
            
        Returns:
            bool: 保存是否成功
        """
        try:
            relation.updated_time = datetime.now()
            result = self.relations_collection.replace_one(
                {'id': relation.id},
                relation.to_dict(),
                upsert=True
            )
            
            logger.debug(f"保存关系: {relation.id} - {relation.label}")
            return result.acknowledged
            
        except Exception as e:
            logger.error(f"保存关系失败: {str(e)}")
            return False
    
    def get_relation(self, relation_id: str) -> Optional[Relation]:
        """
        根据ID获取关系
        
        Args:
            relation_id: 关系ID
            
        Returns:
            Optional[Relation]: 关系对象
        """
        try:
            doc = self.relations_collection.find_one({'id': relation_id})
            if doc:
                return Relation.from_dict(doc)
            return None
            
        except Exception as e:
            logger.error(f"获取关系失败: {str(e)}")
            return None
    
    def find_relations(self, relation_type: RelationType = None,
                      domain: EntityType = None,
                      range: EntityType = None) -> List[Relation]:
        """
        查找关系
        
        Args:
            relation_type: 关系类型
            domain: 定义域
            range: 值域
            
        Returns:
            List[Relation]: 关系列表
        """
        try:
            query = {}
            
            if relation_type:
                query['type'] = relation_type.value
            
            if domain:
                query['domain'] = domain.value
            
            if range:
                query['range'] = range.value
            
            docs = self.relations_collection.find(query)
            return [Relation.from_dict(doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"查找关系失败: {str(e)}")
            return []
    
    # ====== 三元组操作 ======
    
    def save_triple(self, triple: KnowledgeTriple) -> bool:
        """
        保存三元组
        
        Args:
            triple: 三元组对象
            
        Returns:
            bool: 保存是否成功
        """
        try:
            if not triple.is_valid():
                logger.warning(f"三元组无效，跳过保存: {triple.id}")
                return False
            
            triple.updated_time = datetime.now()
            result = self.triples_collection.replace_one(
                {'id': triple.id},
                triple.to_dict(),
                upsert=True
            )
            
            logger.debug(f"保存三元组: {triple.id}")
            return result.acknowledged
            
        except Exception as e:
            logger.error(f"保存三元组失败: {str(e)}")
            return False
    
    def get_triple(self, triple_id: str) -> Optional[KnowledgeTriple]:
        """
        根据ID获取三元组
        
        Args:
            triple_id: 三元组ID
            
        Returns:
            Optional[KnowledgeTriple]: 三元组对象
        """
        try:
            doc = self.triples_collection.find_one({'id': triple_id})
            if doc:
                return KnowledgeTriple.from_dict(doc)
            return None
            
        except Exception as e:
            logger.error(f"获取三元组失败: {str(e)}")
            return None
    
    def find_triples(self, subject_id: str = None,
                    predicate_id: str = None,
                    object_id: str = None,
                    limit: int = 100) -> List[KnowledgeTriple]:
        """
        查找三元组
        
        Args:
            subject_id: 主语实体ID
            predicate_id: 谓语关系ID
            object_id: 宾语实体ID
            limit: 限制数量
            
        Returns:
            List[KnowledgeTriple]: 三元组列表
        """
        try:
            query = {}
            
            if subject_id:
                query['subject.id'] = subject_id
            
            if predicate_id:
                query['predicate.id'] = predicate_id
            
            if object_id:
                query['object.id'] = object_id
            
            docs = self.triples_collection.find(query).limit(limit)
            return [KnowledgeTriple.from_dict(doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"查找三元组失败: {str(e)}")
            return []
    
    def find_entity_relations(self, entity_id: str,
                            relation_type: RelationType = None,
                            direction: str = 'both') -> List[KnowledgeTriple]:
        """
        查找实体的相关关系
        
        Args:
            entity_id: 实体ID
            relation_type: 关系类型筛选
            direction: 方向 ('incoming', 'outgoing', 'both')
            
        Returns:
            List[KnowledgeTriple]: 相关三元组列表
        """
        try:
            query_conditions = []
            
            if direction in ['outgoing', 'both']:
                condition = {'subject.id': entity_id}
                if relation_type:
                    condition['predicate.type'] = relation_type.value
                query_conditions.append(condition)
            
            if direction in ['incoming', 'both']:
                condition = {'object.id': entity_id}
                if relation_type:
                    condition['predicate.type'] = relation_type.value
                query_conditions.append(condition)
            
            if not query_conditions:
                return []
            
            query = {'$or': query_conditions} if len(query_conditions) > 1 else query_conditions[0]
            
            docs = self.triples_collection.find(query)
            return [KnowledgeTriple.from_dict(doc) for doc in docs]
            
        except Exception as e:
            logger.error(f"查找实体关系失败: {str(e)}")
            return []
    
    def batch_save_triples(self, triples: List[KnowledgeTriple]) -> int:
        """
        批量保存三元组
        
        Args:
            triples: 三元组列表
            
        Returns:
            int: 成功保存的数量
        """
        try:
            successful_count = 0
            operations = []
            
            for triple in triples:
                if not triple.is_valid():
                    logger.warning(f"跳过无效三元组: {triple.id}")
                    continue
                
                triple.updated_time = datetime.now()
                operations.append({
                    'replaceOne': {
                        'filter': {'id': triple.id},
                        'replacement': triple.to_dict(),
                        'upsert': True
                    }
                })
            
            if operations:
                result = self.triples_collection.bulk_write(operations)
                successful_count = result.upserted_count + result.modified_count
            
            logger.info(f"批量保存三元组: {successful_count}/{len(triples)}")
            return successful_count
            
        except Exception as e:
            logger.error(f"批量保存三元组失败: {str(e)}")
            return 0
    
    # ====== 本体操作 ======
    
    def save_ontology(self, ontology: Ontology) -> bool:
        """
        保存本体
        
        Args:
            ontology: 本体对象
            
        Returns:
            bool: 保存是否成功
        """
        try:
            ontology.updated_time = datetime.now()
            result = self.ontologies_collection.replace_one(
                {'id': ontology.id},
                ontology.to_dict(),
                upsert=True
            )
            
            logger.debug(f"保存本体: {ontology.id} - {ontology.name}")
            return result.acknowledged
            
        except Exception as e:
            logger.error(f"保存本体失败: {str(e)}")
            return False
    
    def get_ontology(self, ontology_id: str) -> Optional[Ontology]:
        """
        根据ID获取本体
        
        Args:
            ontology_id: 本体ID
            
        Returns:
            Optional[Ontology]: 本体对象
        """
        try:
            doc = self.ontologies_collection.find_one({'id': ontology_id})
            if doc:
                return Ontology.from_dict(doc)
            return None
            
        except Exception as e:
            logger.error(f"获取本体失败: {str(e)}")
            return None
    
    # ====== 统计和分析 ======
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取知识图谱统计信息
        
        Returns:
            Dict: 统计信息
        """
        try:
            stats = {
                'entities': {
                    'total': self.entities_collection.count_documents({}),
                    'by_type': {}
                },
                'relations': {
                    'total': self.relations_collection.count_documents({}),
                    'by_type': {}
                },
                'triples': {
                    'total': self.triples_collection.count_documents({})
                },
                'ontologies': {
                    'total': self.ontologies_collection.count_documents({})
                }
            }
            
            # 按类型统计实体
            entity_type_pipeline = [
                {'$group': {'_id': '$type', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]
            entity_types = list(self.entities_collection.aggregate(entity_type_pipeline))
            stats['entities']['by_type'] = {item['_id']: item['count'] for item in entity_types}
            
            # 按类型统计关系
            relation_type_pipeline = [
                {'$group': {'_id': '$type', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]
            relation_types = list(self.relations_collection.aggregate(relation_type_pipeline))
            stats['relations']['by_type'] = {item['_id']: item['count'] for item in relation_types}
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            return {}
    
    def clear_knowledge_graph(self, confirm: bool = False) -> bool:
        """
        清空知识图谱（谨慎使用）
        
        Args:
            confirm: 确认标志
            
        Returns:
            bool: 操作是否成功
        """
        if not confirm:
            logger.warning("清空知识图谱需要确认标志")
            return False
        
        try:
            self.triples_collection.delete_many({})
            self.entities_collection.delete_many({})
            self.relations_collection.delete_many({})
            
            logger.info("知识图谱已清空")
            return True
            
        except Exception as e:
            logger.error(f"清空知识图谱失败: {str(e)}")
            return False
    
    # ====== 批量操作优化 ======
    
    def batch_save_entities(self, entities: List[Entity]) -> int:
        """
        批量保存实体（高性能版）
        
        Args:
            entities: 实体列表
            
        Returns:
            int: 成功保存的实体数量
        """
        if not entities:
            return 0
        
        start_time = time.time()
        saved_count = 0
        
        try:
            # 分批处理
            for i in range(0, len(entities), self.batch_size):
                batch = entities[i:i + self.batch_size]
                batch_docs = []
                
                for entity in batch:
                    entity.updated_time = datetime.now()
                    entity_dict = entity.to_dict()
                    
                    if self.enable_compression:
                        entity_dict = self._compress_entity_data(entity_dict)
                    
                    batch_docs.append(entity_dict)
                
                # 使用批量写入
                try:
                    from pymongo import ReplaceOne
                    operations = [
                        ReplaceOne(
                            filter={'id': doc['id']},
                            replacement=doc,
                            upsert=True
                        ) for doc in batch_docs
                    ]
                    result = self.entities_collection.bulk_write(operations, ordered=False)
                    
                    saved_count += result.upserted_count + result.modified_count
                    
                    # 批量更新缓存
                    for entity in batch:
                        self._set_to_cache(self._entity_cache, entity.id, entity)
                    
                except BulkWriteError as bwe:
                    logger.warning(f"批量写入部分失败: {bwe.details}")
                    saved_count += len(batch_docs) - len(bwe.details.get('writeErrors', []))
            
            operation_time = time.time() - start_time
            self._track_performance(operation_time)
            
            logger.info(f"批量保存实体完成: {saved_count}/{len(entities)} (耗时: {operation_time:.3f}s)")
            return saved_count
            
        except Exception as e:
            logger.error(f"批量保存实体失败: {str(e)}")
            return saved_count
    
    def batch_save_triples_optimized(self, triples: List[KnowledgeTriple]) -> int:
        """
        批量保存三元组（超高性能版）
        
        Args:
            triples: 三元组列表
            
        Returns:
            int: 成功保存的三元组数量
        """
        if not triples:
            return 0
        
        start_time = time.time()
        saved_count = 0
        
        try:
            # 使用多线程并行处理
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 将三元组分成多个批次
                batches = [
                    triples[i:i + self.batch_size] 
                    for i in range(0, len(triples), self.batch_size)
                ]
                
                # 提交批处理任务
                future_to_batch = {
                    executor.submit(self._process_triple_batch, batch): batch 
                    for batch in batches
                }
                
                # 收集结果
                for future in as_completed(future_to_batch):
                    try:
                        batch_saved = future.result()
                        saved_count += batch_saved
                    except Exception as e:
                        logger.error(f"批处理失败: {e}")
            
            operation_time = time.time() - start_time
            self._track_performance(operation_time)
            
            logger.info(f"批量保存三元组完成: {saved_count}/{len(triples)} (耗时: {operation_time:.3f}s)")
            return saved_count
            
        except Exception as e:
            logger.error(f"批量保存三元组失败: {str(e)}")
            return saved_count
    
    def _process_triple_batch(self, batch: List[KnowledgeTriple]) -> int:
        """处理三元组批次"""
        try:
            batch_docs = []
            
            for triple in batch:
                triple.created_time = datetime.now()
                triple_dict = triple.to_dict()
                
                if self.enable_compression:
                    triple_dict = self._compress_triple_data(triple_dict)
                
                batch_docs.append(triple_dict)
            
            # 批量写入
            from pymongo import ReplaceOne
            operations = [
                ReplaceOne(
                    filter={'id': doc['id']},
                    replacement=doc,
                    upsert=True
                ) for doc in batch_docs
            ]
            result = self.triples_collection.bulk_write(operations, ordered=False)
            
            return result.upserted_count + result.modified_count
            
        except Exception as e:
            logger.error(f"处理三元组批次失败: {e}")
            return 0
    
    # ====== 数据压缩优化 ======
    
    def _compress_entity_data(self, entity_dict: Dict[str, Any]) -> Dict[str, Any]:
        """压缩实体数据"""
        if not self.enable_compression:
            return entity_dict
        
        try:
            # 压缩属性数据（移除空值和重复数据）
            if 'properties' in entity_dict:
                properties = entity_dict['properties']
                compressed_properties = {
                    k: v for k, v in properties.items() 
                    if v is not None and str(v).strip() != ''
                }
                entity_dict['properties'] = compressed_properties
            
            # 压缩别名列表（去重）
            if 'aliases' in entity_dict:
                aliases = entity_dict['aliases']
                entity_dict['aliases'] = list(set(aliases)) if aliases else []
            
            return entity_dict
            
        except Exception as e:
            logger.debug(f"实体数据压缩失败: {e}")
            return entity_dict
    
    def _compress_triple_data(self, triple_dict: Dict[str, Any]) -> Dict[str, Any]:
        """压缩三元组数据"""
        if not self.enable_compression:
            return triple_dict
        
        try:
            # 压缩证据列表（去重和截断）
            if 'evidence' in triple_dict:
                evidence = triple_dict['evidence']
                if evidence:
                    # 去重并限制长度
                    unique_evidence = list(set(evidence))[:10]  # 最多保留10条证据
                    triple_dict['evidence'] = unique_evidence
            
            return triple_dict
            
        except Exception as e:
            logger.debug(f"三元组数据压缩失败: {e}")
            return triple_dict
    
    # ====== 图查询优化 ======
    
    def find_entities_by_text(self, text: str, limit: int = 50) -> List[Entity]:
        """
        全文搜索实体（使用优化索引）
        
        Args:
            text: 搜索文本
            limit: 限制数量
            
        Returns:
            List[Entity]: 实体列表
        """
        start_time = time.time()
        
        try:
            # 使用全文搜索索引
            docs = self.entities_collection.find(
                {'$text': {'$search': text}},
                {'score': {'$meta': 'textScore'}}
            ).sort([('score', {'$meta': 'textScore'})]).limit(limit)
            
            entities = [Entity.from_dict(doc) for doc in docs]
            
            operation_time = time.time() - start_time
            self._track_performance(operation_time)
            
            logger.debug(f"全文搜索实体: {len(entities)} 个结果 (耗时: {operation_time:.3f}s)")
            return entities
            
        except Exception as e:
            logger.error(f"全文搜索实体失败: {str(e)}")
            return []
    
    def find_high_confidence_triples(self, min_confidence: float = 0.8, 
                                   limit: int = 100) -> List[KnowledgeTriple]:
        """
        查找高置信度三元组（使用优化索引）
        
        Args:
            min_confidence: 最小置信度
            limit: 限制数量
            
        Returns:
            List[KnowledgeTriple]: 三元组列表
        """
        start_time = time.time()
        
        try:
            docs = self.triples_collection.find(
                {'confidence': {'$gte': min_confidence}}
            ).sort([('confidence', DESCENDING)]).limit(limit)
            
            triples = [KnowledgeTriple.from_dict(doc) for doc in docs]
            
            operation_time = time.time() - start_time
            self._track_performance(operation_time)
            
            logger.debug(f"查找高置信度三元组: {len(triples)} 个结果 (耗时: {operation_time:.3f}s)")
            return triples
            
        except Exception as e:
            logger.error(f"查找高置信度三元组失败: {str(e)}")
            return []
    
    # ====== 性能统计 ======
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计信息"""
        cache_hit_rate = 0.0
        total_cache_requests = self._cache_stats['hits'] + self._cache_stats['misses']
        
        if total_cache_requests > 0:
            cache_hit_rate = self._cache_stats['hits'] / total_cache_requests
        
        return {
            'operations_count': self._performance_stats['operations'],
            'total_time': self._performance_stats['total_time'],
            'average_time': self._performance_stats['avg_time'],
            'cache_hit_rate': cache_hit_rate,
            'cache_stats': self._cache_stats.copy(),
            'cache_size': {
                'entities': len(self._entity_cache),
                'relations': len(self._relation_cache),
                'max_size': self.cache_size
            },
            'config': {
                'batch_size': self.batch_size,
                'max_workers': self.max_workers,
                'compression_enabled': self.enable_compression
            }
        }
    
    def optimize_collections(self) -> Dict[str, Any]:
        """优化集合性能"""
        start_time = time.time()
        results = {}
        
        try:
            # 重建索引
            logger.info("开始重建索引...")
            self._create_optimized_indexes()
            results['indexes_rebuilt'] = True
            
            # 压缩集合
            if self.config.get('enable_compaction', False):
                logger.info("开始压缩集合...")
                self.db.command('compact', 'kg_entities')
                self.db.command('compact', 'kg_relations') 
                self.db.command('compact', 'kg_triples')
                results['collections_compacted'] = True
            
            # 清理缓存
            self.clear_cache()
            results['cache_cleared'] = True
            
            optimization_time = time.time() - start_time
            results['optimization_time'] = optimization_time
            
            logger.info(f"集合优化完成，耗时: {optimization_time:.2f}秒")
            return results
            
        except Exception as e:
            logger.error(f"集合优化失败: {str(e)}")
            return {'error': str(e)}
    
    def close(self) -> None:
        """关闭数据库连接"""
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("知识图谱存储引擎连接已关闭")