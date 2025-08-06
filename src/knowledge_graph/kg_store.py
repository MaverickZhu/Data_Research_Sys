"""
知识图谱存储引擎
负责知识图谱的持久化存储、索引和查询
"""

import logging
from typing import Dict, List, Any, Optional, Iterator
from datetime import datetime
import json
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database
from pymongo.collection import Collection
from .kg_models import Entity, Relation, KnowledgeTriple, Ontology, EntityType, RelationType

logger = logging.getLogger(__name__)

class KnowledgeGraphStore:
    """知识图谱存储引擎"""
    
    def __init__(self, db_manager=None, database_name: str = "Unit_Info"):
        """
        初始化存储引擎
        
        Args:
            db_manager: 数据库管理器
            database_name: 数据库名称
        """
        if db_manager:
            self.db = db_manager.get_db()
        else:
            self.client = MongoClient('mongodb://localhost:27017/')
            self.db = self.client[database_name]
        
        # 知识图谱相关集合
        self.entities_collection = self.db['kg_entities']
        self.relations_collection = self.db['kg_relations']
        self.triples_collection = self.db['kg_triples']
        self.ontologies_collection = self.db['kg_ontologies']
        
        # 创建索引
        self._create_indexes()
        
        logger.info("知识图谱存储引擎初始化完成")
    
    def _create_indexes(self) -> None:
        """创建必要的索引"""
        try:
            # 实体索引
            self.entities_collection.create_index([('id', ASCENDING)], unique=True)
            self.entities_collection.create_index([('type', ASCENDING)])
            self.entities_collection.create_index([('label', ASCENDING)])
            self.entities_collection.create_index([('source_table', ASCENDING)])
            
            # 关系索引
            self.relations_collection.create_index([('id', ASCENDING)], unique=True)
            self.relations_collection.create_index([('type', ASCENDING)])
            
            # 三元组索引
            self.triples_collection.create_index([('id', ASCENDING)], unique=True)
            self.triples_collection.create_index([('subject.id', ASCENDING)])
            self.triples_collection.create_index([('object.id', ASCENDING)])
            self.triples_collection.create_index([('predicate.id', ASCENDING)])
            self.triples_collection.create_index([
                ('subject.id', ASCENDING),
                ('predicate.id', ASCENDING),
                ('object.id', ASCENDING)
            ], unique=True)
            
            # 本体索引
            self.ontologies_collection.create_index([('id', ASCENDING)], unique=True)
            self.ontologies_collection.create_index([('name', ASCENDING)])
            
            logger.info("知识图谱索引创建完成")
            
        except Exception as e:
            logger.error(f"创建索引失败: {str(e)}")
    
    # ====== 实体操作 ======
    
    def save_entity(self, entity: Entity) -> bool:
        """
        保存实体
        
        Args:
            entity: 实体对象
            
        Returns:
            bool: 保存是否成功
        """
        try:
            entity.updated_time = datetime.now()
            result = self.entities_collection.replace_one(
                {'id': entity.id},
                entity.to_dict(),
                upsert=True
            )
            
            logger.debug(f"保存实体: {entity.id} - {entity.label}")
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
    
    def close(self) -> None:
        """关闭数据库连接"""
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("知识图谱存储引擎连接已关闭")