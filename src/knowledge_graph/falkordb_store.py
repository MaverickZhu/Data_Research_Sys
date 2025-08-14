#!/usr/bin/env python3
"""
FalkorDB知识图谱存储引擎
高性能图数据库后端，专为知识图谱优化
"""

import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from falkordb import FalkorDB
from .kg_models import Entity, Relation, KnowledgeTriple, EntityType, RelationType

logger = logging.getLogger(__name__)

class FalkorDBStore:
    """FalkorDB知识图谱存储引擎（支持双重存储模式）"""
    
    def __init__(self, host='localhost', port=16379, graph_name='knowledge_graph', project_name=None):
        """
        初始化FalkorDB存储引擎
        
        Args:
            host: FalkorDB主机地址
            port: FalkorDB端口
            graph_name: 图数据库名称
            project_name: 项目名称（用于双重存储）
        """
        self.host = host
        self.port = port
        self.graph_name = graph_name
        self.project_name = project_name
        self.db = None
        self.graph = None
        
        # 双重存储模式
        self.global_graph = None  # 全局图
        self.project_graph = None  # 项目图
        
        # 性能统计
        self.stats = {
            'entities_stored': 0,
            'relations_stored': 0,
            'triples_stored': 0,
            'queries_executed': 0,
            'last_operation_time': None
        }
        
        self._connect()
        logger.info(f"FalkorDB知识图谱存储引擎初始化完成（图: {graph_name}，项目: {project_name or '全局'}）")
    
    def _connect(self):
        """连接到FalkorDB（支持双重存储）"""
        try:
            self.db = FalkorDB(host=self.host, port=self.port)
            
            # 连接主图（默认或全局图）
            self.graph = self.db.select_graph(self.graph_name)
            
            # 如果有项目名称，设置双重存储
            if self.project_name:
                # 全局图：所有数据+项目标签
                self.global_graph = self.db.select_graph('knowledge_graph_global')
                
                # 项目图：单项目数据
                project_graph_name = f"knowledge_graph_project_{self.project_name}"
                self.project_graph = self.db.select_graph(project_graph_name)
                
                # 测试连接
                result = self.global_graph.query("RETURN 'connected' AS status")
                if result.result_set and result.result_set[0][0] == 'connected':
                    logger.info(f"FalkorDB双重存储连接成功: 全局图=knowledge_graph_global, 项目图={project_graph_name}")
                else:
                    raise Exception("双重存储连接测试失败")
            else:
                # 测试连接
                result = self.graph.query("RETURN 'connected' AS status")
                if result.result_set and result.result_set[0][0] == 'connected':
                    logger.info(f"FalkorDB连接成功: {self.host}:{self.port}/{self.graph_name}")
                else:
                    raise Exception("连接测试失败")
                
        except Exception as e:
            logger.error(f"FalkorDB连接失败: {e}")
            raise
    
    def create_indexes(self):
        """创建图数据库索引以优化查询性能"""
        try:
            # 为实体创建索引
            entity_indexes = [
                "CREATE INDEX ON :Entity(id)",
                "CREATE INDEX ON :Entity(label)", 
                "CREATE INDEX ON :Entity(type)",
                "CREATE INDEX ON :Organization(label)",
                "CREATE INDEX ON :Person(label)",
                "CREATE INDEX ON :Location(label)",
                "CREATE INDEX ON :Identifier(label)"
            ]
            
            # 为关系创建索引
            relation_indexes = [
                "CREATE INDEX ON :Relation(id)",
                "CREATE INDEX ON :Relation(type)",
                "CREATE INDEX ON :Relation(label)"
            ]
            
            all_indexes = entity_indexes + relation_indexes
            
            for index_query in all_indexes:
                try:
                    self.graph.query(index_query)
                except Exception as e:
                    # 索引可能已存在，忽略错误
                    if "already exists" not in str(e).lower():
                        logger.warning(f"创建索引失败: {index_query} - {e}")
            
            logger.info(f"图数据库索引创建完成，共创建 {len(all_indexes)} 个索引")
            
        except Exception as e:
            logger.error(f"创建索引失败: {e}")
    
    def save_entity(self, entity: Entity) -> bool:
        """
        保存单个实体到FalkorDB（支持双重存储）
        
        Args:
            entity: 实体对象
            
        Returns:
            bool: 保存是否成功
        """
        try:
            success = True
            
            # 如果启用双重存储模式
            if self.project_name and self.global_graph and self.project_graph:
                # 1. 保存到全局图（带项目标签）
                global_cypher = f"""
                MERGE (e:{entity.type.value} {{entity_id: $entity_id}})
                SET e.label = $label,
                    e.type = $type,
                    e.project_name = $project_name,
                    e.properties = $properties,
                    e.aliases = $aliases,
                    e.source_table = $source_table,
                    e.source_column = $source_column,
                    e.source_record_id = $source_record_id,
                    e.confidence = $confidence,
                    e.created_time = $created_time,
                    e.updated_time = $updated_time
                RETURN e.entity_id
                """
                
                global_params = {
                    'entity_id': entity.id,
                    'label': entity.label,
                    'type': entity.type.value,
                    'project_name': self.project_name,
                    'properties': str(entity.properties) if entity.properties else '',
                    'aliases': str(entity.aliases) if entity.aliases else '',
                    'source_table': entity.source_table or '',
                    'source_column': entity.source_column or '',
                    'source_record_id': entity.source_record_id or '',
                    'confidence': float(entity.confidence),
                    'created_time': entity.created_time.isoformat() if entity.created_time else '',
                    'updated_time': entity.updated_time.isoformat() if entity.updated_time else ''
                }
                
                try:
                    self.global_graph.query(global_cypher, global_params)
                except Exception as e:
                    logger.error(f"保存实体到全局图失败: {entity.id} - {e}")
                    success = False
                
                # 2. 保存到项目图（不带项目标签）
                project_cypher = f"""
                MERGE (e:{entity.type.value} {{entity_id: $entity_id}})
                SET e.label = $label,
                    e.type = $type,
                    e.properties = $properties,
                    e.aliases = $aliases,
                    e.source_table = $source_table,
                    e.source_column = $source_column,
                    e.source_record_id = $source_record_id,
                    e.confidence = $confidence,
                    e.created_time = $created_time,
                    e.updated_time = $updated_time
                RETURN e.entity_id
                """
                
                project_params = {
                    'entity_id': entity.id,
                    'label': entity.label,
                    'type': entity.type.value,
                    'properties': str(entity.properties) if entity.properties else '',
                    'aliases': str(entity.aliases) if entity.aliases else '',
                    'source_table': entity.source_table or '',
                    'source_column': entity.source_column or '',
                    'source_record_id': entity.source_record_id or '',
                    'confidence': float(entity.confidence),
                    'created_time': entity.created_time.isoformat() if entity.created_time else '',
                    'updated_time': entity.updated_time.isoformat() if entity.updated_time else ''
                }
                
                try:
                    self.project_graph.query(project_cypher, project_params)
                except Exception as e:
                    logger.error(f"保存实体到项目图失败: {entity.id} - {e}")
                    success = False
            else:
                # 单图模式：保存到默认图
                cypher_query = f"""
                MERGE (e:{entity.type.value} {{entity_id: $entity_id}})
                SET e.label = $label,
                    e.type = $type,
                    e.properties = $properties,
                    e.aliases = $aliases,
                    e.source_table = $source_table,
                    e.source_column = $source_column,
                    e.source_record_id = $source_record_id,
                    e.confidence = $confidence,
                    e.created_time = $created_time,
                    e.updated_time = $updated_time
                RETURN e.entity_id
                """
                
                params = {
                    'entity_id': entity.id,
                    'label': entity.label,
                    'type': entity.type.value,
                    'properties': str(entity.properties) if entity.properties else '',
                    'aliases': str(entity.aliases) if entity.aliases else '',
                    'source_table': entity.source_table or '',
                    'source_column': entity.source_column or '',
                    'source_record_id': entity.source_record_id or '',
                    'confidence': float(entity.confidence),
                    'created_time': entity.created_time.isoformat() if entity.created_time else '',
                    'updated_time': entity.updated_time.isoformat() if entity.updated_time else ''
                }
                
                try:
                    self.graph.query(cypher_query, params)
                except Exception as e:
                    logger.error(f"保存实体失败: {entity.id} - {e}")
                    success = False
            
            if success:
                self.stats['entities_stored'] += 1
                self.stats['queries_executed'] += 1
                self.stats['last_operation_time'] = datetime.now()
            
            return success
            
        except Exception as e:
            logger.error(f"保存实体失败: {entity.id} - {e}")
            return False
    
    def get_all_projects(self) -> List[Dict[str, Any]]:
        """
        获取所有项目列表
        
        Returns:
            List[Dict]: 项目信息列表
        """
        try:
            projects = []
            
            # 从全局图获取所有项目
            if self.db:
                # 获取所有图名称
                graphs_info = []
                
                # 检查全局图
                try:
                    global_graph = self.db.select_graph('knowledge_graph_global')
                    result = global_graph.query("MATCH (n) RETURN DISTINCT n.project_name AS project_name, count(n) AS entity_count")
                    
                    for record in result.result_set:
                        if record[0]:  # 项目名称不为空
                            projects.append({
                                'project_name': record[0],
                                'entity_count': record[1],
                                'graph_type': 'global',
                                'graph_name': 'knowledge_graph_global'
                            })
                except:
                    pass
                
                # 检查项目图（通过图名称模式匹配）
                # 注意：FalkorDB没有直接的列出所有图的API，这里需要根据实际情况调整
                known_projects = set()
                for project_info in projects:
                    project_name = project_info['project_name']
                    if project_name not in known_projects:
                        try:
                            project_graph_name = f"knowledge_graph_project_{project_name}"
                            project_graph = self.db.select_graph(project_graph_name)
                            result = project_graph.query("MATCH (n) RETURN count(n) AS entity_count")
                            
                            if result.result_set and len(result.result_set) > 0:
                                entity_count = result.result_set[0][0] if result.result_set[0] else 0
                                projects.append({
                                    'project_name': project_name,
                                    'entity_count': entity_count,
                                    'graph_type': 'project',
                                    'graph_name': project_graph_name
                                })
                                known_projects.add(project_name)
                        except:
                            pass
            
            # 去重并排序
            unique_projects = {}
            for project in projects:
                name = project['project_name']
                if name not in unique_projects:
                    unique_projects[name] = {
                        'project_name': name,
                        'global_entities': 0,
                        'project_entities': 0,
                        'has_global_graph': False,
                        'has_project_graph': False
                    }
                
                if project['graph_type'] == 'global':
                    unique_projects[name]['global_entities'] = project['entity_count']
                    unique_projects[name]['has_global_graph'] = True
                else:
                    unique_projects[name]['project_entities'] = project['entity_count']
                    unique_projects[name]['has_project_graph'] = True
            
            return list(unique_projects.values())
            
        except Exception as e:
            logger.error(f"获取项目列表失败: {e}")
            return []
    
    def query_entities_by_project(self, project_name: str = None, entity_type: str = None, 
                                 limit: int = 100, offset: int = 0, use_global_graph: bool = False) -> List[Dict[str, Any]]:
        """
        按项目查询实体
        
        Args:
            project_name: 项目名称（None表示查询所有项目）
            entity_type: 实体类型过滤
            limit: 限制数量
            offset: 偏移量
            use_global_graph: 是否使用全局图
            
        Returns:
            List[Dict]: 实体列表
        """
        try:
            entities = []
            
            if use_global_graph and project_name:
                # 从全局图查询特定项目
                graph = self.db.select_graph('knowledge_graph_global')
                cypher = "MATCH (e) WHERE e.project_name = $project_name"
                params = {'project_name': project_name}
                
                if entity_type:
                    cypher = f"MATCH (e:{entity_type}) WHERE e.project_name = $project_name"
                
                cypher += f" RETURN e.entity_id, e.label, e.type, e.confidence, e.project_name SKIP {offset} LIMIT {limit}"
                
            elif project_name and not use_global_graph:
                # 从项目图查询
                project_graph_name = f"knowledge_graph_project_{project_name}"
                graph = self.db.select_graph(project_graph_name)
                cypher = "MATCH (e)"
                params = {}
                
                if entity_type:
                    cypher = f"MATCH (e:{entity_type})"
                
                cypher += f" RETURN e.entity_id, e.label, e.type, e.confidence SKIP {offset} LIMIT {limit}"
                
            else:
                # 查询所有项目（从全局图）
                graph = self.db.select_graph('knowledge_graph_global')
                cypher = "MATCH (e)"
                params = {}
                
                if entity_type:
                    cypher = f"MATCH (e:{entity_type})"
                
                cypher += f" RETURN e.entity_id, e.label, e.type, e.confidence, e.project_name SKIP {offset} LIMIT {limit}"
            
            result = graph.query(cypher, params)
            
            for record in result.result_set:
                entity_data = {
                    'id': record[0],
                    'label': record[1],
                    'type': record[2],
                    'confidence': record[3]
                }
                
                # 如果查询结果包含项目名称
                if len(record) > 4:
                    entity_data['project_name'] = record[4]
                elif project_name:
                    entity_data['project_name'] = project_name
                
                entities.append(entity_data)
            
            return entities
            
        except Exception as e:
            logger.error(f"按项目查询实体失败: {e}")
            return []
    
    def save_relation(self, relation: Relation) -> bool:
        """
        保存单个关系到FalkorDB
        
        Args:
            relation: 关系对象
            
        Returns:
            bool: 保存是否成功
        """
        try:
            # 使用字符串拼接而不是参数化查询来避免FalkorDB的参数问题
            relation_id = relation.id.replace("'", "\\'")  # 转义单引号
            relation_type = relation.type.value.replace("'", "\\'")
            relation_label = (relation.label or '').replace("'", "\\'")
            properties_str = str(relation.properties) if relation.properties else ''
            properties_str = properties_str.replace("'", "\\'")
            domain = (relation.domain or '').replace("'", "\\'")
            range_val = (relation.range or '').replace("'", "\\'")
            inverse_relation = (relation.inverse_relation or '').replace("'", "\\'")
            created_time = relation.created_time.isoformat() if relation.created_time else ''
            updated_time = relation.updated_time.isoformat() if relation.updated_time else ''
            
            cypher_query = f"""
            MERGE (r:Relation {{relation_id: '{relation_id}'}})
            SET r.type = '{relation_type}',
                r.label = '{relation_label}',
                r.properties = '{properties_str}',
                r.domain = '{domain}',
                r.range = '{range_val}',
                r.inverse_relation = '{inverse_relation}',
                r.is_symmetric = {str(relation.is_symmetric).lower()},
                r.is_transitive = {str(relation.is_transitive).lower()},
                r.confidence = {float(relation.confidence)},
                r.created_time = '{created_time}',
                r.updated_time = '{updated_time}'
            RETURN r.relation_id
            """
            
            result = self.graph.query(cypher_query)
            self.stats['relations_stored'] += 1
            self.stats['queries_executed'] += 1
            self.stats['last_operation_time'] = datetime.now()
            
            return True
            
        except Exception as e:
            logger.error(f"保存关系失败: {relation.id} - {e}")
            return False
    
    def save_triple(self, triple: KnowledgeTriple) -> bool:
        """
        保存知识三元组到FalkorDB（创建图中的边）
        
        Args:
            triple: 知识三元组
            
        Returns:
            bool: 保存是否成功
        """
        try:
            # 先确保主体和客体实体存在
            self.save_entity(triple.subject)
            self.save_entity(triple.object)
            self.save_relation(triple.predicate)
            
            # 创建三元组关系
            cypher_query = f"""
            MATCH (s:{triple.subject.type.value} {{entity_id: $subject_id}})
            MATCH (o:{triple.object.type.value} {{entity_id: $object_id}})
            MERGE (s)-[r:{triple.predicate.type.value} {{
                triple_id: $triple_id,
                relation_id: $relation_id,
                confidence: $confidence,
                source: $source,
                evidence: $evidence,
                created_time: $created_time,
                updated_time: $updated_time
            }}]->(o)
            RETURN r.triple_id
            """
            
            params = {
                'subject_id': triple.subject.id,
                'object_id': triple.object.id,
                'triple_id': triple.id,
                'relation_id': triple.predicate.id,
                'confidence': float(triple.confidence),
                'source': triple.source or '',
                'evidence': str(triple.evidence) if triple.evidence else '',
                'created_time': triple.created_time.isoformat() if triple.created_time else '',
                'updated_time': triple.updated_time.isoformat() if triple.updated_time else ''
            }
            
            result = self.graph.query(cypher_query, params)
            self.stats['triples_stored'] += 1
            self.stats['queries_executed'] += 1
            self.stats['last_operation_time'] = datetime.now()
            
            return True
            
        except Exception as e:
            logger.error(f"保存三元组失败: {triple.id} - {e}")
            return False
    
    def batch_save_entities(self, entities: List[Entity], batch_size: int = 1000) -> int:
        """
        批量保存实体（高性能版本）
        
        Args:
            entities: 实体列表
            batch_size: 批处理大小
            
        Returns:
            int: 成功保存的实体数量
        """
        success_count = 0
        start_time = time.time()
        
        logger.info(f"开始批量保存实体: {len(entities)}个")
        
        # 分批处理
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            batch_success = 0
            
            # 构建批量Cypher查询
            cypher_queries = []
            params_list = []
            
            for entity in batch:
                query = f"""
                MERGE (e:{entity.type.value} {{entity_id: $entity_id}})
                SET e.label = $label,
                    e.type = $type,
                    e.confidence = $confidence,
                    e.source_table = $source_table,
                    e.updated_time = $updated_time
                """
                
                params = {
                    'entity_id': entity.id,
                    'label': entity.label,
                    'type': entity.type.value,
                    'confidence': float(entity.confidence),
                    'source_table': entity.source_table or '',
                    'updated_time': datetime.now().isoformat()
                }
                
                try:
                    self.graph.query(query, params)
                    batch_success += 1
                except Exception as e:
                    logger.warning(f"批量保存实体失败: {entity.id} - {e}")
            
            success_count += batch_success
            logger.info(f"批量保存实体进度: {i + len(batch)}/{len(entities)} ({batch_success}/{len(batch)} 成功)")
        
        elapsed_time = time.time() - start_time
        logger.info(f"批量保存实体完成: {success_count}/{len(entities)} 成功, 耗时: {elapsed_time:.2f}秒")
        
        self.stats['entities_stored'] += success_count
        return success_count
    
    def batch_save_triples(self, triples: List[KnowledgeTriple], batch_size: int = 500) -> int:
        """
        批量保存三元组（超高性能版本）
        
        Args:
            triples: 三元组列表
            batch_size: 批处理大小
            
        Returns:
            int: 成功保存的三元组数量
        """
        success_count = 0
        start_time = time.time()
        
        logger.info(f"开始批量保存三元组: {len(triples)}个")
        
        # 分批处理
        for i in range(0, len(triples), batch_size):
            batch = triples[i:i + batch_size]
            batch_success = 0
            
            for triple in batch:
                if self.save_triple(triple):
                    batch_success += 1
            
            success_count += batch_success
            logger.info(f"批量保存三元组进度: {i + len(batch)}/{len(triples)} ({batch_success}/{len(batch)} 成功)")
        
        elapsed_time = time.time() - start_time
        logger.info(f"批量保存三元组完成: {success_count}/{len(triples)} 成功, 耗时: {elapsed_time:.2f}秒")
        
        self.stats['triples_stored'] += success_count
        return success_count
    
    def query_entities(self, entity_type: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        查询实体
        
        Args:
            entity_type: 实体类型过滤
            limit: 返回数量限制
            offset: 偏移量
            
        Returns:
            List[Dict]: 实体列表
        """
        try:
            if entity_type:
                cypher_query = f"""
                MATCH (e:{entity_type.upper()})
                RETURN e.id AS id, e.label AS label, e.type AS type, 
                       e.confidence AS confidence, e.source_table AS source_table
                SKIP {offset} LIMIT {limit}
                """
            else:
                cypher_query = f"""
                MATCH (e)
                WHERE e.type IS NOT NULL
                RETURN e.id AS id, e.label AS label, e.type AS type, 
                       e.confidence AS confidence, e.source_table AS source_table
                SKIP {offset} LIMIT {limit}
                """
            
            result = self.graph.query(cypher_query)
            self.stats['queries_executed'] += 1
            
            entities = []
            if result.result_set:
                for record in result.result_set:
                    entities.append({
                        'id': record[0],
                        'label': record[1],
                        'type': record[2],
                        'confidence': float(record[3]) if record[3] else 0.0,
                        'source_table': record[4] or ''
                    })
            
            return entities
            
        except Exception as e:
            logger.error(f"查询实体失败: {e}")
            return []
    
    def query_relations(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        查询关系（三元组形式）
        
        Args:
            limit: 返回数量限制
            offset: 偏移量
            
        Returns:
            List[Dict]: 关系列表
        """
        try:
            cypher_query = f"""
            MATCH (s)-[r]->(o)
            RETURN s.id AS subject_id, s.label AS subject_label, s.type AS subject_type,
                   type(r) AS predicate_type, r.id AS predicate_id, r.confidence AS confidence,
                   o.id AS object_id, o.label AS object_label, o.type AS object_type
            SKIP {offset} LIMIT {limit}
            """
            
            result = self.graph.query(cypher_query)
            self.stats['queries_executed'] += 1
            
            relations = []
            if result.result_set:
                for record in result.result_set:
                    relations.append({
                        'subject': {
                            'id': record[0],
                            'label': record[1],
                            'type': record[2]
                        },
                        'predicate': {
                            'type': record[3],
                            'id': record[4],
                            'confidence': float(record[5]) if record[5] else 0.0
                        },
                        'object': {
                            'id': record[6],
                            'label': record[7],
                            'type': record[8]
                        }
                    })
            
            return relations
            
        except Exception as e:
            logger.error(f"查询关系失败: {e}")
            return []
    
    def search_entities(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        搜索实体
        
        Args:
            query: 搜索查询
            limit: 返回数量限制
            
        Returns:
            List[Dict]: 搜索结果
        """
        try:
            # 转义查询字符串中的单引号
            escaped_query = query.replace("'", "\\'")
            
            cypher_query = f"""
            MATCH (e)
            WHERE e.label CONTAINS '{escaped_query}' OR e.entity_id CONTAINS '{escaped_query}'
            RETURN e.entity_id AS id, e.label AS label, e.type AS type, e.confidence AS confidence
            LIMIT {limit}
            """
            
            result = self.graph.query(cypher_query)
            self.stats['queries_executed'] += 1
            
            entities = []
            if result.result_set:
                for record in result.result_set:
                    entities.append({
                        'id': record[0],
                        'label': record[1],
                        'type': record[2],
                        'confidence': float(record[3]) if record[3] else 0.0
                    })
            
            return entities
            
        except Exception as e:
            logger.error(f"搜索实体失败: {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取知识图谱统计信息"""
        try:
            # 查询实体统计
            entity_count_query = "MATCH (e) WHERE e.type IS NOT NULL RETURN count(e) AS count"
            entity_result = self.graph.query(entity_count_query)
            entity_count = entity_result.result_set[0][0] if entity_result.result_set else 0
            
            # 查询关系统计
            relation_count_query = "MATCH ()-[r]->() RETURN count(r) AS count"
            relation_result = self.graph.query(relation_count_query)
            relation_count = relation_result.result_set[0][0] if relation_result.result_set else 0
            
            # 查询标签统计
            labels_query = "CALL db.labels()"
            labels_result = self.graph.query(labels_query)
            labels_count = len(labels_result.result_set) if labels_result.result_set else 0
            
            stats = {
                'total_entities': entity_count,
                'total_relations': relation_count,
                'total_labels': labels_count,
                'triples_stored': relation_count,  # 每个关系就是一个三元组
                'performance_stats': self.stats,
                'database_info': {
                    'host': self.host,
                    'port': self.port,
                    'graph_name': self.graph_name,
                    'connected': self.graph is not None
                }
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {
                'total_entities': 0,
                'total_relations': 0,
                'total_labels': 0,
                'performance_stats': self.stats,
                'error': str(e)
            }
    
    def clear_graph(self) -> bool:
        """清空整个知识图谱（谨慎使用）"""
        try:
            self.graph.query("MATCH (n) DETACH DELETE n")
            logger.info("知识图谱已清空")
            return True
        except Exception as e:
            logger.error(f"清空知识图谱失败: {e}")
            return False
    
    def delete_project_graph(self, project_name: str) -> bool:
        """删除特定项目的知识图谱"""
        try:
            success = True
            
            # 如果是双重存储模式
            if self.project_name and self.global_graph and self.project_graph:
                # 1. 从全局图中删除项目相关数据
                try:
                    global_delete_query = f"MATCH (n) WHERE n.project_name = '{project_name}' DETACH DELETE n"
                    self.global_graph.query(global_delete_query)
                    logger.info(f"已从全局图删除项目 {project_name} 的数据")
                except Exception as e:
                    logger.error(f"从全局图删除项目数据失败: {e}")
                    success = False
                
                # 2. 清空项目专用图
                try:
                    self.project_graph.query("MATCH (n) DETACH DELETE n")
                    logger.info(f"已清空项目图: knowledge_graph_project_{project_name}")
                except Exception as e:
                    logger.error(f"清空项目图失败: {e}")
                    success = False
            else:
                # 单图模式：删除指定项目的数据
                delete_query = f"MATCH (n) WHERE n.project_name = '{project_name}' DETACH DELETE n"
                self.graph.query(delete_query)
                logger.info(f"已从知识图谱删除项目 {project_name} 的数据")
            
            return success
            
        except Exception as e:
            logger.error(f"删除项目知识图谱失败: {e}")
            return False
    
    def delete_all_projects(self) -> Dict[str, bool]:
        """删除所有项目的知识图谱数据"""
        try:
            results = {}
            
            # 获取所有项目列表
            projects = self.get_all_projects()
            
            for project_info in projects:
                project_name = project_info['project_name']
                results[project_name] = self.delete_project_graph(project_name)
            
            logger.info(f"批量删除完成，成功: {sum(results.values())}/{len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"批量删除项目失败: {e}")
            return {}
    
    def delete_entities_by_type(self, entity_type: str, project_name: str = None) -> bool:
        """删除指定类型的实体"""
        try:
            if project_name:
                # 删除特定项目的特定类型实体
                delete_query = f"MATCH (n:{entity_type}) WHERE n.project_name = '{project_name}' DETACH DELETE n"
            else:
                # 删除所有特定类型实体
                delete_query = f"MATCH (n:{entity_type}) DETACH DELETE n"
            
            self.graph.query(delete_query)
            logger.info(f"已删除类型为 {entity_type} 的实体" + (f" (项目: {project_name})" if project_name else ""))
            return True
            
        except Exception as e:
            logger.error(f"删除实体类型失败: {e}")
            return False
    
    def get_deletion_preview(self, project_name: str = None) -> Dict[str, Any]:
        """获取删除预览信息"""
        try:
            preview = {
                'project_name': project_name,
                'entities_to_delete': 0,
                'relations_to_delete': 0,
                'entity_types': {},
                'graphs_affected': []
            }
            
            if project_name:
                # 预览特定项目的删除影响
                if self.project_name and self.global_graph:
                    # 双重存储模式
                    preview['graphs_affected'] = ['knowledge_graph_global', f'knowledge_graph_project_{project_name}']
                    
                    # 统计全局图中的项目数据
                    global_entity_query = f"MATCH (n) WHERE n.project_name = '{project_name}' RETURN count(n) AS count"
                    global_result = self.global_graph.query(global_entity_query)
                    global_entities = global_result.result_set[0][0] if global_result.result_set else 0
                    
                    # 统计项目图中的数据
                    project_entity_query = "MATCH (n) RETURN count(n) AS count"
                    project_result = self.project_graph.query(project_entity_query)
                    project_entities = project_result.result_set[0][0] if project_result.result_set else 0
                    
                    preview['entities_to_delete'] = global_entities + project_entities
                else:
                    # 单图模式 - 修复查询逻辑
                    preview['graphs_affected'] = [self.graph_name]
                    # 查询所有节点（包括实体和关系节点）
                    entity_query = "MATCH (n) RETURN count(n) AS count"
                    result = self.graph.query(entity_query)
                    preview['entities_to_delete'] = result.result_set[0][0] if result.result_set else 0
                    
                    # 查询关系数量
                    relation_query = "MATCH ()-[r]->() RETURN count(r) AS count"
                    relation_result = self.graph.query(relation_query)
                    preview['relations_to_delete'] = relation_result.result_set[0][0] if relation_result.result_set else 0
            else:
                # 预览全部删除
                preview['graphs_affected'] = [self.graph_name]
                entity_query = "MATCH (n) RETURN count(n) AS count"
                result = self.graph.query(entity_query)
                preview['entities_to_delete'] = result.result_set[0][0] if result.result_set else 0
            
            return preview
            
        except Exception as e:
            logger.error(f"获取删除预览失败: {e}")
            return {'error': str(e)}
