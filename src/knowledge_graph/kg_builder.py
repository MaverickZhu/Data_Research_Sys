"""
知识图谱构建器
协调实体抽取、关系抽取和知识图谱存储的整体流程
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime

from .kg_models import Entity, Relation, KnowledgeTriple, Ontology, create_default_ontology
from .kg_store import KnowledgeGraphStore
from .entity_extractor import EntityExtractor
from .relation_extractor import RelationExtractor

logger = logging.getLogger(__name__)

class KnowledgeGraphBuilder:
    """知识图谱构建器"""
    
    def __init__(self, kg_store: KnowledgeGraphStore, config: Dict[str, Any] = None):
        """
        初始化知识图谱构建器
        
        Args:
            kg_store: 知识图谱存储引擎
            config: 配置参数
        """
        self.kg_store = kg_store
        self.config = config or {}
        
        # 初始化抽取器
        self.entity_extractor = EntityExtractor(config)
        self.relation_extractor = RelationExtractor(config)
        
        # 本体管理
        self.ontology = None
        self._load_or_create_ontology()
        
        # 构建配置
        self.build_config = self.config.get('kg_builder', {
            'batch_size': 1000,
            'enable_validation': True,
            'auto_merge_entities': True,
            'confidence_threshold': 0.5
        })
        
        logger.info("知识图谱构建器初始化完成")
    
    def _load_or_create_ontology(self) -> None:
        """加载或创建本体"""
        try:
            # 尝试从存储中加载默认本体
            ontology_name = self.config.get('ontology_name', 'default_ontology')
            
            # TODO: 实现本体查询功能
            # self.ontology = self.kg_store.get_ontology_by_name(ontology_name)
            
            if not self.ontology:
                # 创建默认本体
                self.ontology = create_default_ontology('general')
                try:
                    self.kg_store.save_ontology(self.ontology)
                    logger.info("创建并保存了默认本体")
                except Exception as save_error:
                    logger.warning(f"本体保存失败，但不影响系统运行: {str(save_error)}")
                    logger.info("创建了默认本体（仅内存中）")
            else:
                logger.info("加载了现有本体")
                
        except Exception as e:
            logger.error(f"本体加载失败: {str(e)}")
            # 使用默认本体作为fallback
            self.ontology = create_default_ontology('general')
    
    def build_knowledge_graph_from_dataframe(self, df: pd.DataFrame,
                                           table_name: str,
                                           project_id: str = None) -> Dict[str, Any]:
        """
        从DataFrame构建知识图谱
        
        Args:
            df: 数据框
            table_name: 表名
            project_id: 项目ID
            
        Returns:
            Dict: 构建结果
        """
        try:
            logger.info(f"开始构建知识图谱: {table_name}")
            
            build_result = {
                'project_id': project_id,
                'table_name': table_name,
                'start_time': datetime.now().isoformat(),
                'status': 'in_progress',
                'statistics': {},
                'entities_created': 0,
                'relations_created': 0,
                'triples_created': 0,
                'errors': []
            }
            
            # 第一阶段：实体抽取
            logger.info("第一阶段：实体抽取")
            entities = self.entity_extractor.extract_entities_from_dataframe(df, table_name)
            
            if self.build_config['enable_validation']:
                entities = self._validate_entities(entities)
            
            if self.build_config['auto_merge_entities']:
                entities = self._merge_similar_entities(entities)
            
            # 第二阶段：关系抽取
            logger.info("第二阶段：关系抽取")
            triples = self.relation_extractor.extract_relations_from_dataframe(
                df, entities, table_name
            )
            
            if self.build_config['enable_validation']:
                triples = self._validate_triples(triples)
            
            # 过滤低置信度的三元组
            confidence_threshold = self.build_config['confidence_threshold']
            triples = [t for t in triples if t.confidence >= confidence_threshold]
            
            # 第三阶段：批量存储
            logger.info("第三阶段：批量存储")
            entities_saved = self._batch_save_entities(entities)
            triples_saved = self._batch_save_triples(triples)
            
            # 第四阶段：统计和索引
            logger.info("第四阶段：生成统计信息")
            statistics = self._generate_build_statistics(entities, triples)
            
            # 更新构建结果
            build_result.update({
                'status': 'completed',
                'end_time': datetime.now().isoformat(),
                'entities_created': entities_saved,
                'triples_created': triples_saved,
                'statistics': statistics
            })
            
            logger.info(f"知识图谱构建完成: {entities_saved}个实体, {triples_saved}个三元组")
            return build_result
            
        except Exception as e:
            error_msg = f"知识图谱构建失败: {str(e)}"
            logger.error(error_msg)
            
            build_result.update({
                'status': 'failed',
                'error': error_msg,
                'end_time': datetime.now().isoformat()
            })
            
            return build_result
    
    def _validate_entities(self, entities: List[Entity]) -> List[Entity]:
        """验证实体"""
        valid_entities = []
        
        for entity in entities:
            try:
                # 基本验证
                if not entity.label or len(entity.label.strip()) < 2:
                    logger.warning(f"跳过无效实体: 标签为空或过短")
                    continue
                
                # 本体验证（如果有本体的话）
                if self.ontology:
                    entity_type_str = entity.type.value
                    if entity_type_str not in self.ontology.entity_types:
                        logger.warning(f"实体类型 {entity_type_str} 不在本体中，但仍然保留")
                
                valid_entities.append(entity)
                
            except Exception as e:
                logger.warning(f"实体验证失败: {str(e)}")
                continue
        
        logger.info(f"实体验证: {len(entities)} -> {len(valid_entities)}")
        return valid_entities
    
    def _validate_triples(self, triples: List[KnowledgeTriple]) -> List[KnowledgeTriple]:
        """验证三元组"""
        valid_triples = []
        
        for triple in triples:
            try:
                # 基本验证
                if not triple.is_valid():
                    logger.warning(f"跳过无效三元组: {triple.id}")
                    continue
                
                # 本体验证
                if self.ontology and not self.ontology.validate_triple(triple):
                    logger.warning(f"三元组不符合本体规则: {triple.id}")
                    # 不跳过，只是警告
                
                valid_triples.append(triple)
                
            except Exception as e:
                logger.warning(f"三元组验证失败: {str(e)}")
                continue
        
        logger.info(f"三元组验证: {len(triples)} -> {len(valid_triples)}")
        return valid_triples
    
    def _merge_similar_entities(self, entities: List[Entity]) -> List[Entity]:
        """合并相似实体"""
        merged_entities = []
        entity_groups = {}
        
        # 按类型和标签分组
        for entity in entities:
            key = f"{entity.type.value}:{entity.label.lower()}"
            if key not in entity_groups:
                entity_groups[key] = []
            entity_groups[key].append(entity)
        
        # 合并每组中的实体
        for group in entity_groups.values():
            if len(group) == 1:
                merged_entities.append(group[0])
            else:
                # 合并多个实体
                primary_entity = group[0]
                for other_entity in group[1:]:
                    # 合并属性
                    for key, value in other_entity.properties.items():
                        if key not in primary_entity.properties:
                            primary_entity.add_property(key, value)
                    
                    # 合并别名
                    for alias in other_entity.aliases:
                        primary_entity.add_alias(alias)
                    
                    # 使用最高置信度
                    if other_entity.confidence > primary_entity.confidence:
                        primary_entity.confidence = other_entity.confidence
                
                merged_entities.append(primary_entity)
        
        logger.info(f"实体合并: {len(entities)} -> {len(merged_entities)}")
        return merged_entities
    
    def _batch_save_entities(self, entities: List[Entity]) -> int:
        """批量保存实体"""
        batch_size = self.build_config['batch_size']
        saved_count = 0
        
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            
            for entity in batch:
                if self.kg_store.save_entity(entity):
                    saved_count += 1
                else:
                    logger.warning(f"保存实体失败: {entity.id}")
        
        logger.info(f"批量保存实体: {saved_count}/{len(entities)}")
        return saved_count
    
    def _batch_save_triples(self, triples: List[KnowledgeTriple]) -> int:
        """批量保存三元组"""
        batch_size = self.build_config['batch_size']
        saved_count = 0
        
        # 首先保存关系
        relations = {}
        for triple in triples:
            relations[triple.predicate.id] = triple.predicate
        
        for relation in relations.values():
            self.kg_store.save_relation(relation)
        
        # 然后批量保存三元组
        for i in range(0, len(triples), batch_size):
            batch = triples[i:i + batch_size]
            saved_count += self.kg_store.batch_save_triples(batch)
        
        logger.info(f"批量保存三元组: {saved_count}/{len(triples)}")
        return saved_count
    
    def _generate_build_statistics(self, entities: List[Entity],
                                 triples: List[KnowledgeTriple]) -> Dict[str, Any]:
        """生成构建统计信息"""
        # 实体统计
        entity_stats = {}
        for entity in entities:
            entity_type = entity.type.value
            if entity_type not in entity_stats:
                entity_stats[entity_type] = 0
            entity_stats[entity_type] += 1
        
        # 关系统计
        relation_stats = {}
        for triple in triples:
            relation_type = triple.predicate.type.value
            if relation_type not in relation_stats:
                relation_stats[relation_type] = 0
            relation_stats[relation_type] += 1
        
        # 置信度分布
        confidence_distribution = {
            'high': len([e for e in entities if e.confidence > 0.8]) + len([t for t in triples if t.confidence > 0.8]),
            'medium': len([e for e in entities if 0.6 <= e.confidence <= 0.8]) + len([t for t in triples if 0.6 <= t.confidence <= 0.8]),
            'low': len([e for e in entities if e.confidence < 0.6]) + len([t for t in triples if t.confidence < 0.6])
        }
        
        return {
            'entity_count_by_type': entity_stats,
            'relation_count_by_type': relation_stats,
            'confidence_distribution': confidence_distribution,
            'total_entities': len(entities),
            'total_triples': len(triples),
            'average_entity_confidence': sum(e.confidence for e in entities) / len(entities) if entities else 0,
            'average_triple_confidence': sum(t.confidence for t in triples) / len(triples) if triples else 0
        }
    
    def build_incremental_knowledge_graph(self, new_df: pd.DataFrame,
                                        table_name: str,
                                        existing_project_id: str) -> Dict[str, Any]:
        """
        增量构建知识图谱
        
        Args:
            new_df: 新数据框
            table_name: 表名
            existing_project_id: 现有项目ID
            
        Returns:
            Dict: 增量构建结果
        """
        try:
            logger.info(f"开始增量构建知识图谱: {table_name}")
            
            # 获取现有实体用于重复检测
            existing_entities = self.kg_store.find_entities(source_table=table_name)
            
            # 抽取新实体
            new_entities = self.entity_extractor.extract_entities_from_dataframe(new_df, table_name)
            
            # 去除重复实体
            unique_entities = self._filter_duplicate_entities(new_entities, existing_entities)
            
            # 抽取新关系
            all_entities = existing_entities + unique_entities
            new_triples = self.relation_extractor.extract_relations_from_dataframe(
                new_df, all_entities, table_name
            )
            
            # 保存新的实体和关系
            entities_saved = self._batch_save_entities(unique_entities)
            triples_saved = self._batch_save_triples(new_triples)
            
            result = {
                'project_id': existing_project_id,
                'table_name': table_name,
                'incremental_build': True,
                'new_entities': len(unique_entities),
                'new_triples': len(new_triples),
                'entities_saved': entities_saved,
                'triples_saved': triples_saved,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"增量构建完成: {entities_saved}个新实体, {triples_saved}个新三元组")
            return result
            
        except Exception as e:
            logger.error(f"增量构建失败: {str(e)}")
            return {'error': str(e)}
    
    def _filter_duplicate_entities(self, new_entities: List[Entity],
                                 existing_entities: List[Entity]) -> List[Entity]:
        """过滤重复实体"""
        unique_entities = []
        existing_keys = set()
        
        # 创建现有实体的键集合
        for entity in existing_entities:
            key = f"{entity.type.value}:{entity.label.lower()}"
            existing_keys.add(key)
        
        # 过滤新实体
        for entity in new_entities:
            key = f"{entity.type.value}:{entity.label.lower()}"
            if key not in existing_keys:
                unique_entities.append(entity)
                existing_keys.add(key)
        
        logger.info(f"去重过滤: {len(new_entities)} -> {len(unique_entities)}")
        return unique_entities
    
    def get_build_status(self) -> Dict[str, Any]:
        """获取构建状态"""
        try:
            kg_stats = self.kg_store.get_statistics()
            
            return {
                'status': 'ready',
                'knowledge_graph_statistics': kg_stats,
                'ontology_info': {
                    'name': self.ontology.name if self.ontology else 'none',
                    'version': self.ontology.version if self.ontology else 'none',
                    'entity_types_count': len(self.ontology.entity_types) if self.ontology else 0,
                    'relation_types_count': len(self.ontology.relation_types) if self.ontology else 0
                },
                'build_config': self.build_config
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def clear_project_knowledge_graph(self, project_id: str) -> bool:
        """
        清除项目相关的知识图谱数据
        
        Args:
            project_id: 项目ID
            
        Returns:
            bool: 是否成功
        """
        try:
            # TODO: 实现按项目ID删除的功能
            # 这需要在实体和三元组中存储project_id信息
            logger.warning("按项目清除功能待实现")
            return False
            
        except Exception as e:
            logger.error(f"清除项目知识图谱失败: {str(e)}")
            return False