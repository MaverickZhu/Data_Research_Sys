"""
知识图谱数据适配器
将CSV数据转换为知识图谱格式
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from ..knowledge_graph.kg_models import Entity, Relation, KnowledgeTriple, EntityType, RelationType
from ..knowledge_graph.entity_extractor import EntityExtractor
from ..knowledge_graph.relation_extractor import RelationExtractor

logger = logging.getLogger(__name__)

class KGDataAdapter:
    """知识图谱数据适配器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化适配器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        self.entity_extractor = EntityExtractor(config)
        self.relation_extractor = RelationExtractor(config)
        
        # 映射配置
        self.default_mappings = {
            'entity_type_mapping': {
                'organization': EntityType.ORGANIZATION,
                'person': EntityType.PERSON,
                'location': EntityType.LOCATION,
                'identifier': EntityType.IDENTIFIER
            },
            'relation_type_mapping': {
                'located_in': RelationType.LOCATED_IN,
                'managed_by': RelationType.MANAGED_BY,
                'contact_of': RelationType.CONTACT_OF,
                'represents': RelationType.REPRESENTS
            }
        }
    
    def convert_dataframe_to_kg(self, df: pd.DataFrame,
                               table_name: str = None,
                               mapping_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        将DataFrame转换为知识图谱格式
        
        Args:
            df: 数据框
            table_name: 表名
            mapping_config: 映射配置
            
        Returns:
            Dict: 包含实体、关系和三元组的知识图谱数据
        """
        try:
            logger.info(f"开始转换表 {table_name} 为知识图谱格式")
            
            # 1. 抽取实体
            entities = self.entity_extractor.extract_entities_from_dataframe(
                df, table_name
            )
            
            # 2. 抽取关系
            triples = self.relation_extractor.extract_relations_from_dataframe(
                df, entities, table_name
            )
            
            # 3. 获取唯一关系
            relations = self._extract_unique_relations(triples)
            
            # 4. 应用映射配置
            if mapping_config:
                entities, relations, triples = self._apply_mapping_config(
                    entities, relations, triples, mapping_config
                )
            
            # 5. 生成统计信息
            statistics = self._generate_kg_statistics(entities, relations, triples)
            
            result = {
                'table_name': table_name,
                'entities': entities,
                'relations': relations,
                'triples': triples,
                'statistics': statistics,
                'conversion_metadata': {
                    'source_rows': len(df),
                    'source_columns': len(df.columns),
                    'extracted_entities': len(entities),
                    'extracted_relations': len(relations),
                    'extracted_triples': len(triples)
                }
            }
            
            logger.info(f"转换完成: {len(entities)}个实体, {len(relations)}个关系, {len(triples)}个三元组")
            return result
            
        except Exception as e:
            logger.error(f"转换失败: {str(e)}")
            return {
                'error': str(e),
                'table_name': table_name,
                'entities': [],
                'relations': [],
                'triples': []
            }
    
    def _extract_unique_relations(self, triples: List[KnowledgeTriple]) -> List[Relation]:
        """从三元组中提取唯一关系"""
        seen_relations = {}
        unique_relations = []
        
        for triple in triples:
            relation_id = triple.predicate.id
            if relation_id not in seen_relations:
                seen_relations[relation_id] = triple.predicate
                unique_relations.append(triple.predicate)
        
        return unique_relations
    
    def _apply_mapping_config(self, entities: List[Entity],
                            relations: List[Relation],
                            triples: List[KnowledgeTriple],
                            mapping_config: Dict[str, Any]) -> Tuple[List[Entity], List[Relation], List[KnowledgeTriple]]:
        """应用映射配置"""
        # 实体类型映射
        entity_type_mapping = mapping_config.get('entity_type_mapping', {})
        for entity in entities:
            if entity.type.value in entity_type_mapping:
                new_type = entity_type_mapping[entity.type.value]
                if isinstance(new_type, str):
                    entity.type = EntityType(new_type)
                elif isinstance(new_type, EntityType):
                    entity.type = new_type
        
        # 关系类型映射
        relation_type_mapping = mapping_config.get('relation_type_mapping', {})
        for relation in relations:
            if relation.type.value in relation_type_mapping:
                new_type = relation_type_mapping[relation.type.value]
                if isinstance(new_type, str):
                    relation.type = RelationType(new_type)
                elif isinstance(new_type, RelationType):
                    relation.type = new_type
        
        # 字段映射
        field_mapping = mapping_config.get('field_mapping', {})
        if field_mapping:
            for entity in entities:
                self._apply_field_mapping(entity, field_mapping)
        
        return entities, relations, triples
    
    def _apply_field_mapping(self, entity: Entity, field_mapping: Dict[str, str]) -> None:
        """应用字段映射"""
        new_properties = {}
        
        for old_key, value in entity.properties.items():
            new_key = field_mapping.get(old_key, old_key)
            new_properties[new_key] = value
        
        entity.properties = new_properties
    
    def _generate_kg_statistics(self, entities: List[Entity],
                              relations: List[Relation],
                              triples: List[KnowledgeTriple]) -> Dict[str, Any]:
        """生成知识图谱统计信息"""
        # 实体统计
        entity_stats = {}
        for entity in entities:
            entity_type = entity.type.value
            if entity_type not in entity_stats:
                entity_stats[entity_type] = 0
            entity_stats[entity_type] += 1
        
        # 关系统计
        relation_stats = {}
        for relation in relations:
            relation_type = relation.type.value
            if relation_type not in relation_stats:
                relation_stats[relation_type] = 0
            relation_stats[relation_type] += 1
        
        # 三元组统计
        triple_stats = {}
        for triple in triples:
            relation_type = triple.predicate.type.value
            if relation_type not in triple_stats:
                triple_stats[relation_type] = 0
            triple_stats[relation_type] += 1
        
        # 置信度分布
        confidence_distribution = {
            'high': len([t for t in triples if t.confidence > 0.8]),
            'medium': len([t for t in triples if 0.6 <= t.confidence <= 0.8]),
            'low': len([t for t in triples if t.confidence < 0.6])
        }
        
        return {
            'entity_count_by_type': entity_stats,
            'relation_count_by_type': relation_stats,
            'triple_count_by_relation': triple_stats,
            'confidence_distribution': confidence_distribution,
            'total_entities': len(entities),
            'total_relations': len(relations),
            'total_triples': len(triples)
        }
    
    def create_mapping_template(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        为DataFrame创建映射模板
        
        Args:
            df: 数据框
            
        Returns:
            Dict: 映射模板
        """
        template = {
            'entity_type_mapping': {},
            'relation_type_mapping': {},
            'field_mapping': {},
            'column_analysis': {}
        }
        
        # 分析列并生成建议映射
        for col in df.columns:
            col_lower = col.lower()
            
            # 实体类型建议
            if any(keyword in col_lower for keyword in ['公司', '企业', '单位', 'company', 'organization']):
                template['column_analysis'][col] = {
                    'suggested_entity_type': 'ORGANIZATION',
                    'confidence': 0.8
                }
            elif any(keyword in col_lower for keyword in ['姓名', '法人', '代表', 'name', 'person']):
                template['column_analysis'][col] = {
                    'suggested_entity_type': 'PERSON',
                    'confidence': 0.8
                }
            elif any(keyword in col_lower for keyword in ['地址', '位置', 'address', 'location']):
                template['column_analysis'][col] = {
                    'suggested_entity_type': 'LOCATION',
                    'confidence': 0.8
                }
            elif any(keyword in col_lower for keyword in ['电话', '手机', 'phone', 'tel']):
                template['column_analysis'][col] = {
                    'suggested_entity_type': 'IDENTIFIER',
                    'confidence': 0.7
                }
        
        return template
    
    def export_to_formats(self, kg_data: Dict[str, Any],
                         export_formats: List[str] = None) -> Dict[str, Any]:
        """
        导出知识图谱数据为不同格式
        
        Args:
            kg_data: 知识图谱数据
            export_formats: 导出格式列表 ['json', 'csv', 'rdf', 'cypher']
            
        Returns:
            Dict: 各种格式的导出数据
        """
        if not export_formats:
            export_formats = ['json']
        
        exports = {}
        
        for format_type in export_formats:
            try:
                if format_type == 'json':
                    exports['json'] = self._export_to_json(kg_data)
                elif format_type == 'csv':
                    exports['csv'] = self._export_to_csv(kg_data)
                elif format_type == 'rdf':
                    exports['rdf'] = self._export_to_rdf(kg_data)
                elif format_type == 'cypher':
                    exports['cypher'] = self._export_to_cypher(kg_data)
                else:
                    logger.warning(f"不支持的导出格式: {format_type}")
            except Exception as e:
                logger.error(f"导出格式 {format_type} 失败: {str(e)}")
                exports[format_type] = {'error': str(e)}
        
        return exports
    
    def _export_to_json(self, kg_data: Dict[str, Any]) -> Dict[str, Any]:
        """导出为JSON格式"""
        return {
            'entities': [entity.to_dict() for entity in kg_data['entities']],
            'relations': [relation.to_dict() for relation in kg_data['relations']],
            'triples': [triple.to_dict() for triple in kg_data['triples']],
            'statistics': kg_data['statistics']
        }
    
    def _export_to_csv(self, kg_data: Dict[str, Any]) -> Dict[str, str]:
        """导出为CSV格式"""
        # 实体CSV
        entities_data = []
        for entity in kg_data['entities']:
            entities_data.append({
                'id': entity.id,
                'type': entity.type.value,
                'label': entity.label,
                'properties': str(entity.properties),
                'source_table': entity.source_table,
                'confidence': entity.confidence
            })
        
        entities_df = pd.DataFrame(entities_data)
        
        # 三元组CSV
        triples_data = []
        for triple in kg_data['triples']:
            triples_data.append({
                'id': triple.id,
                'subject_id': triple.subject.id,
                'subject_label': triple.subject.label,
                'predicate_id': triple.predicate.id,
                'predicate_type': triple.predicate.type.value,
                'object_id': triple.object.id,
                'object_label': triple.object.label,
                'confidence': triple.confidence,
                'source': triple.source
            })
        
        triples_df = pd.DataFrame(triples_data)
        
        return {
            'entities_csv': entities_df.to_csv(index=False),
            'triples_csv': triples_df.to_csv(index=False)
        }
    
    def _export_to_rdf(self, kg_data: Dict[str, Any]) -> str:
        """导出为RDF格式（基础实现）"""
        rdf_lines = ['@prefix kg: <http://example.org/kg/> .']
        
        # 导出实体
        for entity in kg_data['entities']:
            rdf_lines.append(f'kg:{entity.id} a kg:{entity.type.value} ;')
            rdf_lines.append(f'  kg:label "{entity.label}" .')
        
        # 导出三元组
        for triple in kg_data['triples']:
            rdf_lines.append(
                f'kg:{triple.subject.id} kg:{triple.predicate.type.value} kg:{triple.object.id} .'
            )
        
        return '\n'.join(rdf_lines)
    
    def _export_to_cypher(self, kg_data: Dict[str, Any]) -> List[str]:
        """导出为Cypher查询格式"""
        cypher_statements = []
        
        # 创建实体
        for entity in kg_data['entities']:
            properties = ', '.join([f'{k}: "{v}"' for k, v in entity.properties.items()])
            cypher_statements.append(
                f'CREATE (:{entity.type.value} {{id: "{entity.id}", label: "{entity.label}", {properties}}})'
            )
        
        # 创建关系
        for triple in kg_data['triples']:
            cypher_statements.append(
                f'MATCH (s {{id: "{triple.subject.id}"}}), (o {{id: "{triple.object.id}"}}) '
                f'CREATE (s)-[:{triple.predicate.type.value} {{confidence: {triple.confidence}}}]->(o)'
            )
        
        return cypher_statements