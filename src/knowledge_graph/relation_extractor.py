"""
关系抽取器
从结构化数据中识别和抽取实体间关系
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from .kg_models import Entity, Relation, KnowledgeTriple, EntityType, RelationType

logger = logging.getLogger(__name__)

class RelationExtractor:
    """关系抽取器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化关系抽取器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 关系抽取规则
        self.relation_rules = {
            RelationType.LOCATED_IN: {
                'subject_types': [EntityType.ORGANIZATION, EntityType.PERSON],
                'object_types': [EntityType.LOCATION],
                'field_patterns': [
                    ('unit_name', 'unit_address'),
                    ('company_name', 'address'),
                    ('organization', 'location'),
                    ('name', 'address')
                ],
                'confidence': 0.8
            },
            RelationType.MANAGED_BY: {
                'subject_types': [EntityType.ORGANIZATION],
                'object_types': [EntityType.PERSON],
                'field_patterns': [
                    ('unit_name', 'legal_person'),
                    ('company_name', 'manager'),
                    ('organization', 'representative'),
                    ('unit_name', 'security_manager')
                ],
                'confidence': 0.9
            },
            RelationType.CONTACT_OF: {
                'subject_types': [EntityType.ORGANIZATION, EntityType.PERSON],
                'object_types': [EntityType.IDENTIFIER],
                'field_patterns': [
                    ('unit_name', 'contact_phone'),
                    ('person_name', 'phone'),
                    ('organization', 'telephone')
                ],
                'confidence': 0.7
            },
            RelationType.REPRESENTS: {
                'subject_types': [EntityType.PERSON],
                'object_types': [EntityType.ORGANIZATION],
                'field_patterns': [
                    ('legal_person', 'unit_name'),
                    ('representative', 'company_name'),
                    ('manager', 'organization')
                ],
                'confidence': 0.85
            }
        }
        
        # 置信度阈值
        self.confidence_threshold = self.config.get('confidence_threshold', 0.6)
        
    def extract_relations_from_dataframe(self, df: pd.DataFrame,
                                       entities: List[Entity],
                                       table_name: str = None) -> List[KnowledgeTriple]:
        """
        从DataFrame中抽取关系
        
        Args:
            df: 数据框
            entities: 已抽取的实体列表
            table_name: 表名
            
        Returns:
            List[KnowledgeTriple]: 抽取的知识三元组列表
        """
        try:
            logger.info(f"开始从表 {table_name} 抽取关系，共 {len(entities)} 个实体")
            
            triples = []
            
            # 按源记录ID分组实体，以便在同一行中查找关系
            entities_by_record = self._group_entities_by_record(entities)
            
            # 为每个记录抽取关系
            for record_id, record_entities in entities_by_record.items():
                try:
                    if int(record_id) < len(df):
                        row = df.iloc[int(record_id)]
                        record_triples = self._extract_relations_from_record(
                            row, record_entities, table_name
                        )
                        triples.extend(record_triples)
                except (ValueError, IndexError) as e:
                    logger.warning(f"处理记录 {record_id} 时出错: {str(e)}")
                    continue
            
            # 跨记录关系抽取（基于实体匹配）
            cross_record_triples = self._extract_cross_record_relations(entities)
            triples.extend(cross_record_triples)
            
            # 去重处理
            triples = self._deduplicate_triples(triples)
            
            logger.info(f"从表 {table_name} 抽取到 {len(triples)} 个关系")
            return triples
            
        except Exception as e:
            logger.error(f"从DataFrame抽取关系失败: {str(e)}")
            return []
    
    def _group_entities_by_record(self, entities: List[Entity]) -> Dict[str, List[Entity]]:
        """
        按源记录ID分组实体
        
        Args:
            entities: 实体列表
            
        Returns:
            Dict[str, List[Entity]]: 按记录ID分组的实体
        """
        grouped = {}
        
        for entity in entities:
            record_id = entity.source_record_id
            if record_id:
                if record_id not in grouped:
                    grouped[record_id] = []
                grouped[record_id].append(entity)
        
        return grouped
    
    def _extract_relations_from_record(self, row: pd.Series,
                                     entities: List[Entity],
                                     table_name: str) -> List[KnowledgeTriple]:
        """
        从单个记录中抽取关系
        
        Args:
            row: 数据行
            entities: 该记录的实体列表
            table_name: 表名
            
        Returns:
            List[KnowledgeTriple]: 三元组列表
        """
        triples = []
        
        # 为每种关系类型尝试抽取
        for relation_type, rule in self.relation_rules.items():
            try:
                extracted_triples = self._extract_relation_by_rule(
                    row, entities, relation_type, rule, table_name
                )
                triples.extend(extracted_triples)
            except Exception as e:
                logger.warning(f"抽取关系 {relation_type.value} 失败: {str(e)}")
                continue
        
        return triples
    
    def _extract_relation_by_rule(self, row: pd.Series,
                                entities: List[Entity],
                                relation_type: RelationType,
                                rule: Dict[str, Any],
                                table_name: str) -> List[KnowledgeTriple]:
        """
        根据规则抽取特定类型的关系
        
        Args:
            row: 数据行
            entities: 实体列表
            relation_type: 关系类型
            rule: 抽取规则
            table_name: 表名
            
        Returns:
            List[KnowledgeTriple]: 三元组列表
        """
        triples = []
        
        # 筛选符合类型的实体
        subject_entities = [e for e in entities if e.type in rule['subject_types']]
        object_entities = [e for e in entities if e.type in rule['object_types']]
        
        if not subject_entities or not object_entities:
            return triples
        
        # 根据字段模式抽取关系
        for subject_field, object_field in rule['field_patterns']:
            subject_value = self._get_field_value(row, subject_field)
            object_value = self._get_field_value(row, object_field)
            
            if not subject_value or not object_value:
                continue
            
            # 查找匹配的实体
            subject_entity = self._find_matching_entity(subject_value, subject_entities)
            object_entity = self._find_matching_entity(object_value, object_entities)
            
            if subject_entity and object_entity:
                # 创建关系和三元组
                relation = self._create_relation(relation_type, rule)
                
                triple = KnowledgeTriple(
                    subject=subject_entity,
                    predicate=relation,
                    object=object_entity,
                    confidence=rule['confidence'],
                    source=f"table:{table_name}",
                    evidence=[f"{subject_field}:{subject_value}", f"{object_field}:{object_value}"]
                )
                
                triples.append(triple)
        
        return triples
    
    def _get_field_value(self, row: pd.Series, field_pattern: str) -> Optional[str]:
        """
        获取字段值，支持模糊匹配
        
        Args:
            row: 数据行
            field_pattern: 字段模式
            
        Returns:
            Optional[str]: 字段值
        """
        # 直接匹配
        if field_pattern in row.index:
            value = row[field_pattern]
            if pd.notna(value) and str(value).strip():
                return str(value).strip()
        
        # 模糊匹配
        field_pattern_lower = field_pattern.lower()
        for col in row.index:
            col_lower = col.lower()
            if (field_pattern_lower in col_lower or 
                col_lower in field_pattern_lower or
                self._are_similar_fields(field_pattern_lower, col_lower)):
                value = row[col]
                if pd.notna(value) and str(value).strip():
                    return str(value).strip()
        
        return None
    
    def _are_similar_fields(self, field1: str, field2: str) -> bool:
        """
        判断两个字段是否相似
        
        Args:
            field1: 字段1
            field2: 字段2
            
        Returns:
            bool: 是否相似
        """
        # 定义同义词映射
        synonyms = {
            'name': ['名称', '姓名', 'dwmc', 'unit_name'],
            'address': ['地址', '位置', 'dwdz', 'unit_address'],
            'phone': ['电话', '手机', 'lxdh', 'contact_phone'],
            'legal_person': ['法人', '代表', 'fddbr', 'legal_people'],
            'manager': ['管理', '负责', '经理', 'security_manager']
        }
        
        for key, values in synonyms.items():
            if ((field1 in values and field2 in values) or
                (key in field1 and any(v in field2 for v in values)) or
                (key in field2 and any(v in field1 for v in values))):
                return True
        
        return False
    
    def _find_matching_entity(self, value: str, entities: List[Entity]) -> Optional[Entity]:
        """
        查找匹配的实体
        
        Args:
            value: 要匹配的值
            entities: 实体列表
            
        Returns:
            Optional[Entity]: 匹配的实体
        """
        value_lower = value.lower().strip()
        
        # 精确匹配
        for entity in entities:
            if entity.label.lower().strip() == value_lower:
                return entity
        
        # 别名匹配
        for entity in entities:
            for alias in entity.aliases:
                if alias.lower().strip() == value_lower:
                    return entity
        
        # 属性值匹配
        for entity in entities:
            for prop_value in entity.properties.values():
                if str(prop_value).lower().strip() == value_lower:
                    return entity
        
        # 部分匹配（用于长文本）
        if len(value) > 5:
            for entity in entities:
                entity_label_lower = entity.label.lower()
                if (value_lower in entity_label_lower or 
                    entity_label_lower in value_lower):
                    return entity
        
        return None
    
    def _create_relation(self, relation_type: RelationType, 
                        rule: Dict[str, Any]) -> Relation:
        """
        创建关系对象
        
        Args:
            relation_type: 关系类型
            rule: 关系规则
            
        Returns:
            Relation: 关系对象
        """
        return Relation(
            type=relation_type,
            domain=rule['subject_types'][0] if rule['subject_types'] else None,
            range=rule['object_types'][0] if rule['object_types'] else None,
            confidence=rule['confidence']
        )
    
    def _extract_cross_record_relations(self, entities: List[Entity]) -> List[KnowledgeTriple]:
        """
        抽取跨记录关系（基于实体相似性）
        
        Args:
            entities: 实体列表
            
        Returns:
            List[KnowledgeTriple]: 三元组列表
        """
        triples = []
        
        # 查找相似的组织实体
        org_entities = [e for e in entities if e.type == EntityType.ORGANIZATION]
        
        for i, entity1 in enumerate(org_entities):
            for entity2 in org_entities[i+1:]:
                similarity = self._calculate_entity_similarity(entity1, entity2)
                
                if similarity > 0.8:  # 高相似度阈值
                    # 创建相似关系
                    similar_relation = Relation(
                        type=RelationType.SIMILAR_TO,
                        is_symmetric=True,
                        confidence=similarity
                    )
                    
                    triple = KnowledgeTriple(
                        subject=entity1,
                        predicate=similar_relation,
                        object=entity2,
                        confidence=similarity,
                        source="similarity_analysis",
                        evidence=[f"similarity_score:{similarity:.3f}"]
                    )
                    
                    triples.append(triple)
        
        return triples
    
    def _calculate_entity_similarity(self, entity1: Entity, entity2: Entity) -> float:
        """
        计算实体相似度
        
        Args:
            entity1: 实体1
            entity2: 实体2
            
        Returns:
            float: 相似度 (0-1)
        """
        if entity1.type != entity2.type:
            return 0.0
        
        # 标签相似度
        label_similarity = self._string_similarity(entity1.label, entity2.label)
        
        # 属性相似度
        property_similarity = self._property_similarity(entity1.properties, entity2.properties)
        
        # 综合相似度
        overall_similarity = (label_similarity * 0.7 + property_similarity * 0.3)
        
        return overall_similarity
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """计算字符串相似度（简单实现）"""
        if str1 == str2:
            return 1.0
        
        # 编辑距离相似度
        max_len = max(len(str1), len(str2))
        if max_len == 0:
            return 1.0
        
        # 简单的Jaccard相似度
        set1 = set(str1.lower())
        set2 = set(str2.lower())
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def _property_similarity(self, props1: Dict[str, Any], 
                           props2: Dict[str, Any]) -> float:
        """
        计算属性相似度
        
        Args:
            props1: 属性1
            props2: 属性2
            
        Returns:
            float: 相似度
        """
        if not props1 or not props2:
            return 0.0
        
        common_keys = set(props1.keys()).intersection(set(props2.keys()))
        if not common_keys:
            return 0.0
        
        similarities = []
        
        for key in common_keys:
            val1 = str(props1[key]).lower()
            val2 = str(props2[key]).lower()
            
            if val1 == val2:
                similarities.append(1.0)
            else:
                similarities.append(self._string_similarity(val1, val2))
        
        return sum(similarities) / len(similarities) if similarities else 0.0
    
    def _deduplicate_triples(self, triples: List[KnowledgeTriple]) -> List[KnowledgeTriple]:
        """
        三元组去重
        
        Args:
            triples: 三元组列表
            
        Returns:
            List[KnowledgeTriple]: 去重后的三元组列表
        """
        seen_keys = set()
        deduplicated = []
        
        for triple in triples:
            triple_key = triple.get_triple_key()
            
            if triple_key not in seen_keys:
                seen_keys.add(triple_key)
                deduplicated.append(triple)
            else:
                # 合并证据
                for existing_triple in deduplicated:
                    if existing_triple.get_triple_key() == triple_key:
                        existing_triple.evidence.extend(triple.evidence)
                        # 使用更高的置信度
                        if triple.confidence > existing_triple.confidence:
                            existing_triple.confidence = triple.confidence
                        break
        
        logger.info(f"关系去重: {len(triples)} -> {len(deduplicated)}")
        return deduplicated
    
    def extract_relations_from_text(self, text: str,
                                  entities: List[Entity],
                                  context: Dict[str, Any] = None) -> List[KnowledgeTriple]:
        """
        从自由文本中抽取关系（预留接口）
        
        Args:
            text: 文本内容
            entities: 相关实体
            context: 上下文信息
            
        Returns:
            List[KnowledgeTriple]: 抽取的关系列表
        """
        # TODO: 实现基于NLP的文本关系抽取
        # 可以集成依存句法分析、语义角色标注等技术
        logger.info("文本关系抽取功能待实现")
        return []