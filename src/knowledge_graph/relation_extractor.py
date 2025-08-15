"""
关系抽取器
从结构化数据中识别和抽取实体间关系
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple, Set
import pandas as pd
import math
from .kg_models import Entity, Relation, KnowledgeTriple, EntityType, RelationType

# NLP相关导入 - 可选依赖
try:
    import warnings
    # 抑制jieba的pkg_resources弃用警告
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="jieba._compat")
        import jieba
        import jieba.posseg as pseg
    HAS_JIEBA = True
    logger_init = logging.getLogger(__name__)
    logger_init.info("jieba中文分词库加载成功 - 关系抽取器")
except ImportError:
    HAS_JIEBA = False
    logger_init = logging.getLogger(__name__)
    logger_init.warning("jieba库未安装，将使用基础关系抽取")

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    HAS_SKLEARN = True
    logger_init.info("scikit-learn机器学习库加载成功 - 关系抽取器")
except ImportError:
    HAS_SKLEARN = False
    logger_init.warning("scikit-learn库未安装，将跳过语义关系分析")

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
        
        # 初始化语义推理组件
        self._init_semantic_components()
        
        # 构建关系模式库
        self.relation_patterns = self._build_relation_patterns()
        
        # 语义关系推理规则
        self.semantic_rules = self._build_semantic_rules()
        
        # 关系抽取规则（原有的基础规则）
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
        
        # 置信度阈值 - 降低阈值以获得更多关系
        self.confidence_threshold = self.config.get('confidence_threshold', 0.3)
        
    def _init_semantic_components(self) -> None:
        """初始化语义推理组件"""
        # 关系强度权重
        self.relation_strength_weights = {
            'field_pattern_match': 0.4,
            'semantic_similarity': 0.3,
            'context_coherence': 0.2,
            'domain_knowledge': 0.1
        }
        
        # 语义向量化器（如果可用）
        if HAS_SKLEARN:
            self.relation_vectorizer = TfidfVectorizer(
                max_features=500,
                ngram_range=(1, 3),
                analyzer='char'
            )
            logger.info("关系语义向量化器初始化完成")
        
        logger.info("语义推理组件初始化完成")
    
    def _build_relation_patterns(self) -> Dict[RelationType, Dict[str, Any]]:
        """构建关系模式库"""
        patterns = {
            RelationType.LOCATED_IN: {
                'trigger_words': ['位于', '在', '坐落', '地址', '所在'],
                'context_patterns': [
                    r'(.+?)(位于|在|坐落于)(.+?)',
                    r'(.+?)(地址|所在地)[:：]?(.+?)',
                    r'(.+?)(省|市|区|县|街道|路|号)(.+?)'
                ],
                'semantic_indicators': ['地理位置', '空间关系', '包含关系'],
                'inverse_relation': None,
                'transitivity': True
            },
            RelationType.MANAGED_BY: {
                'trigger_words': ['管理', '负责', '主管', '法人', '代表'],
                'context_patterns': [
                    r'(.+?)(由|被)(.+?)(管理|负责|主管)',
                    r'(.+?)(法人|代表|负责人)[:：]?(.+?)',
                    r'(.+?)(经理|主管|负责)(.+?)'
                ],
                'semantic_indicators': ['管理关系', '责任关系', '权威关系'],
                'inverse_relation': RelationType.REPRESENTS,
                'transitivity': False
            },
            RelationType.CONTACT_OF: {
                'trigger_words': ['联系', '电话', '手机', '邮箱', '联系方式'],
                'context_patterns': [
                    r'(.+?)(联系电话|电话|手机)[:：]?(.+?)',
                    r'(.+?)(联系方式|联系人)[:：]?(.+?)'
                ],
                'semantic_indicators': ['通信关系', '联系方式'],
                'inverse_relation': None,
                'transitivity': False
            },
            RelationType.REPRESENTS: {
                'trigger_words': ['代表', '法人', '代理', '代表人'],
                'context_patterns': [
                    r'(.+?)(代表|法人代表|法定代表人)(.+?)',
                    r'(.+?)(是|为)(.+?)(的)(代表|法人)'
                ],
                'semantic_indicators': ['代表关系', '法律关系', '授权关系'],
                'inverse_relation': RelationType.MANAGED_BY,
                'transitivity': False
            },
            RelationType.SIMILAR_TO: {
                'trigger_words': ['类似', '相似', '相同', '相近'],
                'context_patterns': [
                    r'(.+?)(与|和)(.+?)(类似|相似|相同)',
                    r'(.+?)(类似于|相似于)(.+?)'
                ],
                'semantic_indicators': ['相似关系', '同类关系'],
                'inverse_relation': RelationType.SIMILAR_TO,  # 对称关系
                'transitivity': True
            }
        }
        
        logger.info(f"构建关系模式库完成，包含 {len(patterns)} 种关系类型")
        return patterns
    
    def _build_semantic_rules(self) -> Dict[str, Dict[str, Any]]:
        """构建语义关系推理规则"""
        rules = {
            'transitivity_rules': {
                RelationType.LOCATED_IN: {
                    'rule': 'if A located_in B and B located_in C, then A located_in C',
                    'confidence_decay': 0.8,
                    'max_depth': 3
                },
                RelationType.SIMILAR_TO: {
                    'rule': 'if A similar_to B and B similar_to C, then A similar_to C',
                    'confidence_decay': 0.7,
                    'max_depth': 2
                }
            },
            'inverse_rules': {
                RelationType.MANAGED_BY: {
                    'inverse': RelationType.REPRESENTS,
                    'confidence_transfer': 0.9
                },
                RelationType.REPRESENTS: {
                    'inverse': RelationType.MANAGED_BY,
                    'confidence_transfer': 0.9
                }
            },
            'composition_rules': {
                'organization_hierarchy': {
                    'pattern': [RelationType.LOCATED_IN, RelationType.MANAGED_BY],
                    'result': RelationType.MANAGED_BY,
                    'confidence_formula': 'min(confidences) * 0.8'
                }
            },
            'constraint_rules': {
                'type_constraints': {
                    RelationType.MANAGED_BY: {
                        'subject_must_be': [EntityType.ORGANIZATION],
                        'object_must_be': [EntityType.PERSON],
                        'violation_penalty': 0.5
                    }
                }
            }
        }
        
        logger.info("语义关系推理规则构建完成")
        return rules
        
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
            
            # 语义关系推理增强
            if HAS_JIEBA:
                semantic_triples = self._extract_semantic_relations(entities, df, table_name)
                triples.extend(semantic_triples)
            
            # 关系推理（传递性、逆向关系等）
            inferred_triples = self._apply_semantic_reasoning(triples)
            triples.extend(inferred_triples)
            
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
        获取字段值，支持模糊匹配和备选字段
        
        Args:
            row: 数据行
            field_pattern: 字段模式
            
        Returns:
            Optional[str]: 字段值
        """
        # 定义字段备选方案
        field_alternatives = {
            'unit_name': ['ZT_JGMC', 'JGMC', 'FWCS_JGMC', 'YY_JGMC', 'NSYLJGMC', '机构名称', '单位名称'],
            'unit_address': ['ZCDZ', 'FWCS_DZ', 'NSYLJGDZ', '注册地址', '地址'],
            'legal_person': ['FDDBR', 'YY_FRXM', 'NSYLJGFR', '法定代表人', '法人'],
            'contact_phone': ['YY_LXDH', 'LXDH', '联系电话', '电话'],
            'company_name': ['ZT_JGMC', 'JGMC', 'FWCS_JGMC', 'YY_JGMC', '公司名称'],
            'address': ['ZCDZ', 'FWCS_DZ', 'NSYLJGDZ', '地址'],
            'person_name': ['YY_FRXM', 'FDDBR', 'NSYLJGFR', '姓名', '人员']
        }
        
        # 获取备选字段列表
        alternatives = field_alternatives.get(field_pattern, [field_pattern])
        
        # 按优先级尝试每个备选字段
        for alt_field in alternatives:
            # 直接匹配
            if alt_field in row.index:
                value = row[alt_field]
                if pd.notna(value) and str(value).strip() and str(value).strip() != '':
                    return str(value).strip()
            
            # 模糊匹配
            alt_field_lower = alt_field.lower()
            for col in row.index:
                col_lower = col.lower()
                if (alt_field_lower in col_lower or 
                    col_lower in alt_field_lower or
                    self._are_similar_fields(alt_field_lower, col_lower)):
                    value = row[col]
                    if pd.notna(value) and str(value).strip() and str(value).strip() != '':
                        return str(value).strip()
        
        # 原有的模糊匹配逻辑作为最后备选
        field_pattern_lower = field_pattern.lower()
        for col in row.index:
            col_lower = col.lower()
            if (field_pattern_lower in col_lower or 
                col_lower in field_pattern_lower or
                self._are_similar_fields(field_pattern_lower, col_lower)):
                value = row[col]
                if pd.notna(value) and str(value).strip() and str(value).strip() != '':
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
        # 扩展的同义词映射
        synonyms = {
            'name': ['名称', '姓名', 'dwmc', 'unit_name', 'jgmc', 'zt_jgmc', 'fwcs_jgmc', 'yy_jgmc', 'nsyljgmc'],
            'address': ['地址', '位置', 'dwdz', 'unit_address', 'zcdz', 'fwcs_dz', 'nsyljgdz'],
            'phone': ['电话', '手机', 'lxdh', 'contact_phone', 'yy_lxdh', 'dh'],
            'legal_person': ['法人', '代表', 'fddbr', 'legal_people', 'yy_frxm', 'nsyljgfr', 'frxm'],
            'manager': ['管理', '负责', '经理', 'security_manager', 'nsyljgzyfzr'],
            'organization': ['机构', '组织', '单位', 'jg', 'dw'],
            'location': ['位置', '地点', '地址', 'wz', 'dd', 'dz']
        }
        
        # 转换为小写进行比较
        field1_lower = field1.lower()
        field2_lower = field2.lower()
        
        for key, values in synonyms.items():
            # 将所有值转换为小写
            values_lower = [v.lower() for v in values]
            
            if ((field1_lower in values_lower and field2_lower in values_lower) or
                (key in field1_lower and any(v in field2_lower for v in values_lower)) or
                (key in field2_lower and any(v in field1_lower for v in values_lower))):
                return True
        
        # 检查直接包含关系
        if len(field1_lower) > 2 and len(field2_lower) > 2:
            if field1_lower in field2_lower or field2_lower in field1_lower:
                return True
        
        return False
    
    def _find_matching_entity(self, value: str, entities: List[Entity]) -> Optional[Entity]:
        """
        查找匹配的实体（优化版：更宽松的匹配）
        
        Args:
            value: 要匹配的值
            entities: 实体列表
            
        Returns:
            Optional[Entity]: 匹配的实体
        """
        if not value or not value.strip():
            return None
            
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
        
        # 部分匹配（降低长度要求）
        if len(value) > 3:
            for entity in entities:
                entity_label_lower = entity.label.lower()
                if (value_lower in entity_label_lower or 
                    entity_label_lower in value_lower):
                    return entity
        
        # 更宽松的匹配：基于关键词
        if len(value) > 2:
            value_keywords = set(value_lower.split())
            for entity in entities:
                entity_keywords = set(entity.label.lower().split())
                # 如果有共同关键词，认为匹配
                if value_keywords & entity_keywords:
                    return entity
        
        # 最后的尝试：模糊匹配（适用于短字符串）
        if len(value) >= 2:
            for entity in entities:
                # 计算简单相似度
                if self._simple_similarity(value_lower, entity.label.lower()) > 0.6:
                    return entity
        
        return None
    
    def _simple_similarity(self, str1: str, str2: str) -> float:
        """计算简单字符串相似度"""
        if not str1 or not str2:
            return 0.0
        if str1 == str2:
            return 1.0
        
        # Jaccard相似度
        set1 = set(str1)
        set2 = set(str2)
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
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
    
    def _extract_semantic_relations(self, entities: List[Entity], 
                                   df: pd.DataFrame, 
                                   table_name: str) -> List[KnowledgeTriple]:
        """
        基于语义分析抽取关系
        
        Args:
            entities: 实体列表
            df: 数据框
            table_name: 表名
            
        Returns:
            List[KnowledgeTriple]: 语义关系三元组
        """
        if not HAS_JIEBA:
            return []
        
        try:
            semantic_triples = []
            
            # 1. 基于文本内容的关系抽取
            for _, row in df.iterrows():
                row_text = ' '.join([str(val) for val in row.values if pd.notna(val)])
                if len(row_text.strip()) < 10:  # 跳过内容过少的行
                    continue
                
                # 从文本中抽取关系
                text_triples = self._extract_relations_from_text_content(
                    row_text, entities, table_name
                )
                semantic_triples.extend(text_triples)
            
            # 2. 基于字段语义相似度的关系推断
            field_triples = self._infer_relations_from_field_semantics(
                entities, df.columns.tolist(), table_name
            )
            semantic_triples.extend(field_triples)
            
            # 3. 基于实体共现模式的关系发现
            cooccurrence_triples = self._discover_relations_from_cooccurrence(
                entities, df, table_name
            )
            semantic_triples.extend(cooccurrence_triples)
            
            logger.info(f"语义关系抽取完成，发现 {len(semantic_triples)} 个关系")
            return semantic_triples
            
        except Exception as e:
            logger.error(f"语义关系抽取失败: {e}")
            return []
    
    def _extract_relations_from_text_content(self, text: str,
                                           entities: List[Entity],
                                           table_name: str) -> List[KnowledgeTriple]:
        """从文本内容中抽取关系"""
        triples = []
        
        try:
            # 使用jieba分词和词性标注
            words = list(pseg.cut(text))
            
            # 查找关系触发词和模式
            for relation_type, pattern_info in self.relation_patterns.items():
                trigger_words = pattern_info['trigger_words']
                context_patterns = pattern_info['context_patterns']
                
                # 检查触发词
                trigger_found = any(word in text for word in trigger_words)
                if not trigger_found:
                    continue
                
                # 使用正则模式匹配
                for pattern in context_patterns:
                    matches = re.finditer(pattern, text)
                    
                    for match in matches:
                        groups = match.groups()
                        if len(groups) >= 2:
                            subject_text = groups[0].strip()
                            object_text = groups[-1].strip()
                            
                            # 查找匹配的实体
                            subject_entity = self._find_entity_by_text(subject_text, entities)
                            object_entity = self._find_entity_by_text(object_text, entities)
                            
                            if subject_entity and object_entity:
                                # 计算语义关系置信度
                                confidence = self._calculate_semantic_relation_confidence(
                                    subject_entity, object_entity, relation_type, text
                                )
                                
                                if confidence >= self.confidence_threshold:
                                    relation = self._create_semantic_relation(
                                        relation_type, confidence
                                    )
                                    
                                    triple = KnowledgeTriple(
                                        subject=subject_entity,
                                        predicate=relation,
                                        object=object_entity,
                                        confidence=confidence,
                                        source=f"semantic_text:{table_name}",
                                        evidence=[f"pattern:{pattern}", f"text:{text[:100]}..."]
                                    )
                                    
                                    triples.append(triple)
            
            return triples
            
        except Exception as e:
            logger.debug(f"文本关系抽取失败: {e}")
            return []
    
    def _infer_relations_from_field_semantics(self, entities: List[Entity],
                                            field_names: List[str],
                                            table_name: str) -> List[KnowledgeTriple]:
        """基于字段语义相似度推断关系"""
        triples = []
        
        if not HAS_SKLEARN:
            return triples
        
        try:
            # 构建字段语义向量
            field_vectors = self._build_field_semantic_vectors(field_names)
            
            # 分析字段间的语义关系
            for i, field1 in enumerate(field_names):
                for j, field2 in enumerate(field_names[i+1:], i+1):
                    semantic_similarity = self._calculate_field_semantic_similarity(
                        field1, field2, field_vectors
                    )
                    
                    if semantic_similarity > 0.7:  # 高语义相似度阈值
                        # 推断可能的关系类型
                        relation_type = self._infer_relation_type_from_fields(field1, field2)
                        
                        if relation_type:
                            # 查找相关实体
                            field1_entities = [e for e in entities if e.source_column == field1]
                            field2_entities = [e for e in entities if e.source_column == field2]
                            
                            for e1 in field1_entities:
                                for e2 in field2_entities:
                                    if e1.source_record_id == e2.source_record_id:
                                        confidence = semantic_similarity * 0.8  # 调整置信度
                                        
                                        relation = self._create_semantic_relation(
                                            relation_type, confidence
                                        )
                                        
                                        triple = KnowledgeTriple(
                                            subject=e1,
                                            predicate=relation,
                                            object=e2,
                                            confidence=confidence,
                                            source=f"field_semantics:{table_name}",
                                            evidence=[f"field_similarity:{semantic_similarity:.3f}"]
                                        )
                                        
                                        triples.append(triple)
            
            return triples
            
        except Exception as e:
            logger.debug(f"字段语义关系推断失败: {e}")
            return []
    
    def _discover_relations_from_cooccurrence(self, entities: List[Entity],
                                            df: pd.DataFrame,
                                            table_name: str) -> List[KnowledgeTriple]:
        """基于实体共现模式发现关系"""
        triples = []
        
        try:
            # 分析实体共现模式
            cooccurrence_matrix = self._build_entity_cooccurrence_matrix(entities)
            
            # 基于共现频率和模式推断关系
            for entity1 in entities:
                for entity2 in entities:
                    if entity1.id == entity2.id:
                        continue
                    
                    cooccurrence_score = self._calculate_cooccurrence_score(
                        entity1, entity2, cooccurrence_matrix
                    )
                    
                    if cooccurrence_score > 0.6:
                        # 推断关系类型
                        relation_type = self._infer_relation_from_cooccurrence(
                            entity1, entity2, cooccurrence_score
                        )
                        
                        if relation_type:
                            confidence = cooccurrence_score * 0.7
                            
                            relation = self._create_semantic_relation(
                                relation_type, confidence
                            )
                            
                            triple = KnowledgeTriple(
                                subject=entity1,
                                predicate=relation,
                                object=entity2,
                                confidence=confidence,
                                source=f"cooccurrence:{table_name}",
                                evidence=[f"cooccurrence_score:{cooccurrence_score:.3f}"]
                            )
                            
                            triples.append(triple)
            
            return triples
            
        except Exception as e:
            logger.debug(f"共现关系发现失败: {e}")
            return []
    
    def _apply_semantic_reasoning(self, triples: List[KnowledgeTriple]) -> List[KnowledgeTriple]:
        """应用语义推理规则"""
        inferred_triples = []
        
        try:
            # 1. 传递性推理
            transitive_triples = self._apply_transitivity_rules(triples)
            inferred_triples.extend(transitive_triples)
            
            # 2. 逆向关系推理
            inverse_triples = self._apply_inverse_rules(triples)
            inferred_triples.extend(inverse_triples)
            
            # 3. 组合关系推理
            composition_triples = self._apply_composition_rules(triples)
            inferred_triples.extend(composition_triples)
            
            # 4. 约束验证和修正
            validated_triples = self._validate_with_constraints(inferred_triples)
            
            logger.info(f"语义推理完成，推断出 {len(validated_triples)} 个新关系")
            return validated_triples
            
        except Exception as e:
            logger.error(f"语义推理失败: {e}")
            return []
    
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
        从自由文本中抽取关系（基于NLP实现）
        
        Args:
            text: 文本内容
            entities: 相关实体
            context: 上下文信息
            
        Returns:
            List[KnowledgeTriple]: 抽取的关系列表
        """
        if not HAS_JIEBA:
            logger.warning("jieba库未安装，无法进行文本关系抽取")
            return []
        
        return self._extract_relations_from_text_content(text, entities, 
                                                       context.get('table_name', 'text_input'))
    
    # 辅助方法实现
    def _find_entity_by_text(self, text: str, entities: List[Entity]) -> Optional[Entity]:
        """根据文本查找匹配的实体"""
        text_lower = text.lower().strip()
        
        # 精确匹配
        for entity in entities:
            if entity.label.lower().strip() == text_lower:
                return entity
        
        # 包含匹配
        for entity in entities:
            if text_lower in entity.label.lower() or entity.label.lower() in text_lower:
                return entity
        
        return None
    
    def _calculate_semantic_relation_confidence(self, subject: Entity, object_entity: Entity,
                                              relation_type: RelationType, context: str) -> float:
        """计算语义关系置信度"""
        base_confidence = 0.6
        
        # 实体类型匹配度
        pattern_info = self.relation_patterns.get(relation_type, {})
        subject_types = pattern_info.get('subject_types', [])
        object_types = pattern_info.get('object_types', [])
        
        type_match_score = 0.0
        if subject.type in subject_types:
            type_match_score += 0.2
        if object_entity.type in object_types:
            type_match_score += 0.2
        
        # 上下文相关性
        context_score = 0.1  # 基础上下文分数
        
        return min(base_confidence + type_match_score + context_score, 1.0)
    
    def _create_semantic_relation(self, relation_type: RelationType, confidence: float) -> Relation:
        """创建语义关系对象"""
        return Relation(
            type=relation_type,
            confidence=confidence,
            properties={'extraction_method': 'semantic_analysis'}
        )
    
    def _build_field_semantic_vectors(self, field_names: List[str]) -> Optional[Any]:
        """构建字段语义向量"""
        if not HAS_SKLEARN:
            return None
        
        try:
            return self.relation_vectorizer.fit_transform(field_names)
        except Exception as e:
            logger.debug(f"字段向量构建失败: {e}")
            return None
    
    def _calculate_field_semantic_similarity(self, field1: str, field2: str, vectors: Any) -> float:
        """计算字段语义相似度"""
        if vectors is None or not HAS_SKLEARN:
            return 0.0
        
        try:
            # 简化实现：基于字符相似度
            return self._string_similarity(field1, field2)
        except Exception as e:
            logger.debug(f"字段相似度计算失败: {e}")
            return 0.0
    
    def _infer_relation_type_from_fields(self, field1: str, field2: str) -> Optional[RelationType]:
        """从字段名推断关系类型"""
        field1_lower = field1.lower()
        field2_lower = field2.lower()
        
        # 地址关系
        if ('address' in field1_lower or '地址' in field1_lower) and \
           ('name' in field2_lower or '名称' in field2_lower):
            return RelationType.LOCATED_IN
        
        # 管理关系
        if ('manager' in field1_lower or '负责' in field1_lower or '法人' in field1_lower) and \
           ('name' in field2_lower or '名称' in field2_lower):
            return RelationType.MANAGED_BY
        
        # 联系关系
        if ('phone' in field1_lower or 'contact' in field1_lower or '电话' in field1_lower) and \
           ('name' in field2_lower or '名称' in field2_lower):
            return RelationType.CONTACT_OF
        
        return None
    
    def _build_entity_cooccurrence_matrix(self, entities: List[Entity]) -> Dict[str, Dict[str, float]]:
        """构建实体共现矩阵"""
        matrix = {}
        
        # 按记录ID分组实体
        records = {}
        for entity in entities:
            record_id = entity.source_record_id
            if record_id not in records:
                records[record_id] = []
            records[record_id].append(entity)
        
        # 计算共现频率
        for record_entities in records.values():
            for i, entity1 in enumerate(record_entities):
                for entity2 in record_entities[i+1:]:
                    key1 = entity1.id
                    key2 = entity2.id
                    
                    if key1 not in matrix:
                        matrix[key1] = {}
                    if key2 not in matrix:
                        matrix[key2] = {}
                    
                    matrix[key1][key2] = matrix[key1].get(key2, 0) + 1
                    matrix[key2][key1] = matrix[key2].get(key1, 0) + 1
        
        return matrix
    
    def _calculate_cooccurrence_score(self, entity1: Entity, entity2: Entity, 
                                    matrix: Dict[str, Dict[str, float]]) -> float:
        """计算共现分数"""
        key1, key2 = entity1.id, entity2.id
        
        if key1 in matrix and key2 in matrix[key1]:
            return min(matrix[key1][key2] / 10.0, 1.0)  # 标准化到0-1
        
        return 0.0
    
    def _infer_relation_from_cooccurrence(self, entity1: Entity, entity2: Entity, 
                                        score: float) -> Optional[RelationType]:
        """基于共现推断关系类型"""
        # 基于实体类型组合推断关系
        if entity1.type == EntityType.ORGANIZATION and entity2.type == EntityType.LOCATION:
            return RelationType.LOCATED_IN
        elif entity1.type == EntityType.ORGANIZATION and entity2.type == EntityType.PERSON:
            return RelationType.MANAGED_BY
        elif entity1.type == EntityType.PERSON and entity2.type == EntityType.ORGANIZATION:
            return RelationType.REPRESENTS
        
        return None
    
    def _apply_transitivity_rules(self, triples: List[KnowledgeTriple]) -> List[KnowledgeTriple]:
        """应用传递性规则"""
        inferred = []
        
        transitivity_rules = self.semantic_rules.get('transitivity_rules', {})
        
        for relation_type, rule_info in transitivity_rules.items():
            confidence_decay = rule_info['confidence_decay']
            max_depth = rule_info['max_depth']
            
            # 查找该类型的所有三元组
            type_triples = [t for t in triples if t.predicate.type == relation_type]
            
            # 应用传递性
            for triple1 in type_triples:
                for triple2 in type_triples:
                    if (triple1.object.id == triple2.subject.id and 
                        triple1.subject.id != triple2.object.id):
                        
                        # 创建传递关系
                        new_confidence = min(triple1.confidence, triple2.confidence) * confidence_decay
                        
                        if new_confidence >= self.confidence_threshold:
                            relation = self._create_semantic_relation(relation_type, new_confidence)
                            
                            inferred_triple = KnowledgeTriple(
                                subject=triple1.subject,
                                predicate=relation,
                                object=triple2.object,
                                confidence=new_confidence,
                                source="transitivity_inference",
                                evidence=[f"via:{triple1.object.label}"]
                            )
                            
                            inferred.append(inferred_triple)
        
        return inferred
    
    def _apply_inverse_rules(self, triples: List[KnowledgeTriple]) -> List[KnowledgeTriple]:
        """应用逆向关系规则"""
        inferred = []
        
        inverse_rules = self.semantic_rules.get('inverse_rules', {})
        
        for triple in triples:
            relation_type = triple.predicate.type
            
            if relation_type in inverse_rules:
                rule_info = inverse_rules[relation_type]
                inverse_type = rule_info['inverse']
                confidence_transfer = rule_info['confidence_transfer']
                
                new_confidence = triple.confidence * confidence_transfer
                
                if new_confidence >= self.confidence_threshold:
                    inverse_relation = self._create_semantic_relation(inverse_type, new_confidence)
                    
                    inverse_triple = KnowledgeTriple(
                        subject=triple.object,
                        predicate=inverse_relation,
                        object=triple.subject,
                        confidence=new_confidence,
                        source="inverse_inference",
                        evidence=[f"inverse_of:{triple.id}"]
                    )
                    
                    inferred.append(inverse_triple)
        
        return inferred
    
    def _apply_composition_rules(self, triples: List[KnowledgeTriple]) -> List[KnowledgeTriple]:
        """应用组合关系规则"""
        # 简化实现，返回空列表
        return []
    
    def _validate_with_constraints(self, triples: List[KnowledgeTriple]) -> List[KnowledgeTriple]:
        """使用约束验证三元组"""
        validated = []
        
        constraint_rules = self.semantic_rules.get('constraint_rules', {})
        type_constraints = constraint_rules.get('type_constraints', {})
        
        for triple in triples:
            relation_type = triple.predicate.type
            
            if relation_type in type_constraints:
                constraint = type_constraints[relation_type]
                subject_types = constraint.get('subject_must_be', [])
                object_types = constraint.get('object_must_be', [])
                penalty = constraint.get('violation_penalty', 0.5)
                
                # 检查类型约束
                valid = True
                if subject_types and triple.subject.type not in subject_types:
                    triple.confidence *= penalty
                    valid = False
                
                if object_types and triple.object.type not in object_types:
                    triple.confidence *= penalty
                    valid = False
                
                # 只保留置信度足够的三元组
                if triple.confidence >= self.confidence_threshold:
                    validated.append(triple)
            else:
                validated.append(triple)
        
        return validated
    
    def get_relation_extraction_statistics(self) -> Dict[str, Any]:
        """获取关系抽取统计信息"""
        return {
            'nlp_available': HAS_JIEBA,
            'sklearn_available': HAS_SKLEARN,
            'supported_relation_types': [rt.value for rt in RelationType],
            'relation_patterns_count': len(self.relation_patterns),
            'semantic_rules_count': sum(len(rules) for rules in self.semantic_rules.values()),
            'confidence_threshold': self.confidence_threshold,
            'extraction_methods': [
                'field_pattern_matching',
                'semantic_text_analysis' if HAS_JIEBA else 'semantic_text_analysis_disabled',
                'field_semantics' if HAS_SKLEARN else 'field_semantics_disabled',
                'cooccurrence_analysis',
                'transitivity_inference',
                'inverse_relation_inference',
                'constraint_validation'
            ]
        }