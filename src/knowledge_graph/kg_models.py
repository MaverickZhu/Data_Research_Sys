"""
知识图谱数据模型
定义实体、关系、三元组等核心数据结构
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from enum import Enum
import uuid

class EntityType(Enum):
    """实体类型枚举"""
    ORGANIZATION = "ORGANIZATION"      # 组织机构
    PERSON = "PERSON"                  # 人员
    LOCATION = "LOCATION"              # 地址位置
    IDENTIFIER = "IDENTIFIER"          # 标识符
    PRODUCT = "PRODUCT"                # 产品服务
    TIME = "TIME"                      # 时间
    MONEY = "MONEY"                    # 金额
    EVENT = "EVENT"                    # 事件
    CONCEPT = "CONCEPT"                # 概念
    UNKNOWN = "UNKNOWN"                # 未知类型

class RelationType(Enum):
    """关系类型枚举"""
    LOCATED_IN = "LOCATED_IN"          # 位于
    OWNED_BY = "OWNED_BY"              # 拥有
    EMPLOYED_BY = "EMPLOYED_BY"        # 雇佣
    PARTNERSHIP = "PARTNERSHIP"        # 合作关系
    SUPPLY_CHAIN = "SUPPLY_CHAIN"      # 供应链关系
    COMPETITION = "COMPETITION"        # 竞争关系
    SUBSIDIARY = "SUBSIDIARY"          # 子公司关系
    INVESTMENT = "INVESTMENT"          # 投资关系
    REGULATION_BY = "REGULATION_BY"    # 监管关系
    SIMILAR_TO = "SIMILAR_TO"          # 相似关系
    MANAGED_BY = "MANAGED_BY"          # 管理关系
    CONTACT_OF = "CONTACT_OF"          # 联系关系
    REPRESENTS = "REPRESENTS"          # 代表关系
    CUSTOM = "CUSTOM"                  # 自定义关系

@dataclass
class Entity:
    """实体类"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: EntityType = EntityType.UNKNOWN
    label: str = ""
    properties: Dict[str, Any] = field(default_factory=dict)
    aliases: List[str] = field(default_factory=list)
    source_table: Optional[str] = None
    source_column: Optional[str] = None
    source_record_id: Optional[str] = None
    confidence: float = 1.0
    created_time: datetime = field(default_factory=datetime.now)
    updated_time: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.label and 'name' in self.properties:
            self.label = str(self.properties['name'])
    
    def add_property(self, key: str, value: Any) -> None:
        """添加属性"""
        self.properties[key] = value
        self.updated_time = datetime.now()
    
    def add_alias(self, alias: str) -> None:
        """添加别名"""
        if alias not in self.aliases:
            self.aliases.append(alias)
            self.updated_time = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'type': self.type.value,
            'label': self.label,
            'properties': self.properties,
            'aliases': self.aliases,
            'source_table': self.source_table,
            'source_column': self.source_column,
            'source_record_id': self.source_record_id,
            'confidence': self.confidence,
            'created_time': self.created_time.isoformat(),
            'updated_time': self.updated_time.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """从字典创建实体"""
        entity = cls(
            id=data.get('id', str(uuid.uuid4())),
            type=EntityType(data.get('type', EntityType.UNKNOWN.value)),
            label=data.get('label', ''),
            properties=data.get('properties', {}),
            aliases=data.get('aliases', []),
            source_table=data.get('source_table'),
            source_column=data.get('source_column'),
            source_record_id=data.get('source_record_id'),
            confidence=data.get('confidence', 1.0)
        )
        
        if 'created_time' in data:
            entity.created_time = datetime.fromisoformat(data['created_time'])
        if 'updated_time' in data:
            entity.updated_time = datetime.fromisoformat(data['updated_time'])
            
        return entity

@dataclass
class Relation:
    """关系类"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: RelationType = RelationType.CUSTOM
    label: str = ""
    properties: Dict[str, Any] = field(default_factory=dict)
    domain: Optional[EntityType] = None      # 定义域（主语实体类型）
    range: Optional[EntityType] = None       # 值域（宾语实体类型）
    inverse_relation: Optional[str] = None   # 逆关系
    is_symmetric: bool = False               # 是否对称关系
    is_transitive: bool = False              # 是否传递关系
    confidence: float = 1.0
    created_time: datetime = field(default_factory=datetime.now)
    updated_time: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.label:
            self.label = self.type.value.replace('_', ' ').title()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'type': self.type.value,
            'label': self.label,
            'properties': self.properties,
            'domain': self.domain.value if self.domain else None,
            'range': self.range.value if self.range else None,
            'inverse_relation': self.inverse_relation,
            'is_symmetric': self.is_symmetric,
            'is_transitive': self.is_transitive,
            'confidence': self.confidence,
            'created_time': self.created_time.isoformat(),
            'updated_time': self.updated_time.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relation':
        """从字典创建关系"""
        relation = cls(
            id=data.get('id', str(uuid.uuid4())),
            type=RelationType(data.get('type', RelationType.CUSTOM.value)),
            label=data.get('label', ''),
            properties=data.get('properties', {}),
            domain=EntityType(data['domain']) if data.get('domain') else None,
            range=EntityType(data['range']) if data.get('range') else None,
            inverse_relation=data.get('inverse_relation'),
            is_symmetric=data.get('is_symmetric', False),
            is_transitive=data.get('is_transitive', False),
            confidence=data.get('confidence', 1.0)
        )
        
        if 'created_time' in data:
            relation.created_time = datetime.fromisoformat(data['created_time'])
        if 'updated_time' in data:
            relation.updated_time = datetime.fromisoformat(data['updated_time'])
            
        return relation

@dataclass
class KnowledgeTriple:
    """知识三元组 (Subject, Predicate, Object)"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    subject: Entity = None                   # 主语实体
    predicate: Relation = None               # 谓语关系  
    object: Entity = None                    # 宾语实体
    confidence: float = 1.0                  # 置信度
    source: str = "extracted"                # 数据来源
    evidence: List[str] = field(default_factory=list)  # 支持证据
    created_time: datetime = field(default_factory=datetime.now)
    updated_time: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """验证三元组的完整性"""
        if not all([self.subject, self.predicate, self.object]):
            raise ValueError("三元组必须包含主语、谓语和宾语")
    
    def get_triple_key(self) -> str:
        """获取三元组的唯一键"""
        return f"{self.subject.id}|{self.predicate.id}|{self.object.id}"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'subject': self.subject.to_dict(),
            'predicate': self.predicate.to_dict(),
            'object': self.object.to_dict(),
            'confidence': self.confidence,
            'source': self.source,
            'evidence': self.evidence,
            'created_time': self.created_time.isoformat(),
            'updated_time': self.updated_time.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'KnowledgeTriple':
        """从字典创建三元组"""
        triple = cls(
            id=data.get('id', str(uuid.uuid4())),
            subject=Entity.from_dict(data['subject']),
            predicate=Relation.from_dict(data['predicate']),
            object=Entity.from_dict(data['object']),
            confidence=data.get('confidence', 1.0),
            source=data.get('source', 'extracted'),
            evidence=data.get('evidence', [])
        )
        
        if 'created_time' in data:
            triple.created_time = datetime.fromisoformat(data['created_time'])
        if 'updated_time' in data:
            triple.updated_time = datetime.fromisoformat(data['updated_time'])
            
        return triple
    
    def is_valid(self) -> bool:
        """验证三元组是否有效"""
        # 检查基本完整性
        if not all([self.subject, self.predicate, self.object]):
            return False
        
        # 检查置信度范围
        if not 0.0 <= self.confidence <= 1.0:
            return False
        
        # 检查领域和值域约束
        if (self.predicate.domain and 
            self.subject.type != self.predicate.domain):
            return False
            
        if (self.predicate.range and 
            self.object.type != self.predicate.range):
            return False
        
        return True

@dataclass
class Ontology:
    """本体类 - 定义领域概念和关系的规范"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    version: str = "1.0"
    entity_types: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    relation_types: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    constraints: List[Dict[str, Any]] = field(default_factory=list)
    created_time: datetime = field(default_factory=datetime.now)
    updated_time: datetime = field(default_factory=datetime.now)
    
    def add_entity_type(self, entity_type: EntityType, 
                       properties: Dict[str, Any] = None) -> None:
        """添加实体类型定义"""
        self.entity_types[entity_type.value] = {
            'type': entity_type.value,
            'properties': properties or {},
            'description': properties.get('description', '') if properties else ''
        }
        self.updated_time = datetime.now()
    
    def add_relation_type(self, relation_type: RelationType,
                         domain: EntityType = None,
                         range: EntityType = None,
                         properties: Dict[str, Any] = None) -> None:
        """添加关系类型定义"""
        self.relation_types[relation_type.value] = {
            'type': relation_type.value,
            'domain': domain.value if domain else None,
            'range': range.value if range else None,
            'properties': properties or {},
            'description': properties.get('description', '') if properties else ''
        }
        self.updated_time = datetime.now()
    
    def add_constraint(self, constraint_type: str, 
                      description: str, rule: str) -> None:
        """添加约束规则"""
        constraint = {
            'type': constraint_type,
            'description': description,
            'rule': rule,
            'created_time': datetime.now().isoformat()
        }
        self.constraints.append(constraint)
        self.updated_time = datetime.now()
    
    def validate_triple(self, triple: KnowledgeTriple) -> bool:
        """根据本体规则验证三元组"""
        # 检查实体类型是否在本体中定义
        if triple.subject.type.value not in self.entity_types:
            return False
        if triple.object.type.value not in self.entity_types:
            return False
        
        # 检查关系类型是否在本体中定义
        if triple.predicate.type.value not in self.relation_types:
            return False
        
        # 检查领域和值域约束
        relation_def = self.relation_types[triple.predicate.type.value]
        if (relation_def.get('domain') and 
            triple.subject.type.value != relation_def['domain']):
            return False
        if (relation_def.get('range') and 
            triple.object.type.value != relation_def['range']):
            return False
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'version': self.version,
            'entity_types': self.entity_types,
            'relation_types': self.relation_types,
            'constraints': self.constraints,
            'created_time': self.created_time.isoformat(),
            'updated_time': self.updated_time.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Ontology':
        """从字典创建本体"""
        ontology = cls(
            id=data.get('id', str(uuid.uuid4())),
            name=data.get('name', ''),
            description=data.get('description', ''),
            version=data.get('version', '1.0'),
            entity_types=data.get('entity_types', {}),
            relation_types=data.get('relation_types', {}),
            constraints=data.get('constraints', [])
        )
        
        if 'created_time' in data:
            ontology.created_time = datetime.fromisoformat(data['created_time'])
        if 'updated_time' in data:
            ontology.updated_time = datetime.fromisoformat(data['updated_time'])
            
        return ontology

# 预定义的关系约束
PREDEFINED_RELATIONS = {
    RelationType.LOCATED_IN: {
        'domain': EntityType.ORGANIZATION,
        'range': EntityType.LOCATION,
        'is_transitive': True,
        'inverse': 'CONTAINS'
    },
    RelationType.MANAGED_BY: {
        'domain': EntityType.ORGANIZATION,
        'range': EntityType.PERSON,
        'inverse': 'MANAGES'
    },
    RelationType.OWNED_BY: {
        'domain': EntityType.ORGANIZATION,
        'range': EntityType.PERSON,
        'inverse': 'OWNS'
    },
    RelationType.SIMILAR_TO: {
        'domain': EntityType.ORGANIZATION,
        'range': EntityType.ORGANIZATION,
        'is_symmetric': True
    }
}

def create_default_ontology(domain_name: str = "general") -> Ontology:
    """创建默认本体"""
    ontology = Ontology(
        name=f"{domain_name}_ontology",
        description=f"Default ontology for {domain_name} domain",
        version="1.0"
    )
    
    # 添加基础实体类型
    for entity_type in EntityType:
        ontology.add_entity_type(entity_type)
    
    # 添加基础关系类型
    for relation_type in RelationType:
        relation_config = PREDEFINED_RELATIONS.get(relation_type, {})
        ontology.add_relation_type(
            relation_type,
            domain=relation_config.get('domain'),
            range=relation_config.get('range'),
            properties=relation_config
        )
    
    return ontology