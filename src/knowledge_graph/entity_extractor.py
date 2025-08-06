"""
实体抽取器
从结构化数据中识别和抽取实体
"""

import re
import logging
from typing import Dict, List, Any, Optional, Set, Tuple
import pandas as pd
from .kg_models import Entity, EntityType
from ..data_manager.schema_detector import SchemaDetector

logger = logging.getLogger(__name__)

class EntityExtractor:
    """实体抽取器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化实体抽取器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        self.schema_detector = SchemaDetector()
        
        # 实体识别模式
        self.entity_patterns = {
            EntityType.ORGANIZATION: {
                'keywords': ['公司', '企业', '集团', '有限', '股份', '合作社', '事务所', '中心', '机构'],
                'suffixes': ['有限公司', '股份公司', '集团公司', 'Co.Ltd', 'Inc.', 'Corp.', 'LLC'],
                'regex': r'.*(公司|企业|集团|有限|股份|合作社|事务所|中心|机构).*'
            },
            EntityType.PERSON: {
                'keywords': ['法人', '代表', '负责人', '经理', '主管', '董事', '总裁'],
                'name_patterns': [r'^[\u4e00-\u9fff]{2,4}$', r'^[A-Za-z\s]{2,50}$'],
                'fields': ['法人', '代表', '负责人', '联系人', '经理']
            },
            EntityType.LOCATION: {
                'keywords': ['省', '市', '区', '县', '街道', '路', '号', '楼', '层'],
                'regex': r'.*(省|市|区|县|街道|路|号|楼|层).*',
                'fields': ['地址', '位置', '所在地', '注册地']
            },
            EntityType.IDENTIFIER: {
                'credit_code': r'^[0-9A-HJ-NPQRTUWXY]{2}\d{6}[0-9A-HJ-NPQRTUWXY]{10}$',
                'id_card': r'^\d{15}$|^\d{17}[\dX]$',
                'phone': r'^(\+86)?1[3-9]\d{9}$|^\d{3,4}-\d{7,8}$',
                'fields': ['信用代码', '身份证', '电话', '手机', '联系方式']
            }
        }
        
        # 实体置信度阈值
        self.confidence_thresholds = self.config.get('confidence_thresholds', {
            'high': 0.9,
            'medium': 0.7,
            'low': 0.5
        })
        
    def extract_entities_from_dataframe(self, df: pd.DataFrame, 
                                      table_name: str = None) -> List[Entity]:
        """
        从DataFrame中抽取实体
        
        Args:
            df: 数据框
            table_name: 表名
            
        Returns:
            List[Entity]: 抽取的实体列表
        """
        try:
            logger.info(f"开始从表 {table_name} 抽取实体，共 {len(df)} 行数据")
            
            entities = []
            
            # 首先进行模式检测，识别实体字段
            schema = self.schema_detector.detect_schema(df, table_name)
            entity_fields = schema.get('entity_fields', {})
            
            # 为每个实体类型抽取实体
            for entity_type_str, field_names in entity_fields.items():
                try:
                    entity_type = EntityType(entity_type_str)
                    extracted_entities = self._extract_entities_by_type(
                        df, entity_type, field_names, table_name
                    )
                    entities.extend(extracted_entities)
                except ValueError:
                    logger.warning(f"未知实体类型: {entity_type_str}")
                    continue
            
            # 去重处理
            entities = self._deduplicate_entities(entities)
            
            logger.info(f"从表 {table_name} 抽取到 {len(entities)} 个实体")
            return entities
            
        except Exception as e:
            logger.error(f"从DataFrame抽取实体失败: {str(e)}")
            return []
    
    def _extract_entities_by_type(self, df: pd.DataFrame, 
                                entity_type: EntityType,
                                field_names: List[str],
                                table_name: str) -> List[Entity]:
        """
        按类型抽取实体
        
        Args:
            df: 数据框
            entity_type: 实体类型
            field_names: 字段名列表
            table_name: 表名
            
        Returns:
            List[Entity]: 实体列表
        """
        entities = []
        
        if not field_names:
            return entities
        
        primary_field = field_names[0]  # 主要字段
        
        for index, row in df.iterrows():
            try:
                # 获取主要字段的值
                primary_value = row.get(primary_field)
                if pd.isna(primary_value) or str(primary_value).strip() == '':
                    continue
                
                primary_value = str(primary_value).strip()
                
                # 验证实体值的有效性
                if not self._is_valid_entity_value(primary_value, entity_type):
                    continue
                
                # 创建实体
                entity = Entity(
                    type=entity_type,
                    label=primary_value,
                    source_table=table_name,
                    source_column=primary_field,
                    source_record_id=str(index),
                    confidence=self._calculate_entity_confidence(primary_value, entity_type)
                )
                
                # 添加属性
                for field_name in field_names:
                    field_value = row.get(field_name)
                    if not pd.isna(field_value) and str(field_value).strip() != '':
                        entity.add_property(field_name, str(field_value).strip())
                
                # 添加其他相关属性
                self._add_contextual_properties(entity, row, df.columns)
                
                entities.append(entity)
                
            except Exception as e:
                logger.warning(f"抽取实体失败，行 {index}: {str(e)}")
                continue
        
        return entities
    
    def _is_valid_entity_value(self, value: str, entity_type: EntityType) -> bool:
        """
        验证实体值是否有效
        
        Args:
            value: 实体值
            entity_type: 实体类型
            
        Returns:
            bool: 是否有效
        """
        if not value or len(value.strip()) == 0:
            return False
        
        value = value.strip()
        
        # 基本长度检查
        if len(value) < 2:
            return False
        
        # 根据实体类型进行特定验证
        if entity_type == EntityType.ORGANIZATION:
            return self._is_valid_organization(value)
        elif entity_type == EntityType.PERSON:
            return self._is_valid_person_name(value)
        elif entity_type == EntityType.LOCATION:
            return self._is_valid_location(value)
        elif entity_type == EntityType.IDENTIFIER:
            return self._is_valid_identifier(value)
        
        return True
    
    def _is_valid_organization(self, value: str) -> bool:
        """验证组织名称"""
        # 排除明显无效的值
        invalid_patterns = [
            r'^\d+$',           # 纯数字
            r'^[^a-zA-Z\u4e00-\u9fff]+$',  # 不包含字母和中文
            r'^(无|空|NULL|null|None|-)$'   # 明显的空值标识
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, value):
                return False
        
        # 检查组织关键词
        org_keywords = self.entity_patterns[EntityType.ORGANIZATION]['keywords']
        return any(keyword in value for keyword in org_keywords) or len(value) >= 3
    
    def _is_valid_person_name(self, value: str) -> bool:
        """验证人员姓名"""
        # 中文姓名模式
        if re.match(r'^[\u4e00-\u9fff]{2,4}$', value):
            return True
        
        # 英文姓名模式
        if re.match(r'^[A-Za-z\s]{2,50}$', value) and ' ' in value:
            return True
        
        # 包含人员关键词
        person_keywords = self.entity_patterns[EntityType.PERSON]['keywords']
        return any(keyword in value for keyword in person_keywords)
    
    def _is_valid_location(self, value: str) -> bool:
        """验证地址位置"""
        # 地址关键词
        location_keywords = self.entity_patterns[EntityType.LOCATION]['keywords']
        if any(keyword in value for keyword in location_keywords):
            return True
        
        # 地址格式模式
        location_pattern = self.entity_patterns[EntityType.LOCATION]['regex']
        return bool(re.search(location_pattern, value))
    
    def _is_valid_identifier(self, value: str) -> bool:
        """验证标识符"""
        patterns = self.entity_patterns[EntityType.IDENTIFIER]
        
        # 检查各种标识符模式
        for pattern_name, pattern in patterns.items():
            if pattern_name != 'fields' and re.match(pattern, value):
                return True
        
        return False
    
    def _calculate_entity_confidence(self, value: str, entity_type: EntityType) -> float:
        """
        计算实体置信度
        
        Args:
            value: 实体值
            entity_type: 实体类型
            
        Returns:
            float: 置信度 (0-1)
        """
        confidence = 0.5  # 基础置信度
        
        if entity_type == EntityType.ORGANIZATION:
            # 检查组织后缀
            org_suffixes = self.entity_patterns[EntityType.ORGANIZATION]['suffixes']
            if any(value.endswith(suffix) for suffix in org_suffixes):
                confidence += 0.3
            
            # 检查组织关键词
            org_keywords = self.entity_patterns[EntityType.ORGANIZATION]['keywords']
            keyword_count = sum(1 for keyword in org_keywords if keyword in value)
            confidence += min(keyword_count * 0.1, 0.3)
        
        elif entity_type == EntityType.PERSON:
            # 中文姓名模式
            if re.match(r'^[\u4e00-\u9fff]{2,4}$', value):
                confidence += 0.4
            # 英文姓名模式
            elif re.match(r'^[A-Za-z\s]{2,50}$', value) and ' ' in value:
                confidence += 0.3
        
        elif entity_type == EntityType.LOCATION:
            # 地址关键词数量
            location_keywords = self.entity_patterns[EntityType.LOCATION]['keywords']
            keyword_count = sum(1 for keyword in location_keywords if keyword in value)
            confidence += min(keyword_count * 0.15, 0.4)
        
        elif entity_type == EntityType.IDENTIFIER:
            # 精确模式匹配
            patterns = self.entity_patterns[EntityType.IDENTIFIER]
            for pattern_name, pattern in patterns.items():
                if pattern_name != 'fields' and re.match(pattern, value):
                    confidence = 0.95  # 标识符匹配高置信度
                    break
        
        return min(confidence, 1.0)
    
    def _add_contextual_properties(self, entity: Entity, row: pd.Series, 
                                 all_columns: List[str]) -> None:
        """
        添加上下文属性
        
        Args:
            entity: 实体对象
            row: 数据行
            all_columns: 所有列名
        """
        # 添加与实体相关的其他字段作为属性
        related_fields = self._find_related_fields(entity.type, all_columns)
        
        for field in related_fields:
            if field in row.index:
                field_value = row[field]
                if not pd.isna(field_value) and str(field_value).strip() != '':
                    property_key = self._normalize_property_key(field)
                    entity.add_property(property_key, str(field_value).strip())
    
    def _find_related_fields(self, entity_type: EntityType, 
                           columns: List[str]) -> List[str]:
        """
        查找与实体类型相关的字段
        
        Args:
            entity_type: 实体类型
            columns: 列名列表
            
        Returns:
            List[str]: 相关字段列表
        """
        related_fields = []
        
        # 根据实体类型定义相关字段模式
        if entity_type == EntityType.ORGANIZATION:
            patterns = ['地址', '电话', '法人', '代表', '联系', '注册', '成立', '规模']
        elif entity_type == EntityType.PERSON:
            patterns = ['电话', '邮箱', '职位', '部门', '年龄', '性别']
        elif entity_type == EntityType.LOCATION:
            patterns = ['邮编', '区域', '经度', '纬度', '行政区']
        else:
            patterns = []
        
        for col in columns:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in patterns):
                related_fields.append(col)
        
        return related_fields
    
    def _normalize_property_key(self, field_name: str) -> str:
        """
        标准化属性键名
        
        Args:
            field_name: 原始字段名
            
        Returns:
            str: 标准化的属性键
        """
        # 移除特殊字符，转换为小写
        normalized = re.sub(r'[^\w\u4e00-\u9fff]', '_', field_name)
        return normalized.lower().strip('_')
    
    def _deduplicate_entities(self, entities: List[Entity]) -> List[Entity]:
        """
        实体去重
        
        Args:
            entities: 实体列表
            
        Returns:
            List[Entity]: 去重后的实体列表
        """
        seen_entities = {}
        deduplicated = []
        
        for entity in entities:
            # 创建去重键
            dedup_key = f"{entity.type.value}:{entity.label.lower()}"
            
            if dedup_key in seen_entities:
                # 合并实体属性
                existing_entity = seen_entities[dedup_key]
                for key, value in entity.properties.items():
                    if key not in existing_entity.properties:
                        existing_entity.add_property(key, value)
                
                # 合并别名
                for alias in entity.aliases:
                    existing_entity.add_alias(alias)
                
                # 使用更高的置信度
                if entity.confidence > existing_entity.confidence:
                    existing_entity.confidence = entity.confidence
                    
            else:
                seen_entities[dedup_key] = entity
                deduplicated.append(entity)
        
        logger.info(f"实体去重: {len(entities)} -> {len(deduplicated)}")
        return deduplicated
    
    def extract_entities_from_text(self, text: str, 
                                 context: Dict[str, Any] = None) -> List[Entity]:
        """
        从自由文本中抽取实体（预留接口）
        
        Args:
            text: 文本内容
            context: 上下文信息
            
        Returns:
            List[Entity]: 抽取的实体列表
        """
        # TODO: 实现基于NLP的文本实体抽取
        # 可以集成spaCy、HanLP等NLP工具
        logger.info("文本实体抽取功能待实现")
        return []