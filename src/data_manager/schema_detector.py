"""
模式检测器
自动检测和识别数据模式，为知识图谱构建提供基础
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict
import numpy as np

logger = logging.getLogger(__name__)

class SchemaDetector:
    """数据模式检测器"""
    
    def __init__(self):
        """初始化模式检测器"""
        # 实体类型模式
        self.entity_patterns = {
            'ORGANIZATION': {
                'keywords': ['公司', '企业', '集团', '有限', '股份', '合作社', '事务所', '中心', '机构'],
                'suffixes': ['有限公司', '股份公司', '集团', 'Co.Ltd', 'Inc.', 'Corp.', 'LLC'],
                'regex': r'.*(公司|企业|集团|有限|股份|合作社|事务所|中心|机构).*'
            },
            'PERSON': {
                'keywords': ['法人', '代表', '负责人', '经理', '主管', '董事', '总裁'],
                'name_patterns': [r'^[\u4e00-\u9fff]{2,4}$', r'^[A-Za-z\s]{2,50}$'],
                'fields': ['法人', '代表', '负责人', '联系人', '经理']
            },
            'LOCATION': {
                'keywords': ['省', '市', '区', '县', '街道', '路', '号', '楼', '层'],
                'regex': r'.*(省|市|区|县|街道|路|号|楼|层).*',
                'fields': ['地址', '位置', '所在地', '注册地']
            },
            'IDENTIFIER': {
                'credit_code': r'^[0-9A-HJ-NPQRTUWXY]{2}\d{6}[0-9A-HJ-NPQRTUWXY]{10}$',
                'id_card': r'^\d{15}$|^\d{17}[\dX]$',
                'phone': r'^(\+86)?1[3-9]\d{9}$|^\d{3,4}-\d{7,8}$',
                'fields': ['信用代码', '身份证', '电话', '手机', '联系方式']
            }
        }
        
        # 关系模式
        self.relation_patterns = {
            'LOCATED_IN': ['位于', '在', '地址', '所在'],
            'OWNED_BY': ['拥有', '属于', '隶属'],
            'MANAGED_BY': ['管理', '负责', '主管'],
            'CONTACT_OF': ['联系', '电话', '手机'],
            'REPRESENTS': ['代表', '法人', '代理']
        }
    
    def detect_schema(self, df: pd.DataFrame, table_name: str = None) -> Dict[str, Any]:
        """
        检测数据表的模式
        
        Args:
            df: 要检测的DataFrame
            table_name: 表名
            
        Returns:
            Dict: 检测到的模式信息
        """
        try:
            schema = {
                'table_name': table_name or 'unknown_table',
                'entity_fields': self._detect_entity_fields(df),
                'relation_fields': self._detect_relation_fields(df),
                'key_fields': self._detect_key_fields(df),
                'semantic_types': self._detect_semantic_types(df),
                'field_relationships': self._detect_field_relationships(df),
                'kg_mapping_suggestions': self._suggest_kg_mapping(df),
                'data_patterns': self._detect_data_patterns(df)
            }
            
            logger.info(f"完成模式检测: {table_name}")
            return schema
            
        except Exception as e:
            logger.error(f"模式检测失败: {str(e)}")
            return {'error': str(e)}
    
    def _detect_entity_fields(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """检测实体字段"""
        entity_fields = defaultdict(list)
        
        for col in df.columns:
            col_lower = col.lower()
            col_data = df[col].dropna()
            
            if len(col_data) == 0:
                continue
            
            # 检测组织实体
            if self._is_organization_field(col, col_data):
                entity_fields['ORGANIZATION'].append(col)
            
            # 检测人员实体
            elif self._is_person_field(col, col_data):
                entity_fields['PERSON'].append(col)
            
            # 检测地址实体
            elif self._is_location_field(col, col_data):
                entity_fields['LOCATION'].append(col)
            
            # 检测标识符实体
            elif self._is_identifier_field(col, col_data):
                entity_fields['IDENTIFIER'].append(col)
        
        return dict(entity_fields)
    
    def _is_organization_field(self, col_name: str, col_data: pd.Series) -> bool:
        """判断是否为组织字段"""
        col_lower = col_name.lower()
        
        # 基于列名判断
        org_keywords = self.entity_patterns['ORGANIZATION']['keywords']
        if any(keyword in col_lower for keyword in ['公司', '企业', '单位', '机构', 'company', 'organization']):
            return True
        
        # 基于内容判断
        sample_values = col_data.astype(str).head(20)
        org_count = 0
        
        for value in sample_values:
            if any(keyword in value for keyword in org_keywords):
                org_count += 1
            # 检查后缀
            if any(value.endswith(suffix) for suffix in self.entity_patterns['ORGANIZATION']['suffixes']):
                org_count += 1
        
        return org_count / len(sample_values) > 0.3
    
    def _is_person_field(self, col_name: str, col_data: pd.Series) -> bool:
        """判断是否为人员字段"""
        col_lower = col_name.lower()
        
        # 基于列名判断
        person_keywords = self.entity_patterns['PERSON']['keywords']
        if any(keyword in col_lower for keyword in person_keywords):
            return True
        
        # 基于内容判断（中文姓名模式）
        sample_values = col_data.astype(str).head(20)
        name_count = 0
        
        for value in sample_values:
            # 检查中文姓名模式
            if re.match(r'^[\u4e00-\u9fff]{2,4}$', value):
                name_count += 1
            # 检查英文姓名模式
            elif re.match(r'^[A-Za-z\s]{2,50}$', value) and ' ' in value:
                name_count += 1
        
        return name_count / len(sample_values) > 0.5
    
    def _is_location_field(self, col_name: str, col_data: pd.Series) -> bool:
        """判断是否为地址字段"""
        col_lower = col_name.lower()
        
        # 基于列名判断
        if any(keyword in col_lower for keyword in ['地址', '位置', 'address', 'location']):
            return True
        
        # 基于内容判断
        sample_values = col_data.astype(str).head(20)
        location_count = 0
        
        for value in sample_values:
            if re.search(self.entity_patterns['LOCATION']['regex'], value):
                location_count += 1
        
        return location_count / len(sample_values) > 0.3
    
    def _is_identifier_field(self, col_name: str, col_data: pd.Series) -> bool:
        """判断是否为标识符字段"""
        col_lower = col_name.lower()
        
        # 基于列名判断
        id_keywords = ['id', '编号', '代码', '号码', '证件']
        if any(keyword in col_lower for keyword in id_keywords):
            return True
        
        # 基于内容模式判断
        sample_values = col_data.astype(str).head(50)
        
        # 检查统一社会信用代码
        credit_count = sum(1 for val in sample_values if re.match(self.entity_patterns['IDENTIFIER']['credit_code'], val))
        if credit_count / len(sample_values) > 0.8:
            return True
        
        # 检查身份证号
        id_count = sum(1 for val in sample_values if re.match(self.entity_patterns['IDENTIFIER']['id_card'], val))
        if id_count / len(sample_values) > 0.8:
            return True
        
        # 检查电话号码
        phone_count = sum(1 for val in sample_values if re.match(self.entity_patterns['IDENTIFIER']['phone'], val))
        if phone_count / len(sample_values) > 0.8:
            return True
        
        return False
    
    def _detect_relation_fields(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """检测关系字段"""
        relation_fields = defaultdict(list)
        
        for col in df.columns:
            col_lower = col.lower()
            
            for relation_type, keywords in self.relation_patterns.items():
                if any(keyword in col_lower for keyword in keywords):
                    relation_fields[relation_type].append(col)
        
        return dict(relation_fields)
    
    def _detect_key_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检测主键和外键字段"""
        key_fields = {
            'primary_key_candidates': [],
            'foreign_key_candidates': [],
            'unique_identifiers': []
        }
        
        for col in df.columns:
            col_data = df[col]
            unique_ratio = col_data.nunique() / len(col_data)
            null_count = col_data.isnull().sum()
            
            # 主键候选（唯一且无空值）
            if unique_ratio == 1.0 and null_count == 0:
                key_fields['primary_key_candidates'].append({
                    'column': col,
                    'confidence': 'high'
                })
            elif unique_ratio > 0.95 and null_count < len(col_data) * 0.1:
                key_fields['primary_key_candidates'].append({
                    'column': col,
                    'confidence': 'medium'
                })
            
            # 外键候选（命名模式）
            col_lower = col.lower()
            if (col_lower.endswith('_id') or col_lower.endswith('id') or 
                '编号' in col_lower or '代码' in col_lower):
                key_fields['foreign_key_candidates'].append(col)
            
            # 唯一标识符
            if unique_ratio > 0.9:
                key_fields['unique_identifiers'].append({
                    'column': col,
                    'unique_ratio': unique_ratio
                })
        
        return key_fields
    
    def _detect_semantic_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """检测语义类型"""
        semantic_types = {}
        
        for col in df.columns:
            col_data = df[col].dropna()
            if len(col_data) == 0:
                continue
            
            semantic_type = self._infer_semantic_type(col, col_data)
            semantic_types[col] = semantic_type
        
        return semantic_types
    
    def _infer_semantic_type(self, col_name: str, col_data: pd.Series) -> str:
        """推断语义类型"""
        col_lower = col_name.lower()
        sample_values = col_data.astype(str).head(50)
        
        # 时间类型
        if any(keyword in col_lower for keyword in ['时间', '日期', 'time', 'date']):
            return 'temporal'
        
        # 金额类型
        if any(keyword in col_lower for keyword in ['金额', '价格', '费用', 'money', 'price', 'cost']):
            return 'monetary'
        
        # 数量类型
        if any(keyword in col_lower for keyword in ['数量', '个数', '总数', 'count', 'number']):
            return 'quantity'
        
        # 百分比类型
        if any(keyword in col_lower for keyword in ['比例', '百分比', 'percent', 'ratio']):
            return 'percentage'
        
        # 状态类型
        if any(keyword in col_lower for keyword in ['状态', '类型', 'status', 'type', 'category']):
            return 'categorical'
        
        # 基于内容推断
        try:
            # 检查是否可以转换为数值
            pd.to_numeric(sample_values.head(10))
            return 'numeric'
        except:
            pass
        
        try:
            # 检查是否为日期
            pd.to_datetime(sample_values.head(10))
            return 'temporal'
        except:
            pass
        
        return 'textual'
    
    def _detect_field_relationships(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """检测字段间关系"""
        relationships = []
        
        # 检测层次关系（如：省-市-区）
        location_fields = []
        for col in df.columns:
            if self._is_location_field(col, df[col].dropna()):
                location_fields.append(col)
        
        if len(location_fields) > 1:
            relationships.append({
                'type': 'hierarchical',
                'fields': location_fields,
                'description': '地理位置层次关系'
            })
        
        # 检测依赖关系
        for col1 in df.columns:
            for col2 in df.columns:
                if col1 != col2:
                    dependency = self._check_functional_dependency(df[col1], df[col2])
                    if dependency > 0.9:
                        relationships.append({
                            'type': 'functional_dependency',
                            'source': col1,
                            'target': col2,
                            'strength': dependency
                        })
        
        return relationships
    
    def _check_functional_dependency(self, col1: pd.Series, col2: pd.Series) -> float:
        """检查函数依赖关系"""
        try:
            # 检查col1 -> col2的函数依赖
            grouped = pd.DataFrame({'col1': col1, 'col2': col2}).groupby('col1')['col2'].nunique()
            single_value_groups = (grouped == 1).sum()
            total_groups = len(grouped)
            
            if total_groups == 0:
                return 0.0
            
            return single_value_groups / total_groups
        except:
            return 0.0
    
    def _suggest_kg_mapping(self, df: pd.DataFrame) -> Dict[str, Any]:
        """建议知识图谱映射"""
        entity_fields = self._detect_entity_fields(df)
        key_fields = self._detect_key_fields(df)
        
        mapping_suggestions = {
            'entity_extraction_plan': {},
            'relation_extraction_plan': {},
            'property_mapping': {}
        }
        
        # 实体抽取计划
        for entity_type, fields in entity_fields.items():
            if fields:
                mapping_suggestions['entity_extraction_plan'][entity_type] = {
                    'primary_field': fields[0],  # 主要字段
                    'supporting_fields': fields[1:],  # 支持字段
                    'identifier_fields': [f for f in key_fields.get('unique_identifiers', []) 
                                         if isinstance(f, dict) and f['column'] in fields]
                }
        
        # 关系抽取计划
        org_fields = entity_fields.get('ORGANIZATION', [])
        person_fields = entity_fields.get('PERSON', [])
        location_fields = entity_fields.get('LOCATION', [])
        
        if org_fields and person_fields:
            mapping_suggestions['relation_extraction_plan']['MANAGED_BY'] = {
                'subject_fields': org_fields,
                'object_fields': person_fields,
                'relation_type': 'organization_person'
            }
        
        if org_fields and location_fields:
            mapping_suggestions['relation_extraction_plan']['LOCATED_IN'] = {
                'subject_fields': org_fields,
                'object_fields': location_fields,
                'relation_type': 'organization_location'
            }
        
        # 属性映射
        for col in df.columns:
            semantic_type = self._infer_semantic_type(col, df[col].dropna())
            if semantic_type != 'textual':
                mapping_suggestions['property_mapping'][col] = {
                    'semantic_type': semantic_type,
                    'kg_property_type': self._map_to_kg_property(semantic_type)
                }
        
        return mapping_suggestions
    
    def _map_to_kg_property(self, semantic_type: str) -> str:
        """将语义类型映射到知识图谱属性类型"""
        mapping = {
            'temporal': 'dateTime',
            'monetary': 'decimal',
            'quantity': 'integer',
            'percentage': 'decimal',
            'numeric': 'decimal',
            'categorical': 'string',
            'textual': 'string'
        }
        return mapping.get(semantic_type, 'string')
    
    def _detect_data_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检测数据模式"""
        patterns = {
            'naming_conventions': self._detect_naming_patterns(df),
            'value_patterns': self._detect_value_patterns(df),
            'structural_patterns': self._detect_structural_patterns(df)
        }
        
        return patterns
    
    def _detect_naming_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检测命名模式"""
        patterns = {
            'column_naming_style': 'mixed',
            'common_prefixes': [],
            'common_suffixes': []
        }
        
        columns = df.columns.tolist()
        
        # 检测命名风格
        snake_case_count = sum(1 for col in columns if '_' in col and col.lower() == col)
        camel_case_count = sum(1 for col in columns if any(c.isupper() for c in col[1:]))
        
        if snake_case_count > len(columns) * 0.7:
            patterns['column_naming_style'] = 'snake_case'
        elif camel_case_count > len(columns) * 0.7:
            patterns['column_naming_style'] = 'camelCase'
        
        # 检测常见前缀和后缀
        prefixes = defaultdict(int)
        suffixes = defaultdict(int)
        
        for col in columns:
            if '_' in col:
                parts = col.split('_')
                if len(parts) > 1:
                    prefixes[parts[0]] += 1
                    suffixes[parts[-1]] += 1
        
        patterns['common_prefixes'] = [prefix for prefix, count in prefixes.items() if count > 1]
        patterns['common_suffixes'] = [suffix for suffix, count in suffixes.items() if count > 1]
        
        return patterns
    
    def _detect_value_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检测值模式"""
        patterns = {}
        
        for col in df.columns:
            col_data = df[col].dropna().astype(str)
            if len(col_data) == 0:
                continue
            
            # 检测常见格式模式
            pattern_counts = defaultdict(int)
            
            for value in col_data.head(100):
                # 数字模式
                if re.match(r'^\d+$', value):
                    pattern_counts['pure_numeric'] += 1
                elif re.match(r'^\d+\.\d+$', value):
                    pattern_counts['decimal'] += 1
                # 日期模式
                elif re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                    pattern_counts['date_iso'] += 1
                elif re.match(r'^\d{4}/\d{1,2}/\d{1,2}$', value):
                    pattern_counts['date_slash'] += 1
                # 代码模式
                elif re.match(r'^[A-Z0-9]+$', value):
                    pattern_counts['uppercase_code'] += 1
                elif re.match(r'^[a-z0-9]+$', value):
                    pattern_counts['lowercase_code'] += 1
            
            if pattern_counts:
                dominant_pattern = max(pattern_counts.keys(), key=pattern_counts.get)
                if pattern_counts[dominant_pattern] > len(col_data.head(100)) * 0.5:
                    patterns[col] = dominant_pattern
        
        return patterns
    
    def _detect_structural_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检测结构模式"""
        return {
            'has_id_column': any('id' in col.lower() for col in df.columns),
            'has_timestamp': any('time' in col.lower() or 'date' in col.lower() for col in df.columns),
            'has_status_column': any('status' in col.lower() or '状态' in col for col in df.columns),
            'estimated_record_type': self._estimate_record_type(df)
        }
    
    def _estimate_record_type(self, df: pd.DataFrame) -> str:
        """估计记录类型"""
        columns = [col.lower() for col in df.columns]
        
        # 检查是否为企业记录
        org_indicators = ['公司', '企业', '单位', 'company', 'organization']
        if any(indicator in ' '.join(columns) for indicator in org_indicators):
            return 'organization_record'
        
        # 检查是否为人员记录
        person_indicators = ['姓名', '员工', 'name', 'employee', 'person']
        if any(indicator in ' '.join(columns) for indicator in person_indicators):
            return 'person_record'
        
        # 检查是否为交易记录
        transaction_indicators = ['金额', '交易', '订单', 'amount', 'transaction', 'order']
        if any(indicator in ' '.join(columns) for indicator in transaction_indicators):
            return 'transaction_record'
        
        return 'general_record'