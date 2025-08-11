"""
实体抽取器
从结构化数据中识别和抽取实体
"""

import re
import logging
from typing import Dict, List, Any, Optional, Set, Tuple
import pandas as pd
import math
from .kg_models import Entity, EntityType
from ..data_manager.schema_detector import SchemaDetector

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
    logger_init.info("jieba中文分词库加载成功")
except ImportError:
    HAS_JIEBA = False
    logger_init = logging.getLogger(__name__)
    logger_init.warning("jieba库未安装，将使用基础实体识别")

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    HAS_SKLEARN = True
    logger_init.info("scikit-learn机器学习库加载成功")
except ImportError:
    HAS_SKLEARN = False
    logger_init.warning("scikit-learn库未安装，将跳过向量相似度计算")

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
        
        # 初始化NLP组件
        self._init_nlp_components()
        
        # 实体同义词词典
        self.synonym_dict = self._build_synonym_dict()
        
        # 命名实体识别规则
        self.ner_rules = self._build_ner_rules()
        
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
        
    def _init_nlp_components(self) -> None:
        """初始化NLP组件"""
        if HAS_JIEBA:
            # 加载自定义词典（如果有的话）
            custom_dict_path = self.config.get('custom_dict_path')
            if custom_dict_path:
                try:
                    jieba.load_userdict(custom_dict_path)
                    logger.info(f"加载自定义词典: {custom_dict_path}")
                except Exception as e:
                    logger.warning(f"加载自定义词典失败: {e}")
            
            # 设置分词模式 - Windows系统不支持并行模式
            try:
                import os
                if os.name == 'posix':  # Unix/Linux系统
                    jieba.enable_parallel(4)  # 启用并行分词
                    logger.info("jieba分词器初始化完成（并行模式）")
                else:  # Windows系统
                    logger.info("jieba分词器初始化完成（串行模式）")
            except Exception as e:
                logger.warning(f"jieba并行模式设置失败: {e}，使用串行模式")
        
        if HAS_SKLEARN:
            # 初始化TF-IDF向量化器
            self.tfidf_vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words=None,  # 中文没有内置停用词
                ngram_range=(1, 2),
                analyzer='char'  # 字符级分析适合中文
            )
            logger.info("TF-IDF向量化器初始化完成")
    
    def _build_synonym_dict(self) -> Dict[str, List[str]]:
        """构建实体同义词词典"""
        synonym_dict = {
            # 组织机构同义词
            '公司': ['企业', '公司', '集团', '机构', '单位'],
            '学校': ['学院', '大学', '中学', '小学', '学校', '教育机构'],
            '医院': ['医疗机构', '诊所', '卫生院', '医院', '卫生所'],
            '政府': ['机关', '部门', '委员会', '政府', '行政机构'],
            
            # 人员同义词
            '经理': ['管理者', '负责人', '主管', '经理', '领导'],
            '法人': ['法定代表人', '法人代表', '法人', '代表人'],
            '联系人': ['负责人', '接洽人', '联系人', '对接人'],
            
            # 地址同义词
            '地址': ['位置', '所在地', '地址', '住址', '场所'],
            '注册地': ['注册地址', '登记地址', '注册地', '登记地'],
        }
        
        # 从配置文件加载额外同义词
        config_synonyms = self.config.get('synonyms', {})
        synonym_dict.update(config_synonyms)
        
        logger.info(f"构建同义词词典完成，包含 {len(synonym_dict)} 个词组")
        return synonym_dict
    
    def _build_ner_rules(self) -> Dict[str, List[Dict]]:
        """构建命名实体识别规则"""
        ner_rules = {
            EntityType.ORGANIZATION.value: [
                {
                    'pattern': r'.*?(公司|企业|集团|有限|股份|合作社|事务所|中心|机构).*?',
                    'pos_tags': ['n', 'nz', 'nt'],  # 名词、专有名词、机构团体
                    'min_length': 3,
                    'weight': 0.8
                },
                {
                    'pattern': r'.*?(学校|学院|大学|中学|小学|教育).*?',
                    'pos_tags': ['n', 'nz', 'nt'],
                    'min_length': 4,
                    'weight': 0.9
                }
            ],
            EntityType.PERSON.value: [
                {
                    'pattern': r'^[\u4e00-\u9fff]{2,4}$',  # 中文姓名
                    'pos_tags': ['nr'],  # 人名
                    'min_length': 2,
                    'weight': 0.9
                },
                {
                    'pattern': r'^[A-Za-z\s]{2,50}$',  # 英文姓名
                    'pos_tags': ['nr', 'nrf'],
                    'min_length': 2,
                    'weight': 0.8
                }
            ],
            EntityType.LOCATION.value: [
                {
                    'pattern': r'.*?(省|市|区|县|街道|路|号|楼|层).*?',
                    'pos_tags': ['ns'],  # 地名
                    'min_length': 3,
                    'weight': 0.8
                }
            ]
        }
        
        logger.info("命名实体识别规则构建完成")
        return ner_rules
        
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
        验证实体值是否有效（增强版，集成NLP）
        
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
        
        # 使用增强的NLP验证
        if HAS_JIEBA:
            nlp_score = self._calculate_nlp_entity_score(value, entity_type)
            if nlp_score > 0.6:  # NLP验证通过
                return True
        
        # 回退到基础验证
        if entity_type == EntityType.ORGANIZATION:
            return self._is_valid_organization(value)
        elif entity_type == EntityType.PERSON:
            return self._is_valid_person_name(value)
        elif entity_type == EntityType.LOCATION:
            return self._is_valid_location(value)
        elif entity_type == EntityType.IDENTIFIER:
            return self._is_valid_identifier(value)
        
        return True
    
    def _calculate_nlp_entity_score(self, value: str, entity_type: EntityType) -> float:
        """
        基于NLP计算实体识别分数
        
        Args:
            value: 实体值
            entity_type: 实体类型
            
        Returns:
            float: NLP分数 (0-1)
        """
        if not HAS_JIEBA:
            return 0.0
        
        score = 0.0
        
        try:
            # 1. 词性标注分析
            pos_score = self._calculate_pos_score(value, entity_type)
            score += pos_score * 0.4
            
            # 2. 规则模式匹配
            pattern_score = self._calculate_pattern_score(value, entity_type)
            score += pattern_score * 0.3
            
            # 3. 同义词匹配
            synonym_score = self._calculate_synonym_score(value, entity_type)
            score += synonym_score * 0.2
            
            # 4. 语义向量相似度（如果可用）
            if HAS_SKLEARN:
                vector_score = self._calculate_vector_similarity_score(value, entity_type)
                score += vector_score * 0.1
            
        except Exception as e:
            logger.warning(f"NLP实体分数计算失败: {e}")
            return 0.0
        
        return min(score, 1.0)
    
    def _calculate_pos_score(self, value: str, entity_type: EntityType) -> float:
        """计算词性标注分数"""
        try:
            # 使用jieba进行词性标注
            seg_result = list(pseg.cut(value))
            
            if not seg_result:
                return 0.0
            
            # 获取实体类型对应的词性标签
            target_pos_tags = set()
            entity_type_str = entity_type.value
            
            if entity_type_str in self.ner_rules:
                for rule in self.ner_rules[entity_type_str]:
                    target_pos_tags.update(rule.get('pos_tags', []))
            
            # 计算匹配的词性标签比例
            matched_pos = 0
            total_words = len(seg_result)
            
            for word, pos in seg_result:
                if pos in target_pos_tags:
                    matched_pos += 1
            
            return matched_pos / total_words if total_words > 0 else 0.0
            
        except Exception as e:
            logger.debug(f"词性分析失败: {e}")
            return 0.0
    
    def _calculate_pattern_score(self, value: str, entity_type: EntityType) -> float:
        """计算规则模式匹配分数"""
        entity_type_str = entity_type.value
        
        if entity_type_str not in self.ner_rules:
            return 0.0
        
        max_score = 0.0
        
        for rule in self.ner_rules[entity_type_str]:
            pattern = rule.get('pattern', '')
            min_length = rule.get('min_length', 2)
            weight = rule.get('weight', 0.5)
            
            # 长度检查
            if len(value) < min_length:
                continue
            
            # 模式匹配
            if pattern and re.match(pattern, value):
                max_score = max(max_score, weight)
        
        return max_score
    
    def _calculate_synonym_score(self, value: str, entity_type: EntityType) -> float:
        """计算同义词匹配分数"""
        # 检查值中是否包含相关的同义词
        value_lower = value.lower()
        
        # 根据实体类型获取相关同义词
        relevant_synonyms = []
        if entity_type == EntityType.ORGANIZATION:
            for key in ['公司', '学校', '医院', '政府']:
                if key in self.synonym_dict:
                    relevant_synonyms.extend(self.synonym_dict[key])
        elif entity_type == EntityType.PERSON:
            for key in ['经理', '法人', '联系人']:
                if key in self.synonym_dict:
                    relevant_synonyms.extend(self.synonym_dict[key])
        elif entity_type == EntityType.LOCATION:
            for key in ['地址', '注册地']:
                if key in self.synonym_dict:
                    relevant_synonyms.extend(self.synonym_dict[key])
        
        # 计算匹配度
        matched_count = 0
        for synonym in relevant_synonyms:
            if synonym.lower() in value_lower:
                matched_count += 1
        
        return min(matched_count / max(len(relevant_synonyms), 1), 1.0)
    
    def _calculate_vector_similarity_score(self, value: str, entity_type: EntityType) -> float:
        """计算语义向量相似度分数"""
        if not HAS_SKLEARN:
            return 0.0
        
        try:
            # 构建参考实体样本
            reference_entities = self._get_reference_entities(entity_type)
            
            if not reference_entities:
                return 0.0
            
            # 准备文本数据
            texts = reference_entities + [value]
            
            # 计算TF-IDF向量
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(texts)
            
            # 计算与参考实体的相似度
            target_vector = tfidf_matrix[-1:]  # 目标实体向量
            reference_vectors = tfidf_matrix[:-1]  # 参考实体向量
            
            # 计算余弦相似度
            similarities = cosine_similarity(target_vector, reference_vectors)
            
            # 返回最高相似度
            return float(np.max(similarities)) if similarities.size > 0 else 0.0
            
        except Exception as e:
            logger.debug(f"向量相似度计算失败: {e}")
            return 0.0
    
    def _get_reference_entities(self, entity_type: EntityType) -> List[str]:
        """获取参考实体样本"""
        # 基于实体类型返回一些典型样本
        if entity_type == EntityType.ORGANIZATION:
            return [
                '北京科技有限公司', '上海教育集团', '深圳医疗中心',
                '广州研究院', '天津制造企业', '重庆服务机构'
            ]
        elif entity_type == EntityType.PERSON:
            return [
                '张三', '李四', '王五', '赵六', '陈七', '刘八',
                '法定代表人', '项目经理', '技术负责人'
            ]
        elif entity_type == EntityType.LOCATION:
            return [
                '北京市朝阳区', '上海市浦东新区', '深圳市南山区',
                '广州市天河区', '杭州市西湖区', '成都市高新区'
            ]
        else:
            return []
    
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
        计算实体置信度（增强版，集成NLP）
        
        Args:
            value: 实体值
            entity_type: 实体类型
            
        Returns:
            float: 置信度 (0-1)
        """
        # 基础置信度
        base_confidence = 0.3
        
        # 1. NLP增强置信度计算
        nlp_confidence = 0.0
        if HAS_JIEBA:
            nlp_score = self._calculate_nlp_entity_score(value, entity_type)
            nlp_confidence = nlp_score * 0.5  # NLP贡献50%权重
        
        # 2. 传统规则置信度
        rule_confidence = self._calculate_rule_based_confidence(value, entity_type)
        
        # 3. 综合置信度计算
        final_confidence = base_confidence + nlp_confidence + rule_confidence * 0.5
        
        # 4. 特殊情况调整
        final_confidence = self._adjust_confidence_by_context(value, entity_type, final_confidence)
        
        return min(final_confidence, 1.0)
    
    def _calculate_rule_based_confidence(self, value: str, entity_type: EntityType) -> float:
        """基于传统规则计算置信度"""
        confidence = 0.0
        
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
        
        return confidence
    
    def _adjust_confidence_by_context(self, value: str, entity_type: EntityType, base_confidence: float) -> float:
        """根据上下文调整置信度"""
        adjusted_confidence = base_confidence
        
        # 长度调整
        if len(value) < 3:
            adjusted_confidence *= 0.8  # 过短的实体降低置信度
        elif len(value) > 50:
            adjusted_confidence *= 0.9  # 过长的实体稍微降低置信度
        
        # 字符组成调整
        if entity_type in [EntityType.ORGANIZATION, EntityType.PERSON, EntityType.LOCATION]:
            # 包含数字过多的非标识符实体
            digit_ratio = sum(1 for c in value if c.isdigit()) / len(value)
            if digit_ratio > 0.5:
                adjusted_confidence *= 0.7
            
            # 包含特殊字符过多
            special_char_ratio = sum(1 for c in value if not c.isalnum() and not c.isspace()) / len(value)
            if special_char_ratio > 0.3:
                adjusted_confidence *= 0.8
        
        # 重复字符检查
        if len(set(value)) / len(value) < 0.3:  # 字符重复度过高
            adjusted_confidence *= 0.6
        
        return adjusted_confidence
    
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
        从自由文本中抽取实体（基于NLP实现）
        
        Args:
            text: 文本内容
            context: 上下文信息
            
        Returns:
            List[Entity]: 抽取的实体列表
        """
        if not text or not text.strip():
            return []
        
        if not HAS_JIEBA:
            logger.warning("jieba库未安装，无法进行文本实体抽取")
            return []
        
        try:
            entities = []
            context = context or {}
            
            # 1. 使用jieba进行词性标注
            seg_result = list(pseg.cut(text))
            
            # 2. 基于词性和规则抽取实体
            for word, pos in seg_result:
                word = word.strip()
                if len(word) < 2:
                    continue
                
                # 根据词性判断实体类型
                entity_type = self._infer_entity_type_from_pos(word, pos)
                
                if entity_type and self._is_valid_entity_value(word, entity_type):
                    # 计算置信度
                    confidence = self._calculate_entity_confidence(word, entity_type)
                    
                    # 创建实体
                    entity = Entity(
                        type=entity_type,
                        label=word,
                        source_table=context.get('source_table', 'text_extraction'),
                        source_column=context.get('source_column', 'text_content'),
                        source_record_id=context.get('record_id', 'text_0'),
                        confidence=confidence
                    )
                    
                    # 添加上下文属性
                    entity.add_property('pos_tag', pos)
                    entity.add_property('extraction_method', 'nlp_text_extraction')
                    
                    entities.append(entity)
            
            # 3. 去重处理
            entities = self._deduplicate_entities(entities)
            
            # 4. 基于上下文的实体关系推断（简单版）
            entities = self._enhance_entities_with_context(entities, text, context)
            
            logger.info(f"从文本中抽取到 {len(entities)} 个实体")
            return entities
            
        except Exception as e:
            logger.error(f"文本实体抽取失败: {str(e)}")
            return []
    
    def _infer_entity_type_from_pos(self, word: str, pos: str) -> Optional[EntityType]:
        """根据词性推断实体类型"""
        # jieba词性标注映射到实体类型
        pos_mapping = {
            'nr': EntityType.PERSON,      # 人名
            'nrf': EntityType.PERSON,     # 人名（姓氏）
            'nrg': EntityType.PERSON,     # 人名（名）
            'nt': EntityType.ORGANIZATION, # 机构团体
            'nz': EntityType.ORGANIZATION, # 专有名词（多为机构）
            'ns': EntityType.LOCATION,    # 地名
            'n': None,  # 一般名词，需要进一步判断
        }
        
        # 直接映射
        if pos in pos_mapping and pos_mapping[pos] is not None:
            return pos_mapping[pos]
        
        # 对于一般名词，基于内容判断
        if pos == 'n':
            return self._infer_entity_type_from_content(word)
        
        return None
    
    def _infer_entity_type_from_content(self, word: str) -> Optional[EntityType]:
        """基于内容推断实体类型"""
        # 检查组织关键词
        org_keywords = self.entity_patterns[EntityType.ORGANIZATION]['keywords']
        if any(keyword in word for keyword in org_keywords):
            return EntityType.ORGANIZATION
        
        # 检查地址关键词
        location_keywords = self.entity_patterns[EntityType.LOCATION]['keywords']
        if any(keyword in word for keyword in location_keywords):
            return EntityType.LOCATION
        
        # 检查标识符模式
        if self._is_valid_identifier(word):
            return EntityType.IDENTIFIER
        
        return None
    
    def _enhance_entities_with_context(self, entities: List[Entity], 
                                     text: str, context: Dict[str, Any]) -> List[Entity]:
        """基于上下文增强实体信息"""
        try:
            # 添加文本上下文信息
            for entity in entities:
                # 查找实体在文本中的位置
                start_pos = text.find(entity.label)
                if start_pos != -1:
                    # 添加前后文本作为上下文
                    context_window = 20
                    before_text = text[max(0, start_pos - context_window):start_pos]
                    after_text = text[start_pos + len(entity.label):start_pos + len(entity.label) + context_window]
                    
                    entity.add_property('context_before', before_text.strip())
                    entity.add_property('context_after', after_text.strip())
                    entity.add_property('text_position', str(start_pos))
                
                # 添加其他上下文信息
                for key, value in context.items():
                    if key not in ['source_table', 'source_column', 'record_id']:
                        entity.add_property(f'context_{key}', str(value))
            
            return entities
            
        except Exception as e:
            logger.warning(f"实体上下文增强失败: {e}")
            return entities
    
    def get_entity_extraction_statistics(self) -> Dict[str, Any]:
        """获取实体抽取统计信息"""
        return {
            'nlp_available': HAS_JIEBA,
            'sklearn_available': HAS_SKLEARN,
            'supported_entity_types': [et.value for et in EntityType],
            'confidence_thresholds': self.confidence_thresholds,
            'synonym_dict_size': len(self.synonym_dict),
            'ner_rules_count': sum(len(rules) for rules in self.ner_rules.values()),
            'extraction_methods': [
                'dataframe_extraction',
                'text_extraction' if HAS_JIEBA else 'text_extraction_disabled',
                'nlp_enhanced_validation',
                'vector_similarity' if HAS_SKLEARN else 'vector_similarity_disabled'
            ]
        }