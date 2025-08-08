"""
增强字段映射器
实现基于语义相似度的智能字段映射功能
"""

import numpy as np
import logging
from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
import json

# 动态导入jieba，如果失败则使用基础分词
try:
    import jieba
    JIEBA_AVAILABLE = True
except ImportError:
    JIEBA_AVAILABLE = False
    logging.warning("jieba未安装，将使用基础分词功能")

logger = logging.getLogger(__name__)

class EnhancedFieldMapper:
    """增强字段映射器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化增强字段映射器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 初始化中文分词（如果可用）
        if JIEBA_AVAILABLE:
            jieba.initialize()
        
        # 字段语义词典
        self.field_semantic_dict = {
            # 身份标识类
            'identity': ['编号', '代码', '标识', '序号', 'id', 'code', 'number', '号码', '证件'],
            
            # 名称类  
            'name': ['名称', '姓名', '单位', '公司', '企业', '机构', 'name', '称呼', '标题'],
            
            # 地址位置类
            'location': ['地址', '位置', '所在地', '注册地', '办公地', 'address', 'location', '省', '市', '区', '县'],
            
            # 联系方式类
            'contact': ['电话', '手机', '传真', '邮箱', '邮件', 'phone', 'email', 'tel', '联系方式'],
            
            # 时间日期类
            'datetime': ['时间', '日期', '年月日', 'date', 'time', '创建时间', '更新时间', '成立时间'],
            
            # 数值金额类
            'numeric': ['金额', '数量', '价格', '费用', '面积', '高度', '长度', 'amount', 'price', '收入'],
            
            # 状态类别类
            'category': ['类型', '状态', '级别', '等级', '分类', 'type', 'status', 'level', 'category'],
            
            # 描述说明类
            'description': ['描述', '说明', '备注', '详情', 'description', 'remark', 'note', '内容']
        }
        
        # 构建TF-IDF向量化器
        self.vectorizer = TfidfVectorizer(
            analyzer='char',  # 字符级分析，适合中文
            ngram_range=(1, 3),  # 1-3字符的n-gram
            max_features=5000
        )
        
        # 历史映射缓存
        self.mapping_history = defaultdict(list)
        self.load_mapping_history()
        
        logger.info("增强字段映射器初始化完成")
    
    def calculate_enhanced_similarity(self, field1: str, field2: str) -> Dict[str, float]:
        """
        计算增强的字段相似度
        
        Args:
            field1: 字段1名称
            field2: 字段2名称
            
        Returns:
            Dict: 包含各种相似度分数的字典
        """
        # 1. 字符串相似度
        char_similarity = self._calculate_character_similarity(field1, field2)
        
        # 2. 语义相似度
        semantic_similarity = self._calculate_semantic_similarity(field1, field2)
        
        # 3. 词向量相似度
        vector_similarity = self._calculate_vector_similarity(field1, field2)
        
        # 4. 历史映射相似度
        history_similarity = self._calculate_history_similarity(field1, field2)
        
        # 5. 综合相似度计算
        weights = {
            'character': 0.2,
            'semantic': 0.3,
            'vector': 0.3,
            'history': 0.2
        }
        
        overall_similarity = (
            char_similarity * weights['character'] +
            semantic_similarity * weights['semantic'] +
            vector_similarity * weights['vector'] +
            history_similarity * weights['history']
        )
        
        return {
            'character_similarity': round(char_similarity, 4),
            'semantic_similarity': round(semantic_similarity, 4),
            'vector_similarity': round(vector_similarity, 4),
            'history_similarity': round(history_similarity, 4),
            'overall_similarity': round(overall_similarity, 4),
            'confidence': self._calculate_confidence(overall_similarity, [char_similarity, semantic_similarity, vector_similarity])
        }
    
    def _calculate_character_similarity(self, field1: str, field2: str) -> float:
        """计算字符级相似度"""
        if not field1 or not field2:
            return 0.0
            
        field1 = field1.lower().strip()
        field2 = field2.lower().strip()
        
        if field1 == field2:
            return 1.0
        
        # Levenshtein距离
        def levenshtein_distance(s1, s2):
            if len(s1) < len(s2):
                return levenshtein_distance(s2, s1)
            if len(s2) == 0:
                return len(s1)
            
            previous_row = list(range(len(s2) + 1))
            for i, c1 in enumerate(s1):
                current_row = [i + 1]
                for j, c2 in enumerate(s2):
                    insertions = previous_row[j + 1] + 1
                    deletions = current_row[j] + 1
                    substitutions = previous_row[j] + (c1 != c2)
                    current_row.append(min(insertions, deletions, substitutions))
                previous_row = current_row
            return previous_row[-1]
        
        max_len = max(len(field1), len(field2))
        if max_len == 0:
            return 1.0
            
        distance = levenshtein_distance(field1, field2)
        similarity = 1 - (distance / max_len)
        
        return max(0.0, similarity)
    
    def _calculate_semantic_similarity(self, field1: str, field2: str) -> float:
        """计算语义相似度"""
        # 中文分词（优先使用jieba，否则使用简单分词）
        if JIEBA_AVAILABLE:
            words1 = list(jieba.cut(field1.lower()))
            words2 = list(jieba.cut(field2.lower()))
        else:
            # 基础分词：按常见分隔符分割
            words1 = re.split(r'[_\-\s]+', field1.lower())
            words2 = re.split(r'[_\-\s]+', field2.lower())
        
        # 移除停用词和标点
        stop_words = {'的', '和', '与', '或', '及', '等', '了', '是', '在', '有', '为', '以', '用', '从', '到'}
        words1 = [w for w in words1 if w not in stop_words and len(w.strip()) > 0]
        words2 = [w for w in words2 if w not in stop_words and len(w.strip()) > 0]
        
        if not words1 or not words2:
            return 0.0
        
        # 计算词汇重叠度
        set1, set2 = set(words1), set(words2)
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0.0
            
        jaccard_similarity = intersection / union
        
        # 语义类别匹配
        category_similarity = self._calculate_category_similarity(field1, field2)
        
        # 综合语义相似度
        semantic_score = (jaccard_similarity * 0.6) + (category_similarity * 0.4)
        
        return semantic_score
    
    def _calculate_category_similarity(self, field1: str, field2: str) -> float:
        """计算语义类别相似度"""
        def get_field_categories(field_name):
            categories = []
            field_lower = field_name.lower()
            
            for category, keywords in self.field_semantic_dict.items():
                for keyword in keywords:
                    if keyword in field_lower:
                        categories.append(category)
                        break
            
            return categories
        
        categories1 = get_field_categories(field1)
        categories2 = get_field_categories(field2)
        
        if not categories1 or not categories2:
            return 0.0
        
        # 计算类别重叠度
        overlap = len(set(categories1) & set(categories2))
        total = len(set(categories1) | set(categories2))
        
        return overlap / total if total > 0 else 0.0
    
    def _calculate_vector_similarity(self, field1: str, field2: str) -> float:
        """计算向量相似度"""
        try:
            # 构建文档集合
            documents = [field1, field2]
            
            # TF-IDF向量化
            tfidf_matrix = self.vectorizer.fit_transform(documents)
            
            # 计算余弦相似度
            similarity_matrix = cosine_similarity(tfidf_matrix)
            
            # 返回两个字段间的相似度
            return float(similarity_matrix[0, 1])
            
        except Exception as e:
            logger.warning(f"向量相似度计算失败: {str(e)}")
            return 0.0
    
    def _calculate_history_similarity(self, field1: str, field2: str) -> float:
        """基于历史映射计算相似度"""
        # 查找历史映射记录
        history_score = 0.0
        
        # 检查直接映射历史
        if field2 in self.mapping_history.get(field1, []):
            history_score += 0.8
        
        # 检查相似字段的映射历史
        for historical_source, historical_targets in self.mapping_history.items():
            if self._calculate_character_similarity(field1, historical_source) > 0.8:
                for target in historical_targets:
                    if self._calculate_character_similarity(field2, target) > 0.8:
                        history_score += 0.4
                        break
        
        return min(history_score, 1.0)
    
    def _calculate_confidence(self, overall_similarity: float, individual_scores: List[float]) -> str:
        """计算映射置信度"""
        if overall_similarity > 0.85:
            return 'very_high'
        elif overall_similarity > 0.7:
            return 'high'
        elif overall_similarity > 0.5:
            return 'medium'
        elif overall_similarity > 0.3:
            return 'low'
        else:
            return 'very_low'
    
    def generate_intelligent_mapping_suggestions(self, source_fields: List[Dict], 
                                               target_fields: List[Dict],
                                               max_suggestions: int = 3) -> List[Dict]:
        """
        生成智能映射建议
        
        Args:
            source_fields: 源字段列表
            target_fields: 目标字段列表  
            max_suggestions: 每个源字段的最大建议数
            
        Returns:
            List: 映射建议列表
        """
        suggestions = []
        
        for source_field in source_fields:
            source_name = source_field['field_name']
            source_type = source_field.get('data_type', 'unknown')
            
            # 计算与所有目标字段的相似度
            candidates = []
            
            for target_field in target_fields:
                target_name = target_field['field_name']
                target_type = target_field.get('data_type', 'unknown')
                
                # 增强相似度计算
                similarity_scores = self.calculate_enhanced_similarity(source_name, target_name)
                
                # 数据类型兼容性检查
                type_compatibility = self._check_enhanced_type_compatibility(source_type, target_type)
                
                # 综合评分
                final_score = (
                    similarity_scores['overall_similarity'] * 0.8 +
                    type_compatibility * 0.2
                )
                
                if final_score > 0.2:  # 过滤低分建议
                    candidates.append({
                        'target_field': target_name,
                        'target_type': target_type,
                        'similarity_scores': similarity_scores,
                        'type_compatibility': type_compatibility,
                        'final_score': round(final_score, 4),
                        'recommendation_reason': self._generate_recommendation_reason(
                            source_name, target_name, similarity_scores, type_compatibility
                        )
                    })
            
            # 按最终得分排序并取前N个建议
            candidates.sort(key=lambda x: x['final_score'], reverse=True)
            top_candidates = candidates[:max_suggestions]
            
            suggestions.append({
                'source_field': source_name,
                'source_type': source_type,
                'suggested_mappings': top_candidates,
                'mapping_count': len(top_candidates)
            })
        
        return suggestions
    
    def _check_enhanced_type_compatibility(self, type1: str, type2: str) -> float:
        """增强的数据类型兼容性检查"""
        if type1 == type2:
            return 1.0
        
        # 数值类型兼容性
        numeric_types = {'number', 'integer', 'float', 'decimal', 'numeric'}
        if type1 in numeric_types and type2 in numeric_types:
            return 0.9
        
        # 字符串类型兼容性
        string_types = {'string', 'text', 'varchar', 'char'}
        if type1 in string_types and type2 in string_types:
            return 0.8
        
        # 日期时间类型兼容性
        datetime_types = {'date', 'datetime', 'timestamp', 'time'}
        if type1 in datetime_types and type2 in datetime_types:
            return 0.9
        
        # 布尔类型兼容性
        boolean_types = {'boolean', 'bool', 'bit'}
        if type1 in boolean_types and type2 in boolean_types:
            return 1.0
        
        # 跨类型兼容性
        cross_compatible = {
            ('string', 'number'): 0.3,
            ('string', 'date'): 0.4,
            ('number', 'string'): 0.5,
            ('date', 'string'): 0.6
        }
        
        return cross_compatible.get((type1, type2), 0.1)
    
    def _generate_recommendation_reason(self, source_field: str, target_field: str,
                                      similarity_scores: Dict, type_compatibility: float) -> str:
        """生成推荐理由"""
        reasons = []
        
        # 字符相似度理由
        if similarity_scores['character_similarity'] > 0.8:
            reasons.append("字段名称高度相似")
        elif similarity_scores['character_similarity'] > 0.5:
            reasons.append("字段名称部分相似")
        
        # 语义相似度理由
        if similarity_scores['semantic_similarity'] > 0.7:
            reasons.append("语义含义相近")
        elif similarity_scores['semantic_similarity'] > 0.4:
            reasons.append("语义有一定关联")
        
        # 历史映射理由
        if similarity_scores['history_similarity'] > 0.5:
            reasons.append("历史映射经验支持")
        
        # 类型兼容性理由
        if type_compatibility > 0.8:
            reasons.append("数据类型完全兼容")
        elif type_compatibility > 0.5:
            reasons.append("数据类型基本兼容")
        
        # 综合置信度
        confidence_desc = {
            'very_high': '非常高置信度',
            'high': '高置信度',
            'medium': '中等置信度',
            'low': '较低置信度',
            'very_low': '很低置信度'
        }
        
        confidence_reason = confidence_desc.get(similarity_scores['confidence'], '未知置信度')
        reasons.append(confidence_reason)
        
        return "；".join(reasons) if reasons else "基础匹配"
    
    def save_mapping_history(self, source_field: str, target_field: str):
        """保存映射历史"""
        if target_field not in self.mapping_history[source_field]:
            self.mapping_history[source_field].append(target_field)
            
        # 持久化到文件
        try:
            with open('mapping_history.json', 'w', encoding='utf-8') as f:
                json.dump(dict(self.mapping_history), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存映射历史失败: {str(e)}")
    
    def load_mapping_history(self):
        """加载映射历史"""
        try:
            with open('mapping_history.json', 'r', encoding='utf-8') as f:
                history_data = json.load(f)
                for source, targets in history_data.items():
                    self.mapping_history[source] = targets
        except FileNotFoundError:
            logger.info("映射历史文件不存在，将创建新的历史记录")
        except Exception as e:
            logger.warning(f"加载映射历史失败: {str(e)}")
    
    def analyze_multi_table_relationships(self, table_schemas: List[Dict]) -> Dict[str, Any]:
        """
        分析多表关系
        
        Args:
            table_schemas: 表结构列表
            
        Returns:
            Dict: 多表关系分析结果
        """
        relationships = {
            'potential_foreign_keys': [],
            'similar_fields_across_tables': [],
            'suggested_join_conditions': [],
            'table_similarity_matrix': {}
        }
        
        # 分析潜在的外键关系
        for i, table1 in enumerate(table_schemas):
            for j, table2 in enumerate(table_schemas):
                if i >= j:  # 避免重复比较
                    continue
                    
                table1_name = table1['table_name']
                table2_name = table2['table_name']
                
                # 寻找相似字段
                similar_fields = self._find_similar_fields_between_tables(
                    table1['fields'], table2['fields']
                )
                
                if similar_fields:
                    relationships['similar_fields_across_tables'].append({
                        'table1': table1_name,
                        'table2': table2_name,
                        'similar_fields': similar_fields
                    })
                
                # 分析潜在的连接条件
                join_conditions = self._suggest_join_conditions(
                    table1_name, table1['fields'],
                    table2_name, table2['fields']
                )
                
                if join_conditions:
                    relationships['suggested_join_conditions'].extend(join_conditions)
        
        return relationships
    
    def _find_similar_fields_between_tables(self, fields1: List[Dict], 
                                          fields2: List[Dict]) -> List[Dict]:
        """查找表间相似字段"""
        similar_fields = []
        
        for field1 in fields1:
            for field2 in fields2:
                similarity = self.calculate_enhanced_similarity(
                    field1['field_name'], field2['field_name']
                )
                
                if similarity['overall_similarity'] > 0.7:
                    similar_fields.append({
                        'field1': field1['field_name'],
                        'field2': field2['field_name'],
                        'similarity': similarity['overall_similarity'],
                        'confidence': similarity['confidence']
                    })
        
        return similar_fields
    
    def _suggest_join_conditions(self, table1_name: str, fields1: List[Dict],
                               table2_name: str, fields2: List[Dict]) -> List[Dict]:
        """建议连接条件"""
        join_conditions = []
        
        # 寻找可能的主键-外键关系
        for field1 in fields1:
            field1_name = field1['field_name'].lower()
            
            # 检查是否为ID字段
            if any(keyword in field1_name for keyword in ['id', '编号', '代码', 'code']):
                for field2 in fields2:
                    field2_name = field2['field_name'].lower()
                    
                    # 检查是否包含表名引用
                    if (table1_name.lower() in field2_name or 
                        any(keyword in field2_name for keyword in ['id', '编号', '代码', 'code'])):
                        
                        similarity = self.calculate_enhanced_similarity(field1_name, field2_name)
                        
                        if similarity['overall_similarity'] > 0.6:
                            join_conditions.append({
                                'table1': table1_name,
                                'field1': field1['field_name'],
                                'table2': table2_name,
                                'field2': field2['field_name'],
                                'join_type': 'potential_fk_relationship',
                                'confidence': similarity['confidence'],
                                'similarity_score': similarity['overall_similarity']
                            })
        
        return join_conditions
