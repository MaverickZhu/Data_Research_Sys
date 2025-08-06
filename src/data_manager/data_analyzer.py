"""
数据结构分析器
分析CSV数据的字段类型、数据质量、统计信息等
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Any, Optional
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)

class DataAnalyzer:
    """数据结构分析器"""
    
    def __init__(self):
        """初始化数据分析器"""
        self.phone_pattern = re.compile(r'^(\+86)?1[3-9]\d{9}$|^\d{3,4}-\d{7,8}$')
        self.email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.id_card_pattern = re.compile(r'^\d{15}$|^\d{17}[\dX]$')
        self.credit_code_pattern = re.compile(r'^[0-9A-HJ-NPQRTUWXY]{2}\d{6}[0-9A-HJ-NPQRTUWXY]{10}$')
        
        # 常见字段类型关键词
        self.field_keywords = {
            'name': ['名称', '姓名', 'name', '公司', '企业', '单位', '机构'],
            'address': ['地址', '位置', 'address', '所在地', '注册地'],
            'phone': ['电话', '手机', 'phone', 'tel', '联系方式', '联系电话'],
            'email': ['邮箱', '邮件', 'email', 'mail'],
            'date': ['时间', '日期', 'date', 'time', '创建时间', '更新时间'],
            'id': ['id', 'ID', '编号', '代码', '标识'],
            'money': ['金额', '价格', '费用', '成本', '收入', '支出', 'money', 'price'],
            'person': ['法人', '代表', '负责人', '联系人', '经理', '主管']
        }
    
    def analyze_dataframe(self, df: pd.DataFrame, table_name: str = None) -> Dict[str, Any]:
        """
        分析DataFrame的完整信息
        
        Args:
            df: 要分析的DataFrame
            table_name: 表名称
            
        Returns:
            Dict: 分析结果
        """
        try:
            analysis_result = {
                'table_name': table_name or 'unknown_table',
                'basic_info': self._analyze_basic_info(df),
                'columns_analysis': self._analyze_columns(df),
                'data_quality': self._analyze_data_quality(df),
                'statistical_summary': self._analyze_statistics(df),
                'data_types_recommendation': self._recommend_data_types(df),
                'potential_keys': self._identify_potential_keys(df),
                'relationships_hints': self._analyze_relationships(df),
                'analysis_time': datetime.now().isoformat()
            }
            
            logger.info(f"完成数据分析: {table_name}, {df.shape[0]}行 x {df.shape[1]}列")
            return analysis_result
            
        except Exception as e:
            logger.error(f"数据分析失败: {str(e)}")
            return {'error': str(e)}
    
    def _analyze_basic_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """分析基础信息"""
        return {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'duplicated_rows': df.duplicated().sum(),
            'empty_rows': df.isnull().all(axis=1).sum(),
            'columns': df.columns.tolist()
        }
    
    def _analyze_columns(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """分析每个列的详细信息"""
        columns_analysis = []
        
        for col in df.columns:
            col_data = df[col]
            
            analysis = {
                'column_name': col,
                'pandas_dtype': str(col_data.dtype),
                'inferred_type': self._infer_column_type(col, col_data),
                'null_count': col_data.isnull().sum(),
                'null_percentage': col_data.isnull().sum() / len(col_data) * 100,
                'unique_count': col_data.nunique(),
                'unique_percentage': col_data.nunique() / len(col_data) * 100,
                'sample_values': self._get_sample_values(col_data),
                'value_counts': self._get_value_distribution(col_data),
                'potential_issues': self._identify_column_issues(col, col_data)
            }
            
            # 添加数值类型的统计信息
            if pd.api.types.is_numeric_dtype(col_data):
                analysis.update(self._analyze_numeric_column(col_data))
            
            # 添加文本类型的统计信息
            elif pd.api.types.is_string_dtype(col_data) or col_data.dtype == 'object':
                analysis.update(self._analyze_text_column(col_data))
                
            columns_analysis.append(analysis)
        
        return columns_analysis
    
    def _infer_column_type(self, col_name: str, col_data: pd.Series) -> str:
        """推断列的业务类型"""
        # 先基于列名推断
        col_name_lower = col_name.lower()
        
        for type_name, keywords in self.field_keywords.items():
            if any(keyword.lower() in col_name_lower for keyword in keywords):
                return type_name
        
        # 基于数据内容推断
        non_null_data = col_data.dropna()
        if len(non_null_data) == 0:
            return 'unknown'
        
        # 检查是否为数值类型
        if pd.api.types.is_numeric_dtype(col_data):
            if col_data.dtype in ['int64', 'int32', 'int16', 'int8']:
                return 'integer'
            else:
                return 'numeric'
        
        # 检查字符串类型的特殊模式
        if pd.api.types.is_string_dtype(col_data) or col_data.dtype == 'object':
            sample_values = non_null_data.astype(str).head(100)
            
            # 检查电话号码
            phone_matches = sum(1 for val in sample_values if self.phone_pattern.match(val))
            if phone_matches / len(sample_values) > 0.8:
                return 'phone'
            
            # 检查邮箱
            email_matches = sum(1 for val in sample_values if self.email_pattern.match(val))
            if email_matches / len(sample_values) > 0.8:
                return 'email'
                
            # 检查身份证号
            id_matches = sum(1 for val in sample_values if self.id_card_pattern.match(val))
            if id_matches / len(sample_values) > 0.8:
                return 'id_card'
                
            # 检查统一社会信用代码
            credit_matches = sum(1 for val in sample_values if self.credit_code_pattern.match(val))
            if credit_matches / len(sample_values) > 0.8:
                return 'credit_code'
            
            # 检查日期格式
            try:
                pd.to_datetime(sample_values.head(10))
                return 'date'
            except:
                pass
            
            # 检查是否为长文本
            avg_length = sample_values.str.len().mean()
            if avg_length > 50:
                return 'long_text'
            else:
                return 'text'
        
        return 'unknown'
    
    def _analyze_numeric_column(self, col_data: pd.Series) -> Dict[str, Any]:
        """分析数值列"""
        return {
            'min_value': col_data.min(),
            'max_value': col_data.max(),
            'mean_value': col_data.mean(),
            'median_value': col_data.median(),
            'std_value': col_data.std(),
            'zero_count': (col_data == 0).sum(),
            'negative_count': (col_data < 0).sum(),
            'quartiles': {
                'q1': col_data.quantile(0.25),
                'q2': col_data.quantile(0.5),
                'q3': col_data.quantile(0.75)
            }
        }
    
    def _analyze_text_column(self, col_data: pd.Series) -> Dict[str, Any]:
        """分析文本列"""
        text_data = col_data.dropna().astype(str)
        
        return {
            'min_length': text_data.str.len().min(),
            'max_length': text_data.str.len().max(),
            'avg_length': text_data.str.len().mean(),
            'empty_strings': (text_data == '').sum(),
            'contains_chinese': text_data.str.contains(r'[\u4e00-\u9fff]').sum(),
            'contains_english': text_data.str.contains(r'[a-zA-Z]').sum(),
            'contains_numbers': text_data.str.contains(r'\d').sum()
        }
    
    def _get_sample_values(self, col_data: pd.Series, max_samples: int = 5) -> List[Any]:
        """获取列的样本值"""
        non_null_data = col_data.dropna()
        if len(non_null_data) == 0:
            return []
        
        # 获取最常见的值
        value_counts = non_null_data.value_counts().head(max_samples)
        return [{'value': val, 'count': count} for val, count in value_counts.items()]
    
    def _get_value_distribution(self, col_data: pd.Series, max_categories: int = 10) -> Dict[str, int]:
        """获取值分布（适用于分类数据）"""
        if col_data.nunique() <= max_categories:
            return col_data.value_counts().to_dict()
        else:
            return {'unique_values_too_many': col_data.nunique()}
    
    def _identify_column_issues(self, col_name: str, col_data: pd.Series) -> List[str]:
        """识别列的潜在问题"""
        issues = []
        
        # 检查缺失值比例
        null_percentage = col_data.isnull().sum() / len(col_data) * 100
        if null_percentage > 50:
            issues.append(f"缺失值过多: {null_percentage:.1f}%")
        
        # 检查唯一值比例
        unique_percentage = col_data.nunique() / len(col_data) * 100
        if unique_percentage > 95:
            issues.append("几乎所有值都是唯一的，可能是主键")
        elif unique_percentage < 5:
            issues.append("值重复度过高，数据多样性不足")
        
        # 检查数据类型一致性
        if col_data.dtype == 'object':
            try:
                pd.to_numeric(col_data.dropna())
                issues.append("可能应该是数值类型但存储为文本")
            except:
                pass
        
        return issues
    
    def _analyze_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """分析数据质量"""
        return {
            'completeness': {
                'total_cells': df.size,
                'non_null_cells': df.notna().sum().sum(),
                'completeness_rate': df.notna().sum().sum() / df.size * 100
            },
            'consistency': {
                'duplicated_rows': df.duplicated().sum(),
                'duplicated_percentage': df.duplicated().sum() / len(df) * 100
            },
            'validity': self._check_data_validity(df),
            'accuracy_hints': self._analyze_accuracy_hints(df)
        }
    
    def _check_data_validity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检查数据有效性"""
        validity_issues = []
        
        for col in df.columns:
            col_data = df[col]
            
            # 检查数值列的异常值
            if pd.api.types.is_numeric_dtype(col_data):
                q1 = col_data.quantile(0.25)
                q3 = col_data.quantile(0.75)
                iqr = q3 - q1
                outliers = ((col_data < (q1 - 1.5 * iqr)) | (col_data > (q3 + 1.5 * iqr))).sum()
                
                if outliers > 0:
                    validity_issues.append({
                        'column': col,
                        'issue': f'检测到{outliers}个异常值'
                    })
        
        return {'issues': validity_issues}
    
    def _analyze_accuracy_hints(self, df: pd.DataFrame) -> List[str]:
        """分析数据准确性提示"""
        hints = []
        
        # 检查可能的编码问题
        for col in df.select_dtypes(include=['object']).columns:
            sample_text = df[col].dropna().astype(str).head(100)
            if any('?' in text or '乱码' in text for text in sample_text):
                hints.append(f"列'{col}'可能存在编码问题")
        
        return hints
    
    def _analyze_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """分析统计摘要"""
        return {
            'numeric_summary': df.describe().to_dict() if len(df.select_dtypes(include=[np.number]).columns) > 0 else {},
            'categorical_summary': {
                col: {
                    'unique_count': df[col].nunique(),
                    'most_frequent': df[col].mode().iloc[0] if len(df[col].mode()) > 0 else None,
                    'frequency': df[col].value_counts().iloc[0] if len(df[col].value_counts()) > 0 else 0
                }
                for col in df.select_dtypes(include=['object', 'category']).columns
            }
        }
    
    def _recommend_data_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """推荐数据类型优化"""
        recommendations = {}
        
        for col in df.columns:
            current_type = str(df[col].dtype)
            inferred_type = self._infer_column_type(col, df[col])
            
            if inferred_type == 'date' and current_type == 'object':
                recommendations[col] = 'datetime64'
            elif inferred_type == 'integer' and current_type == 'float64':
                recommendations[col] = 'int64'
            elif inferred_type in ['text', 'name'] and df[col].nunique() / len(df) < 0.5:
                recommendations[col] = 'category'
        
        return recommendations
    
    def _identify_potential_keys(self, df: pd.DataFrame) -> Dict[str, Any]:
        """识别潜在的主键和外键"""
        potential_keys = {
            'primary_key_candidates': [],
            'foreign_key_candidates': [],
            'composite_key_candidates': []
        }
        
        for col in df.columns:
            unique_ratio = df[col].nunique() / len(df)
            
            # 主键候选
            if unique_ratio > 0.95 and df[col].notna().sum() == len(df):
                potential_keys['primary_key_candidates'].append({
                    'column': col,
                    'unique_ratio': unique_ratio,
                    'confidence': 'high' if unique_ratio == 1.0 else 'medium'
                })
            
            # 外键候选（基于命名模式）
            if col.lower().endswith('_id') or col.lower().endswith('id'):
                potential_keys['foreign_key_candidates'].append({
                    'column': col,
                    'reason': 'naming_pattern'
                })
        
        return potential_keys
    
    def _analyze_relationships(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """分析列之间的潜在关系"""
        relationships = []
        
        # 分析列之间的相关性（仅数值列）
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 1:
            correlation_matrix = df[numeric_cols].corr()
            
            for i, col1 in enumerate(numeric_cols):
                for j, col2 in enumerate(numeric_cols):
                    if i < j:  # 避免重复
                        corr_value = correlation_matrix.loc[col1, col2]
                        if abs(corr_value) > 0.7:  # 强相关
                            relationships.append({
                                'type': 'correlation',
                                'column1': col1,
                                'column2': col2,
                                'strength': abs(corr_value),
                                'direction': 'positive' if corr_value > 0 else 'negative'
                            })
        
        return relationships