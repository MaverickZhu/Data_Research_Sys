"""
数据验证引擎
用于验证CSV数据的质量、完整性和一致性
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class ValidationEngine:
    """数据验证引擎"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化验证引擎
        
        Args:
            config: 验证配置
        """
        self.config = config or {}
        
        # 验证规则配置
        self.validation_rules = self.config.get('validation_rules', {
            'completeness': {
                'enabled': True,
                'min_completeness': 0.8  # 最低完整度要求
            },
            'consistency': {
                'enabled': True,
                'max_duplicate_rate': 0.1  # 最大重复率
            },
            'validity': {
                'enabled': True,
                'outlier_threshold': 3.0  # 异常值检测阈值
            }
        })
    
    def validate_dataframe(self, df: pd.DataFrame, 
                          table_name: str = None) -> Dict[str, Any]:
        """
        验证DataFrame数据质量
        
        Args:
            df: 要验证的DataFrame
            table_name: 表名
            
        Returns:
            Dict: 验证结果
        """
        try:
            logger.info(f"开始验证数据表: {table_name}")
            
            validation_result = {
                'table_name': table_name or 'unknown_table',
                'validation_time': datetime.now().isoformat(),
                'overall_score': 0.0,
                'is_valid': True,
                'completeness': {},
                'consistency': {},
                'validity': {},
                'recommendations': []
            }
            
            scores = []
            
            # 完整性验证
            if self.validation_rules['completeness']['enabled']:
                completeness_result = self._validate_completeness(df)
                validation_result['completeness'] = completeness_result
                scores.append(completeness_result['score'])
                
                if completeness_result['score'] < 0.6:
                    validation_result['is_valid'] = False
                    validation_result['recommendations'].append(
                        "数据完整性不足，建议检查缺失值处理"
                    )
            
            # 一致性验证
            if self.validation_rules['consistency']['enabled']:
                consistency_result = self._validate_consistency(df)
                validation_result['consistency'] = consistency_result
                scores.append(consistency_result['score'])
                
                if consistency_result['score'] < 0.7:
                    validation_result['recommendations'].append(
                        "数据一致性存在问题，建议检查重复数据"
                    )
            
            # 有效性验证
            if self.validation_rules['validity']['enabled']:
                validity_result = self._validate_validity(df)
                validation_result['validity'] = validity_result
                scores.append(validity_result['score'])
                
                if validity_result['score'] < 0.7:
                    validation_result['recommendations'].append(
                        "数据有效性存在问题，建议检查异常值"
                    )
            
            # 计算总体评分
            validation_result['overall_score'] = np.mean(scores) if scores else 0.0
            
            logger.info(f"数据验证完成: {table_name}, 总体评分: {validation_result['overall_score']:.2f}")
            return validation_result
            
        except Exception as e:
            logger.error(f"数据验证失败: {str(e)}")
            return {
                'table_name': table_name,
                'error': str(e),
                'is_valid': False,
                'overall_score': 0.0
            }
    
    def _validate_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """验证数据完整性"""
        total_cells = df.size
        non_null_cells = df.notna().sum().sum()
        completeness_rate = non_null_cells / total_cells if total_cells > 0 else 0
        
        # 按列分析完整性
        column_completeness = {}
        for col in df.columns:
            col_completeness = df[col].notna().sum() / len(df)
            column_completeness[col] = {
                'completeness_rate': col_completeness,
                'missing_count': df[col].isna().sum(),
                'status': 'good' if col_completeness > 0.9 else 'warning' if col_completeness > 0.7 else 'poor'
            }
        
        # 计算完整性评分
        score = min(completeness_rate * 1.2, 1.0)  # 轻微加权
        
        return {
            'score': score,
            'overall_completeness': completeness_rate,
            'total_cells': total_cells,
            'non_null_cells': non_null_cells,
            'missing_cells': total_cells - non_null_cells,
            'column_analysis': column_completeness,
            'status': 'good' if score > 0.8 else 'warning' if score > 0.6 else 'poor'
        }
    
    def _validate_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """验证数据一致性"""
        total_rows = len(df)
        duplicate_rows = df.duplicated().sum()
        duplicate_rate = duplicate_rows / total_rows if total_rows > 0 else 0
        
        # 数据类型一致性检查
        type_inconsistencies = []
        for col in df.columns:
            if df[col].dtype == 'object':
                # 检查是否应该是数值类型
                try:
                    pd.to_numeric(df[col].dropna())
                    type_inconsistencies.append({
                        'column': col,
                        'issue': 'should_be_numeric',
                        'description': '可能应该是数值类型但存储为文本'
                    })
                except:
                    pass
        
        # 值域一致性检查
        value_inconsistencies = []
        for col in df.select_dtypes(include=[np.number]).columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            outliers = df[(df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))]
            if len(outliers) > 0:
                value_inconsistencies.append({
                    'column': col,
                    'outlier_count': len(outliers),
                    'outlier_rate': len(outliers) / len(df)
                })
        
        # 计算一致性评分
        duplicate_penalty = min(duplicate_rate * 2, 0.5)  # 重复数据惩罚
        type_penalty = min(len(type_inconsistencies) * 0.1, 0.3)  # 类型不一致惩罚
        score = max(1.0 - duplicate_penalty - type_penalty, 0.0)
        
        return {
            'score': score,
            'duplicate_rate': duplicate_rate,
            'duplicate_count': duplicate_rows,
            'type_inconsistencies': type_inconsistencies,
            'value_inconsistencies': value_inconsistencies,
            'status': 'good' if score > 0.8 else 'warning' if score > 0.6 else 'poor'
        }
    
    def _validate_validity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """验证数据有效性"""
        validity_issues = []
        
        # 格式有效性检查
        for col in df.columns:
            col_data = df[col].dropna()
            
            # 检查邮箱格式
            if 'email' in col.lower() or '邮箱' in col:
                import re
                email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
                invalid_emails = col_data[~col_data.astype(str).str.match(email_pattern)]
                if len(invalid_emails) > 0:
                    validity_issues.append({
                        'column': col,
                        'issue': 'invalid_email_format',
                        'count': len(invalid_emails),
                        'rate': len(invalid_emails) / len(col_data)
                    })
            
            # 检查电话格式
            if 'phone' in col.lower() or '电话' in col or '手机' in col:
                import re
                phone_pattern = re.compile(r'^(\+86)?1[3-9]\d{9}$|^\d{3,4}-\d{7,8}$')
                invalid_phones = col_data[~col_data.astype(str).str.match(phone_pattern)]
                if len(invalid_phones) > 0:
                    validity_issues.append({
                        'column': col,
                        'issue': 'invalid_phone_format',
                        'count': len(invalid_phones),
                        'rate': len(invalid_phones) / len(col_data)
                    })
        
        # 业务逻辑有效性检查
        business_issues = self._validate_business_logic(df)
        validity_issues.extend(business_issues)
        
        # 计算有效性评分
        total_issues = sum(issue.get('count', 0) for issue in validity_issues)
        total_values = df.notna().sum().sum()
        error_rate = total_issues / total_values if total_values > 0 else 0
        score = max(1.0 - error_rate * 2, 0.0)
        
        return {
            'score': score,
            'validity_issues': validity_issues,
            'total_issues': total_issues,
            'error_rate': error_rate,
            'status': 'good' if score > 0.8 else 'warning' if score > 0.6 else 'poor'
        }
    
    def _validate_business_logic(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """验证业务逻辑"""
        issues = []
        
        # 日期逻辑检查
        date_columns = []
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['date', 'time', '时间', '日期']):
                date_columns.append(col)
        
        for col in date_columns:
            try:
                dates = pd.to_datetime(df[col].dropna())
                
                # 检查未来日期
                future_dates = dates[dates > datetime.now()]
                if len(future_dates) > 0:
                    issues.append({
                        'column': col,
                        'issue': 'future_dates',
                        'count': len(future_dates),
                        'description': '存在未来日期'
                    })
                
                # 检查过于久远的日期
                old_dates = dates[dates < datetime(1900, 1, 1)]
                if len(old_dates) > 0:
                    issues.append({
                        'column': col,
                        'issue': 'too_old_dates',
                        'count': len(old_dates),
                        'description': '存在过于久远的日期'
                    })
                    
            except:
                continue
        
        return issues
    
    def generate_validation_report(self, validation_result: Dict[str, Any]) -> str:
        """生成验证报告"""
        table_name = validation_result.get('table_name', 'Unknown')
        overall_score = validation_result.get('overall_score', 0)
        is_valid = validation_result.get('is_valid', False)
        
        report = f"""
# 数据验证报告 - {table_name}

## 总体评估
- **验证时间**: {validation_result.get('validation_time', 'Unknown')}
- **总体评分**: {overall_score:.2f}/1.00
- **验证状态**: {'✅ 通过' if is_valid else '❌ 不通过'}

## 详细分析

### 完整性分析
- **评分**: {validation_result.get('completeness', {}).get('score', 0):.2f}
- **完整率**: {validation_result.get('completeness', {}).get('overall_completeness', 0)*100:.1f}%
- **缺失数据**: {validation_result.get('completeness', {}).get('missing_cells', 0)} 个单元格

### 一致性分析  
- **评分**: {validation_result.get('consistency', {}).get('score', 0):.2f}
- **重复率**: {validation_result.get('consistency', {}).get('duplicate_rate', 0)*100:.1f}%
- **类型不一致**: {len(validation_result.get('consistency', {}).get('type_inconsistencies', []))} 个问题

### 有效性分析
- **评分**: {validation_result.get('validity', {}).get('score', 0):.2f}
- **错误率**: {validation_result.get('validity', {}).get('error_rate', 0)*100:.1f}%
- **有效性问题**: {validation_result.get('validity', {}).get('total_issues', 0)} 个

## 改进建议
"""
        
        recommendations = validation_result.get('recommendations', [])
        for i, rec in enumerate(recommendations, 1):
            report += f"\n{i}. {rec}"
        
        return report