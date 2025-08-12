"""
CSV文件处理器
处理CSV文件的上传、解析、编码检测和基础清洗
"""

import pandas as pd
import chardet
import io
import logging
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import numpy as np

logger = logging.getLogger(__name__)

class CSVProcessor:
    """CSV文件处理器"""
    
    def __init__(self, max_file_size: int = 500 * 1024 * 1024):  # 500MB
        """
        初始化CSV处理器
        
        Args:
            max_file_size: 最大文件大小限制(字节)
        """
        self.max_file_size = max_file_size
        self.supported_encodings = ['utf-8', 'gbk', 'gb2312', 'utf-16', 'ascii']
        self.chunk_size = 10000  # 分块读取大小
        
    def detect_encoding(self, file_content: bytes) -> str:
        """
        检测文件编码
        
        Args:
            file_content: 文件二进制内容
            
        Returns:
            str: 检测到的编码格式
        """
        try:
            # 使用chardet检测编码
            result = chardet.detect(file_content[:10000])  # 检测前10KB
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            logger.info(f"检测到编码: {encoding}, 置信度: {confidence:.2f}")
            
            # 如果置信度太低，尝试常见编码
            if confidence < 0.7:
                for enc in self.supported_encodings:
                    try:
                        file_content.decode(enc)
                        logger.info(f"使用备用编码: {enc}")
                        return enc
                    except UnicodeDecodeError:
                        continue
                        
            return encoding if encoding else 'utf-8'
            
        except Exception as e:
            logger.warning(f"编码检测失败: {str(e)}, 使用默认编码utf-8")
            return 'utf-8'
    
    def validate_file(self, file_path: str, file_content: bytes = None) -> Dict[str, Any]:
        """
        验证文件有效性
        
        Args:
            file_path: 文件路径
            file_content: 文件内容(可选)
            
        Returns:
            Dict: 验证结果
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'file_info': {}
        }
        
        try:
            # 检查文件扩展名
            file_ext = Path(file_path).suffix.lower()
            allowed_extensions = ['.csv', '.txt', '.xlsx', '.xls']  # 默认支持的扩展名
            if file_ext not in allowed_extensions:
                validation_result['errors'].append(f"不支持的文件格式: {file_ext}")
                validation_result['valid'] = False
                
            # 检查文件大小
            if file_content:
                file_size = len(file_content)
                validation_result['file_info']['size'] = file_size
                
                if file_size > self.max_file_size:
                    validation_result['errors'].append(
                        f"文件大小超限: {file_size / 1024 / 1024:.1f}MB > {self.max_file_size / 1024 / 1024}MB"
                    )
                    validation_result['valid'] = False
                elif file_size == 0:
                    validation_result['errors'].append("文件为空")
                    validation_result['valid'] = False
                    
            return validation_result
            
        except Exception as e:
            validation_result['errors'].append(f"文件验证失败: {str(e)}")
            validation_result['valid'] = False
            return validation_result
    
    def parse_csv(self, file_content: bytes, file_name: str) -> Dict[str, Any]:
        """
        解析CSV文件
        
        Args:
            file_content: 文件二进制内容
            file_name: 文件名
            
        Returns:
            Dict: 解析结果
        """
        result = {
            'success': False,
            'data': None,
            'metadata': {},
            'errors': [],
            'warnings': []
        }
        
        try:
            logger.info(f"开始解析文件: {file_name}")
            # 1. 验证文件
            validation = self.validate_file(file_name, file_content)
            logger.info(f"文件验证结果: {validation}")
            if not validation['valid']:
                result['errors'].extend(validation['errors'])
                return result
                
            # 2. 检测编码
            encoding = self.detect_encoding(file_content)
            logger.info(f"检测到编码: {encoding}")
            result['metadata']['encoding'] = encoding
            
            # 3. 解码文件内容（增强编码容错）
            text_content = None
            encoding_attempts = [encoding]
            
            # 如果检测到GB2312，尝试GBK作为备选
            if encoding.lower() == 'gb2312':
                encoding_attempts.append('gbk')
            
            # 添加常用编码作为备选
            for fallback in ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']:
                if fallback not in encoding_attempts:
                    encoding_attempts.append(fallback)
            
            decode_error = None
            for attempt_encoding in encoding_attempts:
                try:
                    text_content = file_content.decode(attempt_encoding)
                    logger.info(f"文件解码成功，使用编码: {attempt_encoding}，内容长度: {len(text_content)}")
                    result['metadata']['encoding'] = attempt_encoding
                    break
                except UnicodeDecodeError as e:
                    decode_error = e
                    logger.warning(f"编码 {attempt_encoding} 解码失败: {str(e)}")
                    continue
            
            if text_content is None:
                result['errors'].append(f"文件解码失败，尝试了所有编码: {str(decode_error)}")
                logger.error(f"文件解码失败，尝试了编码: {encoding_attempts}")
                return result
                
            # 4. 检测文件类型并解析
            file_ext = Path(file_name).suffix.lower()
            logger.info(f"文件扩展名: {file_ext}")
            
            if file_ext in ['.xlsx', '.xls']:
                # Excel文件处理
                csv_data = pd.read_excel(
                    io.BytesIO(file_content),
                    na_values=['', 'NULL', 'null', 'None', 'N/A', 'NA']
                )
                result['metadata']['delimiter'] = 'N/A (Excel)'
            else:
                # CSV/TXT文件处理
                delimiter = self._detect_delimiter(text_content[:5000])
                result['metadata']['delimiter'] = delimiter
                
                csv_data = pd.read_csv(
                    io.StringIO(text_content),
                    delimiter=delimiter,
                    low_memory=False,
                    na_values=['', 'NULL', 'null', 'None', 'N/A', 'NA']
                )
            
            # 6. 基础数据清洗
            csv_data = self._basic_cleaning(csv_data)
            
            result['data'] = csv_data
            result['metadata'].update({
                'file_name': file_name,
                'total_rows': len(csv_data),
                'total_columns': len(csv_data.columns),
                'columns': csv_data.columns.tolist(),
                'dtypes': csv_data.dtypes.to_dict()
            })
            
            result['success'] = True
            logger.info(f"成功解析CSV文件: {file_name}, {len(csv_data)}行 x {len(csv_data.columns)}列")
            
        except Exception as e:
            error_msg = f"CSV解析失败: {str(e)}"
            result['errors'].append(error_msg)
            logger.error(error_msg)
            
        return result
    
    def _detect_delimiter(self, sample_text: str) -> str:
        """
        检测CSV分隔符
        
        Args:
            sample_text: 样本文本
            
        Returns:
            str: 分隔符
        """
        delimiters = [',', ';', '\t', '|']
        delimiter_counts = {}
        
        for delimiter in delimiters:
            lines = sample_text.split('\n')[:10]  # 检查前10行
            counts = []
            
            for line in lines:
                if line.strip():
                    counts.append(line.count(delimiter))
                    
            if counts:
                # 检查分隔符数量是否一致
                if len(set(counts)) == 1 and counts[0] > 0:
                    delimiter_counts[delimiter] = counts[0]
                    
        # 返回出现次数最多的分隔符
        if delimiter_counts:
            return max(delimiter_counts.keys(), key=delimiter_counts.get)
        else:
            return ','  # 默认使用逗号
    
    def _basic_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        基础数据清洗
        
        Args:
            df: 原始DataFrame
            
        Returns:
            pd.DataFrame: 清洗后的DataFrame
        """
        try:
            # 1. 移除完全为空的行和列
            df = df.dropna(how='all').dropna(axis=1, how='all')
            
            # 2. 清理列名
            df.columns = df.columns.str.strip()  # 移除首尾空格
            
            # 3. 移除重复行
            duplicated_count = df.duplicated().sum()
            if duplicated_count > 0:
                df = df.drop_duplicates()
                logger.info(f"移除了 {duplicated_count} 个重复行")
                
            # 4. 基础数据类型优化
            df = self._optimize_dtypes(df)
            
            return df
            
        except Exception as e:
            logger.error(f"数据清洗失败: {str(e)}")
            return df
    
    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        优化数据类型以节省内存
        
        Args:
            df: DataFrame
            
        Returns:
            pd.DataFrame: 优化后的DataFrame
        """
        try:
            for col in df.columns:
                col_type = df[col].dtype
                
                # 优化整数类型
                if col_type == 'int64':
                    c_min = df[col].min()
                    c_max = df[col].max()
                    
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                        
                # 优化浮点数类型
                elif col_type == 'float64':
                    df[col] = pd.to_numeric(df[col], downcast='float')
                    
                # 优化字符串类型
                elif col_type == 'object':
                    if df[col].nunique() / len(df) < 0.5:  # 如果唯一值比例小于50%
                        df[col] = df[col].astype('category')
                        
            return df
            
        except Exception as e:
            logger.warning(f"数据类型优化失败: {str(e)}")
            return df
    
    def get_sample_data(self, df: pd.DataFrame, sample_size: int = 10) -> Dict[str, Any]:
        """
        获取样本数据用于预览
        
        Args:
            df: DataFrame
            sample_size: 样本大小
            
        Returns:
            Dict: 样本数据信息
        """
        try:
            sample_data = {
                'head': df.head(sample_size).to_dict('records'),
                'columns': df.columns.tolist(),
                'dtypes': df.dtypes.to_dict(),
                'shape': df.shape,
                'memory_usage': df.memory_usage(deep=True).sum(),
                'null_counts': df.isnull().sum().to_dict()
            }
            
            return sample_data
            
        except Exception as e:
            logger.error(f"获取样本数据失败: {str(e)}")
            return {}
    
    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        处理CSV文件 - 完整的文件处理流程
        
        Args:
            file_path: 文件路径
            
        Returns:
            Dict: 处理结果，包含dataframe和其他元数据
        """
        try:
            file_ext = Path(file_path).suffix.lower()

            # Excel文件处理
            if file_ext in ['.xlsx', '.xls']:
                # 直接使用pandas读取Excel
                df = pd.read_excel(file_path)
                df = self._basic_cleaning(df)

                metadata = {
                    'file_name': Path(file_path).name,
                    'total_rows': len(df),
                    'total_columns': len(df.columns),
                    'columns': df.columns.tolist(),
                    'dtypes': df.dtypes.to_dict(),
                    'encoding': 'binary',
                    'delimiter': ','
                }

                return {
                    'success': True,
                    'dataframe': df,
                    'metadata': metadata,
                    'encoding': metadata.get('encoding', 'binary'),
                    'delimiter': metadata.get('delimiter', ','),
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'sample_data': self.get_sample_data(df),
                    'warnings': []
                }

            # 文本类CSV/TXT处理
            # 读取文件内容
            with open(file_path, 'rb') as f:
                file_content = f.read()

            # 验证文件
            validation_result = self.validate_file(file_path, file_content)
            if not validation_result['valid']:
                return {
                    'success': False,
                    'error': f"文件验证失败: {', '.join(validation_result['errors'])}",
                    'dataframe': None
                }

            # 解析CSV文件
            file_name = Path(file_path).name
            parse_result = self.parse_csv(file_content, file_name)

            if not parse_result['success']:
                return {
                    'success': False,
                    'error': f"CSV解析失败: {', '.join(parse_result['errors'])}",
                    'dataframe': None
                }

            # 返回成功结果
            metadata = parse_result['metadata']
            return {
                'success': True,
                'dataframe': parse_result['data'],
                'metadata': metadata,
                'encoding': metadata.get('encoding', 'utf-8'),
                'delimiter': metadata.get('delimiter', ','),
                'row_count': len(parse_result['data']) if parse_result['data'] is not None else 0,
                'column_count': len(parse_result['data'].columns) if parse_result['data'] is not None else 0,
                'sample_data': self.get_sample_data(parse_result['data']),
                'warnings': parse_result['warnings']
            }

        except Exception as e:
            logger.error(f"处理文件失败: {str(e)}")
            return {
                'success': False,
                'error': f"处理文件时发生错误: {str(e)}",
                'dataframe': None
            }