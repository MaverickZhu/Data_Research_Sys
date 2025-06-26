"""
一对多匹配处理器
支持单个安全排查系统单位匹配多个消防监督系统检查记录
"""

import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from bson import ObjectId

from ..database.connection import DatabaseManager
from .exact_matcher import ExactMatcher
from .fuzzy_matcher import FuzzyMatcher
from .match_result import MultiMatchResult

logger = logging.getLogger(__name__)


class MultiMatchProcessor:
    """一对多匹配处理器"""
    
    def __init__(self, db_manager: DatabaseManager, config: Dict):
        """
        初始化一对多匹配处理器
        
        Args:
            db_manager: 数据库管理器
            config: 匹配配置
        """
        self.db_manager = db_manager
        self.config = config
        
        # 初始化匹配器
        self.exact_matcher = ExactMatcher(config)
        self.fuzzy_matcher = FuzzyMatcher(config)
        
        # 获取集合配置
        db_config = config.get('database', {})
        self.source_collection_name = db_config.get('inspection_collection', 'xfaqpc_jzdwxx')
        self.target_collection_name = db_config.get('supervision_collection', 'xxj_shdwjbxx')
        self.result_collection_name = db_config.get('multi_match_results', 'unit_multi_match_results')
        
        # 批处理配置
        self.batch_size = config.get('performance', {}).get('batch_size', 1000)
        
        logger.info(f"一对多匹配处理器初始化完成")
        logger.info(f"  源集合: {self.source_collection_name}")
        logger.info(f"  目标集合: {self.target_collection_name}")
        logger.info(f"  结果集合: {self.result_collection_name}")
        logger.info(f"  批处理大小: {self.batch_size}")
    
    def process_all_records(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        处理所有记录的一对多匹配
        
        Args:
            limit: 限制处理的记录数量（用于测试）
            
        Returns:
            Dict: 处理结果统计
        """
        start_time = time.time()
        
        logger.info("开始一对多匹配处理...")
        
        try:
            # 获取源记录（安全排查系统）
            source_collection = self.db_manager.get_collection(self.source_collection_name)
            target_collection = self.db_manager.get_collection(self.target_collection_name)
            
            # 统计总数
            total_source_count = source_collection.count_documents({})
            total_target_count = target_collection.count_documents({})
            
            logger.info(f"数据统计:")
            logger.info(f"  安全排查系统记录: {total_source_count:,}")
            logger.info(f"  消防监督系统记录: {total_target_count:,}")
            
            # 应用限制
            if limit:
                total_source_count = min(total_source_count, limit)
                logger.info(f"  限制处理数量: {limit:,}")
            
            # 清空结果集合
            result_collection = self.db_manager.get_collection(self.result_collection_name)
            result_collection.delete_many({})
            logger.info("清空了之前的匹配结果")
            
            # 分批处理
            processed_count = 0
            matched_count = 0
            unmatched_count = 0
            total_matched_records = 0
            
            # 获取所有目标记录（消防监督系统）到内存中
            logger.info("加载目标记录到内存...")
            target_records = list(target_collection.find({}))
            logger.info(f"已加载 {len(target_records):,} 条目标记录")
            
            # 分批处理源记录
            for batch_start in range(0, total_source_count, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_source_count)
                
                logger.info(f"处理批次 {batch_start:,} - {batch_end:,}")
                
                # 获取当前批次的源记录
                source_records = list(source_collection.find({}).skip(batch_start).limit(self.batch_size))
                
                # 处理当前批次
                batch_results = []
                for source_record in source_records:
                    try:
                        # 执行一对多匹配
                        match_result = self._match_single_record(source_record, target_records)
                        
                        # 转换为数据库格式
                        db_record = self._convert_to_db_format(match_result)
                        batch_results.append(db_record)
                        
                        # 更新统计
                        if match_result.matched:
                            matched_count += 1
                            total_matched_records += match_result.total_matches
                        else:
                            unmatched_count += 1
                        
                        processed_count += 1
                        
                        # 定期输出进度
                        if processed_count % 100 == 0:
                            elapsed = time.time() - start_time
                            rate = processed_count / elapsed
                            logger.info(f"已处理 {processed_count:,}/{total_source_count:,} ({processed_count/total_source_count*100:.1f}%) - 速度: {rate:.1f} 记录/秒")
                    
                    except Exception as e:
                        logger.error(f"处理记录时出错: {str(e)}")
                        unmatched_count += 1
                        processed_count += 1
                
                # 批量插入结果
                if batch_results:
                    try:
                        result_collection.insert_many(batch_results, ordered=False)
                        logger.info(f"批次结果已保存: {len(batch_results)} 条记录")
                    except Exception as e:
                        logger.error(f"保存批次结果时出错: {str(e)}")
            
            # 最终统计
            end_time = time.time()
            total_time = end_time - start_time
            
            statistics = {
                'total_processed': processed_count,
                'matched_units': matched_count,
                'unmatched_units': unmatched_count,
                'total_matched_records': total_matched_records,
                'match_rate': matched_count / max(processed_count, 1) * 100,
                'average_matches_per_unit': total_matched_records / max(matched_count, 1),
                'processing_time_seconds': total_time,
                'processing_rate': processed_count / total_time,
                'start_time': datetime.fromtimestamp(start_time).isoformat(),
                'end_time': datetime.fromtimestamp(end_time).isoformat()
            }
            
            logger.info("一对多匹配处理完成!")
            logger.info(f"  处理总数: {processed_count:,}")
            logger.info(f"  匹配成功: {matched_count:,} ({statistics['match_rate']:.1f}%)")
            logger.info(f"  匹配失败: {unmatched_count:,}")
            logger.info(f"  总匹配记录数: {total_matched_records:,}")
            logger.info(f"  平均每单位匹配记录数: {statistics['average_matches_per_unit']:.1f}")
            logger.info(f"  处理时间: {total_time:.1f} 秒")
            logger.info(f"  处理速度: {statistics['processing_rate']:.1f} 记录/秒")
            
            return statistics
            
        except Exception as e:
            logger.error(f"一对多匹配处理过程出错: {str(e)}")
            raise
    
    def _match_single_record(self, source_record: Dict, target_records: List[Dict]) -> MultiMatchResult:
        """
        对单条记录执行一对多匹配
        
        Args:
            source_record: 源记录（安全排查系统）
            target_records: 目标记录列表（消防监督系统）
            
        Returns:
            MultiMatchResult: 一对多匹配结果
        """
        try:
            # 首先尝试精确匹配
            exact_result = self.exact_matcher.match_single_record_multi(source_record, target_records)
            
            if exact_result.matched:
                logger.debug(f"精确匹配成功: {source_record.get('UNIT_NAME', 'Unknown')} -> {exact_result.total_matches} 条记录")
                return exact_result
            
            # 如果精确匹配失败，尝试模糊匹配
            fuzzy_result = self.fuzzy_matcher.match_single_record_multi(source_record, target_records)
            
            if fuzzy_result.matched:
                logger.debug(f"模糊匹配成功: {source_record.get('UNIT_NAME', 'Unknown')} -> {fuzzy_result.total_matches} 条记录")
                return fuzzy_result
            
            # 如果都失败，返回未匹配结果
            logger.debug(f"匹配失败: {source_record.get('UNIT_NAME', 'Unknown')}")
            return MultiMatchResult(
                matched=False,
                source_record=source_record,
                matched_records=[],
                match_summary={
                    'exact_matches': 0,
                    'fuzzy_matches': 0,
                    'total_matches': 0,
                    'match_method': 'none',
                    'reason': 'no_match_found'
                }
            )
            
        except Exception as e:
            logger.error(f"单记录匹配过程出错: {str(e)}")
            return MultiMatchResult(
                matched=False,
                source_record=source_record,
                matched_records=[],
                match_summary={
                    'exact_matches': 0,
                    'fuzzy_matches': 0,
                    'total_matches': 0,
                    'match_method': 'error',
                    'error': str(e)
                }
            )
    
    def _convert_to_db_format(self, match_result: MultiMatchResult) -> Dict:
        """
        将匹配结果转换为数据库存储格式
        
        Args:
            match_result: 一对多匹配结果
            
        Returns:
            Dict: 数据库格式的记录
        """
        source_record = match_result.source_record
        
        # 基础记录信息（来自安全排查系统）
        db_record = {
            '_id': ObjectId(),
            'primary_record_id': source_record.get('_id'),
            'primary_system': 'inspection',
            'unit_name': source_record.get('UNIT_NAME', ''),
            'address': source_record.get('ADDRESS', ''),
            'legal_representative': source_record.get('LEGAL_REPRESENTATIVE', ''),
            'contact_person': source_record.get('CONTACT_PERSON', ''),
            'contact_phone': source_record.get('CONTACT_PHONE', ''),
            'credit_code': source_record.get('CREDIT_CODE', ''),
            
            # 匹配状态
            'match_status': 'matched' if match_result.matched else 'unmatched',
            'match_summary': match_result.match_summary,
            
            # 匹配记录列表
            'matched_records': match_result.matched_records,
            'total_matched_records': match_result.total_matches,
            
            # 主要匹配记录信息
            'primary_matched_record': None,
            
            # 时间戳
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        
        # 如果有匹配记录，添加主要匹配信息
        if match_result.matched and match_result.primary_match:
            primary_match = match_result.primary_match
            db_record['primary_matched_record'] = {
                'record_id': primary_match.get('_id'),
                'unit_name': primary_match.get('dwmc', ''),
                'address': primary_match.get('dwdz', ''),
                'legal_representative': primary_match.get('fddbr', ''),
                'contact_phone': primary_match.get('frlxdh', ''),
                'credit_code': primary_match.get('tyshxydm', ''),
                'inspection_date': self._extract_inspection_date(primary_match),
                'match_type': primary_match.get('match_info', {}).get('match_type', 'unknown'),
                'similarity_score': primary_match.get('match_info', {}).get('similarity_score', 0.0)
            }
        
        return db_record
    
    def _extract_inspection_date(self, record: Dict) -> Optional[str]:
        """从记录中提取检查日期"""
        date_fields = ['jcsj', 'inspection_date', 'create_time', 'update_time']
        
        for field in date_fields:
            if field in record and record[field]:
                date_value = record[field]
                if isinstance(date_value, str):
                    return date_value
                elif hasattr(date_value, 'isoformat'):
                    return date_value.isoformat()
                else:
                    return str(date_value)
        
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取匹配结果统计信息
        
        Returns:
            Dict: 统计信息
        """
        try:
            result_collection = self.db_manager.get_collection(self.result_collection_name)
            
            # 基础统计
            total_records = result_collection.count_documents({})
            matched_records = result_collection.count_documents({'match_status': 'matched'})
            unmatched_records = result_collection.count_documents({'match_status': 'unmatched'})
            
            # 匹配类型统计
            exact_matches = result_collection.count_documents({
                'match_summary.exact_matches': {'$gt': 0}
            })
            fuzzy_matches = result_collection.count_documents({
                'match_summary.fuzzy_matches': {'$gt': 0}
            })
            
            # 多重匹配统计
            multiple_matches = result_collection.count_documents({
                'total_matched_records': {'$gt': 1}
            })
            
            # 计算总匹配记录数
            pipeline = [
                {'$group': {
                    '_id': None,
                    'total_matched_records': {'$sum': '$total_matched_records'}
                }}
            ]
            total_matched_records_result = list(result_collection.aggregate(pipeline))
            total_matched_records = total_matched_records_result[0]['total_matched_records'] if total_matched_records_result else 0
            
            statistics = {
                'total_units': total_records,
                'matched_units': matched_records,
                'unmatched_units': unmatched_records,
                'match_rate': (matched_records / max(total_records, 1)) * 100,
                'exact_matches': exact_matches,
                'fuzzy_matches': fuzzy_matches,
                'multiple_matches': multiple_matches,
                'multiple_match_rate': (multiple_matches / max(matched_records, 1)) * 100,
                'total_matched_records': total_matched_records,
                'average_matches_per_unit': total_matched_records / max(matched_records, 1),
                'collection_name': self.result_collection_name
            }
            
            return statistics
            
        except Exception as e:
            logger.error(f"获取统计信息时出错: {str(e)}")
            return {}
    
    def get_match_results(self, limit: int = 10, skip: int = 0) -> List[Dict]:
        """
        获取匹配结果列表
        
        Args:
            limit: 限制返回数量
            skip: 跳过数量
            
        Returns:
            List[Dict]: 匹配结果列表
        """
        try:
            result_collection = self.db_manager.get_collection(self.result_collection_name)
            
            results = list(result_collection.find({}).skip(skip).limit(limit))
            
            return results
            
        except Exception as e:
            logger.error(f"获取匹配结果时出错: {str(e)}")
            return [] 