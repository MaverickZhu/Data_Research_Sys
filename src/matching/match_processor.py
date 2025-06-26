"""
匹配处理器模块
统一处理精确匹配和模糊匹配的业务逻辑
"""

import logging
import uuid
import json
from typing import Dict, List, Optional, Union
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

from .exact_matcher import ExactMatcher, MatchResult
from .fuzzy_matcher import FuzzyMatcher, FuzzyMatchResult
from src.utils.helpers import batch_iterator, generate_match_id, format_timestamp

logger = logging.getLogger(__name__)


class MatchProgress:
    """匹配进度跟踪"""
    
    def __init__(self, task_id: str, total_records: int):
        self.task_id = task_id
        self.total_records = total_records
        self.processed_records = 0
        self.matched_records = 0
        self.error_records = 0
        self.start_time = datetime.now()
        self.status = "running"  # running, completed, error, stopped
        self.lock = Lock()
    
    def update_progress(self, processed: int = 1, matched: int = 0, error: int = 0):
        """更新进度"""
        with self.lock:
            self.processed_records += processed
            self.matched_records += matched
            self.error_records += error
    
    def get_progress(self) -> Dict:
        """获取进度信息"""
        with self.lock:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            
            # 计算进度百分比
            progress_percent = (self.processed_records / self.total_records * 100) if self.total_records > 0 else 0
            
            # 估算剩余时间
            if self.processed_records > 0 and elapsed_time > 0:
                avg_time_per_record = elapsed_time / self.processed_records
                remaining_records = self.total_records - self.processed_records
                estimated_remaining_time = remaining_records * avg_time_per_record
            else:
                estimated_remaining_time = 0
            
            return {
                'task_id': self.task_id,
                'status': self.status,
                'total_records': self.total_records,
                'processed_records': self.processed_records,
                'matched_records': self.matched_records,
                'error_records': self.error_records,
                'progress_percent': round(progress_percent, 2),
                'elapsed_time': round(elapsed_time, 2),
                'estimated_remaining_time': round(estimated_remaining_time, 2),
                'match_rate': round((self.matched_records / self.processed_records * 100) if self.processed_records > 0 else 0, 2)
            }
    
    def set_status(self, status: str):
        """设置状态"""
        with self.lock:
            self.status = status


class MatchProcessor:
    """匹配处理器"""
    
    def __init__(self, db_manager, config: Dict):
        """
        初始化匹配处理器
        
        Args:
            db_manager: 数据库管理器
            config: 匹配配置
        """
        self.db_manager = db_manager
        self.config = config
        
        # 初始化匹配器
        self.exact_matcher = ExactMatcher(config)
        self.fuzzy_matcher = FuzzyMatcher(config)
        
        # 批处理配置
        self.batch_config = config.get('batch_processing', {})
        self.batch_size = self.batch_config.get('batch_size', 100)
        self.max_workers = self.batch_config.get('max_workers', 4)
        self.timeout = self.batch_config.get('timeout', 300)
        
        # 任务管理
        self.active_tasks = {}  # task_id -> MatchProgress
        self.tasks_lock = Lock()
        
    def start_matching_task(self, match_type: str = "both", batch_size: Optional[int] = None) -> str:
        """
        启动匹配任务
        
        Args:
            match_type: 匹配类型 ('exact', 'fuzzy', 'both')
            batch_size: 批处理大小
            
        Returns:
            str: 任务ID
        """
        # 生成任务ID
        task_id = str(uuid.uuid4())
        
        # 使用自定义批次大小
        if batch_size:
            self.batch_size = batch_size
        
        try:
            # 获取数据总数
            total_records = self.db_manager.get_collection_count('xxj_shdwjbxx')
            
            if total_records == 0:
                raise ValueError("源数据为空，无法进行匹配")
            
            # 创建进度跟踪器
            progress = MatchProgress(task_id, total_records)
            
            with self.tasks_lock:
                self.active_tasks[task_id] = progress
            
            # 启动异步任务
            from threading import Thread
            task_thread = Thread(
                target=self._execute_matching_task,
                args=(task_id, match_type),
                daemon=True
            )
            task_thread.start()
            
            logger.info(f"匹配任务启动成功: {task_id}, 类型: {match_type}, 总记录数: {total_records}")
            
            return task_id
            
        except Exception as e:
            logger.error(f"启动匹配任务失败: {str(e)}")
            raise
    
    def _execute_matching_task(self, task_id: str, match_type: str):
        """执行匹配任务"""
        progress = self.active_tasks.get(task_id)
        if not progress:
            return
        
        try:
            logger.info(f"开始执行匹配任务: {task_id}")
            
            # 获取目标数据（安全排查系统）
            target_records = self._load_target_records()
            logger.info(f"加载目标数据: {len(target_records)} 条")
            
            # 分批处理源数据
            skip = 0
            batch_count = 0
            
            while True:
                # 获取一批源数据
                source_batch = self.db_manager.get_supervision_units(
                    skip=skip, 
                    limit=self.batch_size
                )
                
                if not source_batch:
                    break
                
                batch_count += 1
                logger.info(f"处理第 {batch_count} 批数据: {len(source_batch)} 条")
                
                # 检查任务是否被停止
                if progress.status == "stopped":
                    logger.info(f"任务被停止: {task_id}")
                    return
                
                # 处理当前批次
                self._process_batch(task_id, source_batch, target_records, match_type)
                
                skip += self.batch_size
            
            # 任务完成
            progress.set_status("completed")
            logger.info(f"匹配任务完成: {task_id}")
            
        except Exception as e:
            logger.error(f"匹配任务执行失败 {task_id}: {str(e)}")
            progress.set_status("error")
    
    def _load_target_records(self) -> List[Dict]:
        """加载目标记录（安全排查系统）"""
        try:
            # 分批加载避免内存溢出
            all_records = []
            skip = 0
            
            while True:
                batch = self.db_manager.get_inspection_units(skip=skip, limit=1000)
                if not batch:
                    break
                
                all_records.extend(batch)
                skip += 1000
            
            return all_records
            
        except Exception as e:
            logger.error(f"加载目标记录失败: {str(e)}")
            return []
    
    def _process_batch(self, task_id: str, source_batch: List[Dict], 
                      target_records: List[Dict], match_type: str):
        """处理一批数据"""
        progress = self.active_tasks.get(task_id)
        if not progress:
            return
        
        try:
            # 并行处理
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for source_record in source_batch:
                    future = executor.submit(
                        self._process_single_record,
                        source_record, target_records, match_type
                    )
                    futures.append(future)
                
                # 收集结果
                for future in as_completed(futures, timeout=self.timeout):
                    try:
                        result = future.result()
                        
                        if result:
                            # 保存匹配结果
                            self._save_match_result(result)
                            progress.update_progress(processed=1, matched=1)
                        else:
                            progress.update_progress(processed=1)
                            
                    except Exception as e:
                        logger.error(f"处理记录失败: {str(e)}")
                        progress.update_progress(processed=1, error=1)
                        
        except Exception as e:
            logger.error(f"批处理失败: {str(e)}")
            # 更新失败记录数
            progress.update_progress(processed=len(source_batch), error=len(source_batch))
    
    def _process_single_record(self, source_record: Dict, target_records: List[Dict], 
                              match_type: str) -> Optional[Dict]:
        """处理单条记录"""
        try:
            result = None
            
            # 精确匹配
            if match_type in ['exact', 'both']:
                exact_result = self.exact_matcher.match_single_record(source_record, target_records)
                
                if exact_result.matched:
                    result = self._format_match_result(exact_result, 'exact')
                    logger.debug(f"精确匹配成功: {source_record.get('dwmc', 'Unknown')}")
                    return result
            
            # 模糊匹配（仅在精确匹配失败或指定模糊匹配时进行）
            if match_type in ['fuzzy', 'both'] and not result:
                fuzzy_result = self.fuzzy_matcher.match_single_record(source_record, target_records)
                
                if fuzzy_result.matched:
                    result = self._format_match_result(fuzzy_result, 'fuzzy')
                    logger.debug(f"模糊匹配成功: {source_record.get('dwmc', 'Unknown')}")
                    return result
            
            # 无匹配结果时，记录未匹配
            if not result:
                result = self._format_no_match_result(source_record)
                logger.debug(f"未找到匹配: {source_record.get('dwmc', 'Unknown')}")
            
            return result
            
        except Exception as e:
            logger.error(f"处理单条记录失败: {str(e)}")
            return None
    
    def _format_match_result(self, match_result: Union[MatchResult, FuzzyMatchResult], 
                           match_type: str) -> Dict:
        """格式化匹配结果"""
        source_record = match_result.source_record
        target_record = match_result.target_record
        
        # 生成组合单位名称
        source_name = source_record.get('dwmc', '')
        target_name = target_record.get('UNIT_NAME', '') if target_record else ''
        
        if source_name == target_name:
            combined_name = source_name
        else:
            combined_name = f"{target_name}（{source_name}）" if target_name and source_name else (target_name or source_name)
        
        # 基础结果
        result = {
            'match_id': generate_match_id(
                str(source_record.get('_id', '')), 
                str(target_record.get('_id', '')) if target_record else ''
            ),
            'primary_unit_id': target_record.get('_id') if target_record else None,
            'primary_unit_name': target_name,
            'matched_unit_id': source_record.get('_id'),
            'matched_unit_name': source_name,
            'combined_unit_name': combined_name,
            'match_type': match_type,
            'status': 'matched',
            'created_time': datetime.now()
        }
        
        # 添加匹配详情
        if match_type == 'exact':
            result.update({
                'similarity_score': match_result.confidence,
                'match_details': match_result.match_details or {}
            })
        elif match_type == 'fuzzy':
            result.update({
                'similarity_score': match_result.similarity_score,
                'match_details': {
                    'field_similarities': match_result.field_similarities or {},
                    'threshold': match_result.match_details.get('threshold', 0.75) if match_result.match_details else 0.75
                }
            })
        
        return result
    
    def _format_no_match_result(self, source_record: Dict) -> Dict:
        """格式化无匹配结果"""
        return {
            'match_id': generate_match_id(str(source_record.get('_id', '')), 'no_match'),
            'primary_unit_id': None,
            'primary_unit_name': None,
            'matched_unit_id': source_record.get('_id'),
            'matched_unit_name': source_record.get('dwmc', ''),
            'combined_unit_name': source_record.get('dwmc', ''),
            'match_type': 'none',
            'similarity_score': 0.0,
            'status': 'unmatched',
            'match_details': {},
            'created_time': datetime.now()
        }
    
    def _save_match_result(self, result: Dict) -> bool:
        """保存匹配结果"""
        try:
            return self.db_manager.save_match_result(result)
        except Exception as e:
            logger.error(f"保存匹配结果失败: {str(e)}")
            return False
    
    def get_task_progress(self, task_id: str) -> Dict:
        """
        获取任务进度
        
        Args:
            task_id: 任务ID
            
        Returns:
            Dict: 进度信息
        """
        with self.tasks_lock:
            progress = self.active_tasks.get(task_id)
            
            if not progress:
                return {
                    'error': '任务不存在',
                    'task_id': task_id
                }
            
            return progress.get_progress()
    
    def stop_matching_task(self, task_id: str) -> bool:
        """
        停止匹配任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否成功停止
        """
        try:
            with self.tasks_lock:
                progress = self.active_tasks.get(task_id)
                
                if not progress:
                    return False
                
                if progress.status == "running":
                    progress.set_status("stopped")
                    logger.info(f"匹配任务已停止: {task_id}")
                    return True
                else:
                    return False
                    
        except Exception as e:
            logger.error(f"停止匹配任务失败: {str(e)}")
            return False
    
    def get_all_tasks(self) -> List[Dict]:
        """获取所有任务状态"""
        with self.tasks_lock:
            return [progress.get_progress() for progress in self.active_tasks.values()]
    
    def cleanup_completed_tasks(self, max_age_hours: int = 24):
        """清理已完成的任务"""
        try:
            current_time = datetime.now()
            to_remove = []
            
            with self.tasks_lock:
                for task_id, progress in self.active_tasks.items():
                    if progress.status in ['completed', 'error', 'stopped']:
                        elapsed_hours = (current_time - progress.start_time).total_seconds() / 3600
                        if elapsed_hours > max_age_hours:
                            to_remove.append(task_id)
                
                for task_id in to_remove:
                    del self.active_tasks[task_id]
                    logger.info(f"清理任务: {task_id}")
                    
        except Exception as e:
            logger.error(f"清理任务失败: {str(e)}")
    
    def get_matching_statistics(self) -> Dict:
        """获取匹配统计信息"""
        try:
            # 从数据库获取统计信息
            db_stats = self.db_manager.get_match_statistics()
            
            # 添加任务统计
            with self.tasks_lock:
                task_stats = {
                    'active_tasks': len([p for p in self.active_tasks.values() if p.status == 'running']),
                    'completed_tasks': len([p for p in self.active_tasks.values() if p.status == 'completed']),
                    'total_tasks': len(self.active_tasks)
                }
            
            return {
                'database_stats': db_stats,
                'task_stats': task_stats,
                'last_updated': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"获取匹配统计失败: {str(e)}")
            return {
                'database_stats': {},
                'task_stats': {},
                'error': str(e)
            } 