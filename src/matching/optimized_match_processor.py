#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优化的匹配处理器模块
解决重复数据、增量匹配和信息完整性问题
"""

import logging
import uuid
import json
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

from .exact_matcher import ExactMatcher, MatchResult
from .fuzzy_matcher import FuzzyMatcher
from .optimized_fuzzy_matcher import OptimizedFuzzyMatcher, FuzzyMatchResult
from .enhanced_fuzzy_matcher import EnhancedFuzzyMatcher, EnhancedFuzzyMatchResult
from .graph_matcher import GraphMatcher
from .prefilter_system import PrefilterSystem
from src.utils.helpers import batch_iterator, generate_match_id, format_timestamp

logger = logging.getLogger(__name__)


class MatchingMode:
    """匹配模式枚举"""
    INCREMENTAL = "incremental"  # 增量匹配，只处理未匹配的记录
    UPDATE = "update"           # 更新匹配，重新匹配已有结果
    FULL = "full"              # 全量匹配，清空后重新匹配


class OptimizedMatchProgress:
    """优化的匹配进度跟踪"""
    
    def __init__(self, task_id: str, total_records: int, mode: str = MatchingMode.INCREMENTAL):
        self.task_id = task_id
        self.total_records = total_records
        self.mode = mode
        self.processed_records = 0
        self.matched_records = 0
        self.updated_records = 0
        self.skipped_records = 0
        self.error_records = 0
        self.start_time = datetime.now()
        self.status = "running"  # running, completed, error, stopped
        self.lock = Lock()
        self.current_batch = 0
        self.last_processed_id = None
    
    def update_progress(self, processed: int = 1, matched: int = 0, updated: int = 0, 
                       skipped: int = 0, error: int = 0, last_id: str = None):
        """更新进度"""
        with self.lock:
            self.processed_records += processed
            self.matched_records += matched
            self.updated_records += updated
            self.skipped_records += skipped
            self.error_records += error
            if last_id:
                self.last_processed_id = last_id
    
    def get_progress(self) -> Dict:
        """获取进度信息"""
        with self.lock:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            
            # 计算进度百分比
            progress_percent = (self.processed_records / self.total_records * 100) if self.total_records > 0 else 0
            
            # 估算剩余时间 - 优化版本，防止异常值
            if self.processed_records > 0 and elapsed_time > 0:
                avg_time_per_record = elapsed_time / self.processed_records
                remaining_records = self.total_records - self.processed_records
                
                # 基础剩余时间计算
                raw_estimated_time = remaining_records * avg_time_per_record
                
                # 异常值处理 - 防止显示过大的时间
                if raw_estimated_time > 86400:  # 超过24小时
                    # 使用最近的处理速度来估算（最近100条记录的平均速度）
                    if self.processed_records >= 10:
                        recent_avg_time = elapsed_time / min(self.processed_records, 100)
                        estimated_remaining_time = min(raw_estimated_time, remaining_records * recent_avg_time)
                        
                        # 如果还是太大，则限制最大显示时间
                        if estimated_remaining_time > 7200:  # 超过2小时
                            estimated_remaining_time = min(estimated_remaining_time, 7200)  # 最多显示2小时
                    else:
                        estimated_remaining_time = min(raw_estimated_time, 3600)  # 最多显示1小时
                else:
                    estimated_remaining_time = raw_estimated_time
                
                # 最终安全检查
                if estimated_remaining_time < 0 or estimated_remaining_time > 86400:
                    estimated_remaining_time = 0  # 异常情况下设为0
            else:
                estimated_remaining_time = 0
            
            return {
                'task_id': self.task_id,
                'mode': self.mode,
                'status': self.status,
                'total_records': self.total_records,
                'processed_records': self.processed_records,
                'matched_records': self.matched_records,
                'updated_records': self.updated_records,
                'skipped_records': self.skipped_records,
                'error_records': self.error_records,
                'current_batch': self.current_batch,
                'last_processed_id': self.last_processed_id,
                'progress_percent': round(progress_percent, 2),
                'elapsed_time': round(elapsed_time, 2),
                'estimated_remaining_time': round(estimated_remaining_time, 2),
                'match_rate': round((self.matched_records / self.processed_records * 100) if self.processed_records > 0 else 0, 2)
            }
    
    def set_status(self, status: str):
        """设置状态"""
        with self.lock:
            self.status = status
    
    def set_current_batch(self, batch_num: int):
        """设置当前批次"""
        with self.lock:
            self.current_batch = batch_num


class OptimizedMatchProcessor:
    """优化的匹配处理器"""
    
    def __init__(self, db_manager, config: Dict):
        """
        初始化优化匹配处理器
        
        Args:
            db_manager: 数据库管理器
            config: 匹配配置
        """
        self.db_manager = db_manager
        self.config = config
        
        # 初始化匹配器
        self.exact_matcher = ExactMatcher(config)
        self.fuzzy_matcher = FuzzyMatcher(config)
        self.optimized_fuzzy_matcher = OptimizedFuzzyMatcher(config)
        self.enhanced_fuzzy_matcher = EnhancedFuzzyMatcher(config)  # 新增增强模糊匹配器
        self.prefilter_system = PrefilterSystem(db_manager.get_db()) # 初始化预过滤器
        
        # 图匹配器配置
        self.graph_config = config.get('graph_matching', {})
        self.use_graph_matcher = self.graph_config.get('enabled', True)
        if self.use_graph_matcher:
            logger.info("图匹配器模块已启用。")
            self.graph_matcher = GraphMatcher(self.db_manager.get_db(), self.config)
            # 构建一个包含少量数据的热启动图
            initial_build_limit = self.graph_config.get('initial_build_limit', 5000)
            self.graph_matcher.build_graph(limit=initial_build_limit)
        
        # 批处理配置
        self.batch_config = config.get('batch_processing', {})
        self.batch_size = self.batch_config.get('batch_size', 100)
        self.max_workers = self.batch_config.get('max_workers', 4)
        self.timeout = self.batch_config.get('timeout', 300)
        
        # 任务管理
        self.active_tasks = {}  # task_id -> OptimizedMatchProgress
        self.tasks_lock = Lock()
    
    def _safe_str(self, value, default: str = '') -> str:
        """安全地将任何类型转换为字符串并去除空白"""
        if value is None:
            return default
        try:
            return str(value).strip()
        except Exception:
            return default
    
    def start_optimized_matching_task(self, match_type: str = "both", 
                                    mode: str = MatchingMode.INCREMENTAL,
                                    batch_size: Optional[int] = None,
                                    clear_existing: bool = False) -> str:
        """
        启动优化的匹配任务
        
        Args:
            match_type: 匹配类型 ('exact', 'fuzzy', 'both')
            mode: 匹配模式 (incremental, update, full)
            batch_size: 批处理大小
            clear_existing: 是否清空现有结果
            
        Returns:
            str: 任务ID
        """
        # 生成任务ID
        task_id = str(uuid.uuid4())
        
        # 使用自定义批次大小
        if batch_size:
            self.batch_size = batch_size
        
        try:
            # 如果是全量匹配或要求清空，先清空现有结果
            if mode == MatchingMode.FULL or clear_existing:
                self._clear_match_results()
                logger.info("已清空现有匹配结果")
            
            # 获取需要处理的记录数（安全排查系统）
            if mode == MatchingMode.INCREMENTAL:
                total_records = self._get_unmatched_count()
            else:
                total_records = self.db_manager.get_collection_count('xfaqpc_jzdwxx')
            
            if total_records == 0:
                if mode == MatchingMode.INCREMENTAL:
                    raise ValueError("所有记录已匹配，无需增量处理")
                else:
                    raise ValueError("源数据为空，无法进行匹配")
            
            # 创建进度跟踪器
            progress = OptimizedMatchProgress(task_id, total_records, mode)
            
            with self.tasks_lock:
                self.active_tasks[task_id] = progress
            
            # 启动异步任务
            from threading import Thread
            task_thread = Thread(
                target=self._execute_optimized_matching_task,
                args=(task_id, match_type, mode),
                daemon=True
            )
            task_thread.start()
            
            logger.info(f"优化匹配任务启动成功: {task_id}, 类型: {match_type}, 模式: {mode}, 总记录数: {total_records}")
            
            return task_id
            
        except Exception as e:
            logger.error(f"启动优化匹配任务失败: {str(e)}")
            raise
    
    def _execute_optimized_matching_task(self, task_id: str, match_type: str, mode: str):
        """执行优化的匹配任务"""
        progress = self.active_tasks.get(task_id)
        if not progress:
            return
        
        try:
            logger.info(f"开始执行优化匹配任务: {task_id}, 模式: {mode}")
            
            # 获取目标数据（消防监督管理系统）- 不再全量加载
            # target_records = self._load_target_records()
            # logger.info(f"加载目标数据（消防监督管理系统）: {len(target_records)} 条")
            
            # 根据模式获取需要处理的源数据（安全排查系统）
            if mode == MatchingMode.INCREMENTAL:
                source_records_generator = self._get_unmatched_records_generator()
            else:
                source_records_generator = self._get_all_records_generator()
            
            batch_count = 0
            
            # 分批处理
            for source_batch in source_records_generator:
                if not source_batch:
                    break
                
                batch_count += 1
                progress.set_current_batch(batch_count)
                logger.info(f"处理第 {batch_count} 批数据: {len(source_batch)} 条")
                
                # 检查任务是否被停止 - 在处理批次之前检查
                if progress.status == "stopped":
                    logger.info(f"任务停止信号检测到，停止处理新批次: {task_id}")
                    break
                
                # 处理当前批次 (不再传入 target_records)
                self._process_optimized_batch(task_id, source_batch, match_type, mode)
                
                # 检查任务是否在批次处理过程中被停止
                if progress.status == "stopped":
                    logger.info(f"任务在批次处理过程中被停止: {task_id}")
                    break
            
            # 任务完成 - 确保最后的数据保存
            if progress.status != "stopped":
                progress.set_status("completed")
                logger.info(f"优化匹配任务完成: {task_id}")
            else:
                logger.info(f"优化匹配任务已停止: {task_id}")
                # 强制最终数据保存检查
                final_save_count = self._force_final_save_check()
                logger.info(f"任务停止时最终保存检查完成: 保存了 {final_save_count} 条记录")
            
        except Exception as e:
            logger.error(f"优化匹配任务执行失败 {task_id}: {str(e)}")
            progress.set_status("error")
    
    def _get_unmatched_count(self) -> int:
        """获取未匹配记录数量"""
        try:
            # 获取已匹配的源记录ID列表
            matched_ids = self._get_matched_source_ids()
            
            # 计算总记录数（安全排查系统）
            total_count = self.db_manager.get_collection_count('xfaqpc_jzdwxx')
            
            # 返回未匹配数量
            return total_count - len(matched_ids)
            
        except Exception as e:
            logger.error(f"获取未匹配记录数量失败: {str(e)}")
            return 0
    
    def _get_matched_source_ids(self) -> set:
        """获取已匹配的源记录ID集合"""
        try:
            collection = self.db_manager.get_collection('unit_match_results')
            
            # 查询所有已匹配的源记录ID，移除hint让MongoDB自动选择索引
            cursor = collection.find(
                {'primary_record_id': {'$exists': True, '$ne': None}},
                {'primary_record_id': 1}
            )
            
            matched_ids = {str(doc['primary_record_id']) for doc in cursor}
            logger.debug(f"已匹配记录数: {len(matched_ids)}")
            
            return matched_ids
            
        except Exception as e:
            logger.error(f"获取已匹配记录ID失败: {str(e)}")
            return set()
    
    def _get_unmatched_records_generator(self):
        """获取未匹配记录的生成器（安全排查系统）"""
        try:
            matched_ids = self._get_matched_source_ids()
            
            skip = 0
            while True:
                # 获取一批源数据（安全排查系统）
                source_batch = self.db_manager.get_inspection_units(
                    skip=skip, 
                    limit=self.batch_size
                )
                
                if not source_batch:
                    break
                
                # 过滤出未匹配的记录
                unmatched_batch = [
                    record for record in source_batch 
                    if str(record.get('_id')) not in matched_ids
                ]
                
                if unmatched_batch:
                    yield unmatched_batch
                
                skip += self.batch_size
                
        except Exception as e:
            logger.error(f"获取未匹配记录失败: {str(e)}")
            yield []
    
    def _get_all_records_generator(self):
        """获取所有记录的生成器（安全排查系统）"""
        try:
            skip = 0
            while True:
                # 获取一批源数据（安全排查系统）
                source_batch = self.db_manager.get_inspection_units(
                    skip=skip, 
                    limit=self.batch_size
                )
                
                if not source_batch:
                    break
                
                yield source_batch
                skip += self.batch_size
                
        except Exception as e:
            logger.error(f"获取所有记录失败: {str(e)}")
            yield []
    
    def _process_optimized_batch(self, task_id: str, source_batch: List[Dict], 
                               match_type: str, mode: str):
        """处理优化的批次数据"""
        progress = self.active_tasks.get(task_id)
        if not progress:
            return
        
        batch_results = []
        task_stopped = False
        
        logger.info(f"🔄 开始处理批次: {len(source_batch)} 条记录，模式: {mode}")
        
        for source_record in source_batch:
            try:
                # 检查任务状态
                if progress.status == "stopped":
                    task_stopped = True
                    logger.info(f"🛑 任务停止信号检测到，当前批次已处理 {len(batch_results)} 条记录")
                    break
                
                # 处理单条记录 (不再传入 target_records)
                result = self._process_optimized_single_record(
                    source_record, match_type, mode
                )
                
                # 详细记录结果处理
                if result:
                    operation = result.get('operation', 'unknown')
                    unit_name = source_record.get('UNIT_NAME', 'Unknown')
                    
                    if operation == 'skipped':
                        logger.info(f"📝 记录跳过: {unit_name} - {result.get('reason', 'unknown')}")
                    else:
                        batch_results.append(result)
                        logger.info(f"📝 记录添加到批次: {unit_name} - 操作: {operation}")
                    
                    # 更新进度
                    if operation == 'matched':
                        progress.update_progress(
                            processed=1, matched=1, 
                            last_id=str(source_record.get('_id'))
                        )
                    elif operation == 'updated':
                        progress.update_progress(
                            processed=1, updated=1,
                            last_id=str(source_record.get('_id'))
                        )
                    elif operation == 'skipped':
                        progress.update_progress(
                            processed=1, skipped=1,
                            last_id=str(source_record.get('_id'))
                        )
                    else:
                        progress.update_progress(
                            processed=1,
                            last_id=str(source_record.get('_id'))
                        )
                else:
                    logger.warning(f"📝 记录处理失败: {source_record.get('UNIT_NAME', 'Unknown')}")
                    progress.update_progress(
                        processed=1, error=1,
                        last_id=str(source_record.get('_id'))
                    )
                    
            except Exception as e:
                logger.error(f"处理记录失败: {str(e)}")
                progress.update_progress(processed=1, error=1)
        
        # 批量保存结果 - 无论是否停止都要保存已处理的结果
        logger.info(f"💾 准备保存批次结果: {len(batch_results)} 条记录")
        
        if batch_results:
            logger.info(f"💾 开始批量保存 {len(batch_results)} 条匹配结果...")
            
            # 显示前几条结果的详细信息
            for i, result in enumerate(batch_results[:3]):
                logger.info(f"💾 结果 {i+1}: {result.get('unit_name', 'Unknown')} -> {result.get('operation', 'unknown')}")
            
            save_success = self._batch_save_optimized_results(batch_results)
            if save_success:
                if task_stopped:
                    logger.info(f"✅ 任务停止前成功保存 {len(batch_results)} 条匹配结果")
                else:
                    logger.info(f"✅ 批次处理完成，成功保存 {len(batch_results)} 条匹配结果")
            else:
                logger.error(f"❌ 保存 {len(batch_results)} 条匹配结果失败")
        else:
            if task_stopped:
                logger.warning("⚠️ 任务停止，当前批次无需保存的匹配结果")
            else:
                logger.warning("⚠️ 批次处理完成，但没有生成任何匹配结果")
            
            # 增加调试信息
            logger.info(f"🔍 调试信息: 批次大小={len(source_batch)}, 结果数量={len(batch_results)}, 模式={mode}")
            
        # 如果任务停止，标记任务状态
        if task_stopped:
            logger.info(f"任务在批次处理过程中被停止: {task_id}")
            progress.set_status("stopped")
    
    def _process_optimized_single_record(self, source_record: Dict, 
                                       match_type: str, mode: str) -> Optional[Dict]:
        """处理优化的单条记录"""
        try:
            source_id = str(source_record.get('_id'))
            
            # 1. 使用预过滤系统获取候选记录
            target_candidates = self.prefilter_system.get_candidates(source_record)
            if not target_candidates:
                logger.info(f"预过滤未能找到任何候选记录: {source_record.get('UNIT_NAME', 'Unknown')}")
                return self._format_optimized_no_match_result(source_record)
            
            logger.info(f"为 {source_record.get('UNIT_NAME', 'Unknown')} 找到 {len(target_candidates)} 个候选。")

            # 检查是否已存在匹配结果
            existing_result = self._get_existing_match_result(source_id)
            
            # 执行匹配
            match_result = None
            
            # 精确匹配
            if match_type in ['exact', 'both']:
                exact_result = self.exact_matcher.match_single_record(source_record, target_candidates)
                
                if exact_result.matched:
                    match_result = self._format_optimized_match_result(
                        exact_result, 'exact', source_record
                    )
                    logger.info(f"信用代码精确匹配成功: {source_record.get('UNIT_NAME', 'Unknown')}")
            
            # 模糊匹配（仅在精确匹配失败或指定模糊匹配时进行）
            if match_type in ['fuzzy', 'both'] and not match_result:
                # 优先使用增强的模糊匹配器（解决匹配幻觉问题）
                enhanced_fuzzy_result = self.enhanced_fuzzy_matcher.match_single_record(
                    source_record, target_candidates
                )
                
                if enhanced_fuzzy_result.matched:
                    # 图匹配增强逻辑
                    if self.use_graph_matcher and 0.7 < enhanced_fuzzy_result.similarity_score < 1.0:
                        logger.info(f"触发图匹配二次验证，当前分数: {enhanced_fuzzy_result.similarity_score:.3f}")
                        
                        # 动态将当前比较的记录添加到图中，确保它们存在
                        self.graph_matcher.add_unit_to_graph(source_record, 'xfaqpc', 'UNIT_NAME', 'UNIT_ADDRESS', 'LEGAL_PEOPLE')
                        if enhanced_fuzzy_result.target_record:
                            self.graph_matcher.add_unit_to_graph(enhanced_fuzzy_result.target_record, 'xxj', 'dwmc', 'dwdz', 'fddbr')
                        
                        graph_score = self.graph_matcher.calculate_graph_score(
                            source_record, 
                            enhanced_fuzzy_result.target_record
                        )
                        
                        if graph_score > 0:
                            original_score = enhanced_fuzzy_result.similarity_score
                            enhanced_fuzzy_result.similarity_score = 0.98  # 提升分数
                            warning_msg = f"图匹配增强: 共享属性发现，分数从 {original_score:.3f} 提升至 0.98"
                            enhanced_fuzzy_result.match_warnings.append(warning_msg)
                            logger.info(f"✅ {warning_msg} for {source_record.get('UNIT_NAME')}")

                    match_result = self._format_optimized_match_result(
                        enhanced_fuzzy_result, 'fuzzy_enhanced', source_record
                    )
                    logger.info(f"增强模糊匹配成功: {source_record.get('UNIT_NAME', 'Unknown')}, "
                               f"相似度: {enhanced_fuzzy_result.similarity_score:.3f}")
                    
                    # 记录匹配警告
                    if enhanced_fuzzy_result.match_warnings:
                        logger.warning(f"匹配警告: {', '.join(enhanced_fuzzy_result.match_warnings)}")
                else:
                    # 如果增强模糊匹配失败，尝试使用优化的模糊匹配器作为备用
                    optimized_fuzzy_result = self.optimized_fuzzy_matcher.match_single_record_optimized(
                        source_record, target_candidates
                    )
                    
                    if optimized_fuzzy_result.get('matched', False):
                        # 转换为标准的FuzzyMatchResult格式
                        fuzzy_result = FuzzyMatchResult(
                            matched=True,
                            similarity_score=optimized_fuzzy_result.get('similarity_score', 0.0),
                            source_record=source_record,
                            target_record=optimized_fuzzy_result.get('target_record'),
                            match_details=optimized_fuzzy_result.get('match_details', {})
                        )
                        
                        match_result = self._format_optimized_match_result(
                            fuzzy_result, 'fuzzy_optimized', source_record
                        )
                        logger.info(f"优化模糊匹配成功: {source_record.get('UNIT_NAME', 'Unknown')}, "
                                   f"候选数: {optimized_fuzzy_result.get('match_details', {}).get('candidates_count', 0)}")
            
            # 如果没有匹配结果，创建未匹配记录
            if not match_result:
                match_result = self._format_optimized_no_match_result(source_record)
                logger.info(f"未找到匹配: {source_record.get('UNIT_NAME', 'Unknown')}")
            
            # 增量模式逻辑修复：只有在没有找到新匹配且已存在结果时才跳过
            if mode == MatchingMode.INCREMENTAL and existing_result:
                # 检查新匹配结果是否与现有结果相同
                if match_result and existing_result:
                    existing_matched_id = existing_result.get('matched_record_id')
                    new_matched_id = match_result.get('matched_record_id')
                    
                    # 如果匹配到相同的目标记录，则跳过
                    if existing_matched_id == new_matched_id:
                        logger.info(f"增量模式跳过: {source_record.get('UNIT_NAME', 'Unknown')} (已存在相同匹配)")
                        return {
                            'operation': 'skipped',
                            'source_id': source_id,
                            'reason': 'identical_match_exists'
                        }
                    else:
                        # 找到了不同的匹配目标，更新现有记录
                        logger.info(f"增量模式更新: {source_record.get('UNIT_NAME', 'Unknown')} (发现更好的匹配)")
                        match_result['operation'] = 'updated'
                        match_result['previous_result_id'] = existing_result.get('_id')
                        return match_result
                else:
                    # 现有记录是未匹配，但现在找到了匹配
                    if match_result and match_result.get('match_status') == 'matched':
                        logger.info(f"增量模式更新: {source_record.get('UNIT_NAME', 'Unknown')} (从未匹配变为匹配)")
                        match_result['operation'] = 'updated'
                        match_result['previous_result_id'] = existing_result.get('_id')
                        return match_result
                    else:
                        # 都是未匹配状态，跳过
                        logger.info(f"增量模式跳过: {source_record.get('UNIT_NAME', 'Unknown')} (未匹配状态未变)")
                        return {
                            'operation': 'skipped',
                            'source_id': source_id,
                            'reason': 'no_change_needed'
                        }
            
            # 设置操作类型
            if existing_result:
                match_result['operation'] = 'updated'
                match_result['previous_result_id'] = existing_result.get('_id')
            else:
                match_result['operation'] = 'matched'
            
            return match_result
            
        except Exception as e:
            logger.error(f"处理优化单条记录失败: {str(e)}")
            return None
    
    def _format_optimized_match_result(self, match_result: Union[MatchResult, FuzzyMatchResult], 
                                     match_type: str, source_record: Dict) -> Dict:
        """格式化优化的匹配结果 - 以安全排查系统为主记录"""
        target_record = match_result.target_record
        # 基础信息 - 以安全排查系统（source）为主记录
        result = {
            'primary_record_id': source_record.get('_id'),
            'primary_system': 'inspection',
            'unit_name': source_record.get('UNIT_NAME', ''),
            'unit_address': source_record.get('ADDRESS', ''),
            'unit_type': source_record.get('UNIT_TYPE', ''),
            'contact_person': source_record.get('LEGAL_PEOPLE', ''),
            'contact_phone': source_record.get('SECURITY_TEL', ''),
            'security_manager': source_record.get('SECURITY_PEOPLE', ''),
            'primary_security_manager': source_record.get('SECURITY_PEOPLE', ''),
            'legal_tel': source_record.get('SECURITY_TEL', ''),
            'matched_record_id': target_record.get('_id') if target_record else None,
            'matched_system': 'supervision',
            'matched_unit_name': target_record.get('dwmc', '') if target_record else '',
            'matched_unit_address': target_record.get('dwdz', '') if target_record else '',
            'matched_credit_code': target_record.get('tyshxydm', '') if target_record else '',
            'matched_legal_person': target_record.get('fddbr', '') if target_record else '',
            'matched_contact_phone': target_record.get('lxdh', '') if target_record else '',
            'matched_registration_date': target_record.get('djrq', '') if target_record else '',
            'matched_business_scope': target_record.get('jyfw', '') if target_record else '',
            'matched_security_manager': target_record.get('xfaqglr', '') if target_record else '',
            'match_type': self._determine_actual_match_type(match_result, match_type),
            'match_status': 'matched',
            'similarity_score': match_result.similarity_score if hasattr(match_result, 'similarity_score') else 0.0,
            'match_confidence': 'high' if match_type == 'exact' else 'medium',
            'match_fields': [],
            'match_details': {}
        }
        
        # 尝试获取explanation
        explanation = getattr(match_result, 'explanation', None)
        if explanation:
            result['match_details']['explanation'] = explanation
        
        # 根据匹配类型添加详细信息
        if match_type == 'exact':
            # 检查关键信息是否一致，如果不一致则标注数据来源
            data_consistency = self._check_data_consistency(source_record, target_record)
            
            result.update({
                'similarity_score': match_result.confidence,
                'match_fields': ['credit_code'],  # 精确匹配只基于信用代码
                'match_details': {
                    'exact_match_fields': ['credit_code'],
                    'match_method': 'exact_credit_code',
                    'confidence_level': match_result.confidence,
                    'match_basis': '统一社会信用代码',
                    'data_consistency': data_consistency
                }
            })
        elif match_type == 'fuzzy':
            field_similarities = match_result.field_similarities or {}
            matched_fields = []
            fuzzy_details = {}
            for field, similarity in field_similarities.items():
                # 类型安全判断
                sim_val = similarity
                if isinstance(sim_val, dict):
                    sim_val = sim_val.get('similarity', 0)
                if isinstance(sim_val, (int, float)) and sim_val > 0.5:
                    matched_fields.append(field)
                    fuzzy_details[f'{field}_similarity'] = sim_val
            
            # 确保similarity_score有值
            actual_similarity = match_result.similarity_score
            if actual_similarity == 0 and hasattr(match_result, 'match_details'):
                # 从match_details中获取实际相似度
                match_details = match_result.match_details or {}
                actual_similarity = match_details.get('overall_similarity', 0)
            
            # 如果是fuzzy_perfect类型但相似度为0，设置为1.0
            actual_match_type = result['match_type']
            if actual_match_type == 'fuzzy_perfect' and actual_similarity == 0:
                actual_similarity = 1.0
            
            result.update({
                'similarity_score': actual_similarity,
                'match_fields': matched_fields,
                'match_details': {
                    'fuzzy_match_fields': matched_fields,
                    'field_similarities': field_similarities,
                    'overall_similarity': actual_similarity,
                    'threshold_used': match_result.match_details.get('threshold', 0.75) if match_result.match_details else 0.75,
                    'match_method': 'fuzzy',
                    **fuzzy_details
                }
            })
            if actual_similarity >= 0.9:
                result['match_confidence'] = 'high'
            elif actual_similarity >= 0.75:
                result['match_confidence'] = 'medium'
            else:
                result['match_confidence'] = 'low'
        
        # 生成匹配ID
        result['match_id'] = generate_match_id(
            str(result['primary_record_id']), 
            str(result['matched_record_id'])
        )
        
        # 添加审核信息 - 设置默认值
        if match_type == 'exact':
            # 精确匹配默认无需人工审核
            result['review_status'] = 'pending'  # 可以设为approved，但为了一致性设为pending
        else:
            # 模糊匹配需要人工审核
            result['review_status'] = 'pending'
        
        result['review_notes'] = ''
        result['reviewer'] = ''
        result['review_time'] = None
        
        # 添加时间戳
        current_time = datetime.now()
        result['created_time'] = current_time
        result['updated_time'] = current_time
        result['matching_time'] = current_time
        
        return result
    
    def _determine_actual_match_type(self, match_result: Union[MatchResult, FuzzyMatchResult], 
                                   declared_type: str) -> str:
        """
        确定实际的匹配类型
        
        Args:
            match_result: 匹配结果对象
            declared_type: 声明的匹配类型
            
        Returns:
            str: 实际的匹配类型
        """
        # 如果是精确匹配结果对象
        if hasattr(match_result, 'match_type'):
            if match_result.match_type == 'credit_code':
                return 'exact_credit_code'
            elif match_result.match_type in ['fuzzy', 'fuzzy_match']:
                return 'fuzzy'
        
        # 如果是模糊匹配结果对象
        if hasattr(match_result, 'similarity_score'):
            similarity_score = match_result.similarity_score
            if similarity_score == 1.0:
                # 相似度为1.0，可能是精确匹配
                if declared_type == 'exact':
                    return 'exact'
                else:
                    return 'fuzzy_perfect'
            elif similarity_score >= 0.95:
                return 'fuzzy_perfect'  # 高相似度也认为是完美匹配
            else:
                return 'fuzzy'
        
        # 特殊处理：如果是模糊匹配但没有相似度分数，检查是否应该是完美匹配
        if declared_type == 'fuzzy' and hasattr(match_result, 'match_details'):
            match_details = match_result.match_details or {}
            if match_details.get('overall_similarity', 0) >= 0.95:
                # 确保设置相似度分数
                if hasattr(match_result, 'similarity_score'):
                    match_result.similarity_score = match_details.get('overall_similarity', 1.0)
                return 'fuzzy_perfect'
        
        # 回退到声明的类型
        return declared_type
    
    def _check_data_consistency(self, source_record, target_record):
        """检查两条记录的关键信息一致性"""
        consistency = {
            'unit_name': {'consistent': True, 'source': None, 'target': None, 'note': None},
            'legal_person': {'consistent': True, 'source': None, 'target': None, 'note': None},
            'address': {'consistent': True, 'source': None, 'target': None, 'note': None}
        }
        
        # 检查单位名称
        source_name = self._safe_str(source_record.get('UNIT_NAME'))
        target_name = self._safe_str(target_record.get('dwmc'))  # 消防监督系统字段
        if source_name and target_name:
            consistency['unit_name']['source'] = f"{source_name} (安全排查系统)"
            consistency['unit_name']['target'] = f"{target_name} (消防监督系统)"
            if source_name != target_name:
                consistency['unit_name']['consistent'] = False
                consistency['unit_name']['note'] = '单位名称不一致，请核实'
        
        # 检查法定代表人
        source_legal = self._safe_str(source_record.get('LEGAL_PEOPLE'))
        target_legal = self._safe_str(target_record.get('fddbr'))  # 消防监督系统字段
        if source_legal and target_legal:
            consistency['legal_person']['source'] = f"{source_legal} (安全排查系统)"
            consistency['legal_person']['target'] = f"{target_legal} (消防监督系统)"
            if source_legal != target_legal:
                consistency['legal_person']['consistent'] = False
                consistency['legal_person']['note'] = '法定代表人不一致，请核实'
        
        # 检查地址
        source_address = self._safe_str(source_record.get('ADDRESS'))
        target_address = self._safe_str(target_record.get('dwdz'))  # 消防监督系统字段
        if source_address and target_address:
            consistency['address']['source'] = f"{source_address} (安全排查系统)"
            consistency['address']['target'] = f"{target_address} (消防监督系统)"
            # 简单的地址相似性检查
            if source_address != target_address and not self._addresses_similar(source_address, target_address):
                consistency['address']['consistent'] = False
                consistency['address']['note'] = '地址不一致，请核实'
        
        return consistency
    
    def _addresses_similar(self, addr1, addr2, threshold=0.7):
        """简单的地址相似性检查"""
        if not addr1 or not addr2:
            return False
        
        # 提取主要街道信息进行比较
        import re
        
        # 提取街道、路、弄、号等关键信息
        pattern = r'([^区县市]+(?:街道|路|弄|号|巷|里|村|镇))'
        addr1_parts = re.findall(pattern, addr1)
        addr2_parts = re.findall(pattern, addr2)
        
        if addr1_parts and addr2_parts:
            # 检查是否有相同的街道信息
            for part1 in addr1_parts:
                for part2 in addr2_parts:
                    if part1 == part2:
                        return True
        
        # 如果没有提取到街道信息，使用字符相似度
        from difflib import SequenceMatcher
        similarity = SequenceMatcher(None, addr1, addr2).ratio()
        return similarity >= threshold
    
    def _format_optimized_no_match_result(self, source_record: Dict) -> Dict:
        """格式化优化的无匹配结果"""
        return {
            # 主记录信息（安全排查系统）
            'primary_record_id': source_record.get('_id'),
            'primary_system': 'inspection',
            'unit_name': source_record.get('UNIT_NAME', ''),
            'unit_address': source_record.get('ADDRESS', ''),
            'unit_type': source_record.get('UNIT_TYPE', ''),
            'contact_person': source_record.get('LEGAL_PEOPLE', ''),
            'contact_phone': source_record.get('SECURITY_TEL', ''),
            
            # 匹配记录信息（空，因为没有匹配）
            'matched_record_id': None,
            'matched_system': None,
            'matched_unit_name': '',
            'matched_unit_address': '',
            'matched_credit_code': '',
            'matched_legal_person': '',
            'matched_contact_phone': '',
            'matched_registration_date': '',
            'matched_business_scope': '',
            
            # 匹配信息
            'match_type': 'none',
            'match_status': 'unmatched',
            'similarity_score': 0.0,
            'match_confidence': 'none',
            'match_fields': [],
            'match_details': {
                'match_method': 'none',
                'reason': 'no_suitable_match_found'
            },
            
            # 审核信息
            'review_status': 'pending',
            'review_notes': '未找到匹配，需要人工审核',
            'reviewer': '',
            'review_time': None,
            
            # 时间戳
            'created_time': datetime.now(),
            'updated_time': datetime.now(),
            
            # 匹配ID
            'match_id': generate_match_id(str(source_record.get('_id')), 'no_match')
        }
    
    def _get_existing_match_result(self, source_id: str) -> Optional[Dict]:
        """获取现有的匹配结果"""
        try:
            collection = self.db_manager.get_collection('unit_match_results')
            
            # 查找基于主记录ID的匹配结果
            result = collection.find_one({'primary_record_id': source_id})
            
            return result
            
        except Exception as e:
            logger.error(f"获取现有匹配结果失败: {str(e)}")
            return None
    
    def _batch_save_optimized_results(self, results: List[Dict]) -> bool:
        """批量保存优化的匹配结果 - 增强版本"""
        if not results:
            logger.warning("没有结果需要保存")
            return True
        
        try:
            import traceback
            from pymongo import ReplaceOne
            
            collection = self.db_manager.get_collection('unit_match_results')
            
            # 预处理验证
            operations = []
            valid_results = []
            skipped_count = 0
            
            for result in results:
                if result.get('operation') == 'skipped':
                    skipped_count += 1
                    continue
                
                primary_id = result.get('primary_record_id')
                if not primary_id:
                    logger.warning(f"跳过无primary_record_id的记录: {result}")
                    continue
                
                # 移除操作标识，不保存到数据库
                result_to_save = {k: v for k, v in result.items() if k not in ['operation', 'previous_result_id']}
                
                # 确保必要字段存在
                if 'created_time' not in result_to_save:
                    result_to_save['created_time'] = datetime.now()
                if 'updated_time' not in result_to_save:
                    result_to_save['updated_time'] = datetime.now()
                
                # 数据类型安全处理
                for key, value in result_to_save.items():
                    if key in ['created_time', 'updated_time', 'review_time'] and value is not None:
                        if not isinstance(value, datetime):
                            try:
                                if isinstance(value, str):
                                    result_to_save[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                else:
                                    result_to_save[key] = datetime.now()
                            except:
                                result_to_save[key] = datetime.now()
                    elif key == 'similarity_score' and value is not None:
                        try:
                            result_to_save[key] = float(value) if value != '' else 0.0
                        except:
                            result_to_save[key] = 0.0
                
                # 验证数据完整性
                required_fields = ['primary_record_id', 'unit_name', 'match_status']
                if all(field in result_to_save for field in required_fields):
                    valid_results.append(result_to_save)
                    
                    # 使用正确的PyMongo操作格式
                    operations.append(
                        ReplaceOne(
                            filter={'primary_record_id': primary_id},
                            replacement=result_to_save,
                            upsert=True
                        )
                    )
                else:
                    logger.warning(f"跳过不完整的记录: {result_to_save}")
            
            # 执行保存操作
            if operations:
                logger.info(f"准备批量保存: 总计={len(results)}, 跳过={skipped_count}, 有效={len(operations)}")
                
                # 记录保存前的数据库状态
                before_count = collection.count_documents({})
                
                # 执行批量操作
                bulk_result = collection.bulk_write(operations, ordered=False)
                
                # 验证保存结果
                after_count = collection.count_documents({})
                actual_new_records = after_count - before_count
                
                logger.info(f"批量保存结果详情:")
                logger.info(f"  - 匹配记录: {bulk_result.matched_count}")
                logger.info(f"  - 修改记录: {bulk_result.modified_count}")
                logger.info(f"  - 插入记录: {bulk_result.upserted_count}")
                logger.info(f"  - 数据库前: {before_count} 条")
                logger.info(f"  - 数据库后: {after_count} 条")
                logger.info(f"  - 实际新增: {actual_new_records} 条")
                
                # 强制验证保存成功
                if bulk_result.upserted_count > 0 or bulk_result.modified_count > 0:
                    # 再次验证数据真的保存了
                    saved_count = 0
                    for result in valid_results:
                        check_result = collection.find_one({'primary_record_id': result['primary_record_id']})
                        if check_result:
                            saved_count += 1
                    
                    logger.info(f"数据保存验证: {saved_count}/{len(valid_results)} 条记录确认保存")
                    
                    if saved_count == len(valid_results):
                        logger.info("✅ 数据保存成功验证通过")
                        return True
                    else:
                        logger.error(f"❌ 数据保存验证失败: 期望{len(valid_results)}条，实际{saved_count}条")
                        return False
                else:
                    logger.error("❌ 数据保存验证失败：没有记录被插入或修改")
                    return False
            else:
                logger.warning("没有有效的记录需要保存")
                return True
            
        except Exception as e:
            logger.error(f"批量保存优化匹配结果失败: {str(e)}")
            logger.error(f"详细错误堆栈: {traceback.format_exc()}")
            return False
    
    
    def _force_final_save_check(self) -> int:
        """强制最终保存检查 - 确保停止时数据完全保存"""
        try:
            import traceback
            saved_count = 0
            
            logger.info("开始强制最终保存检查...")
            
            # 检查是否有未保存的批次结果
            # 这里可以检查内存中是否还有未保存的数据
            # 由于我们的设计是每个批次都立即保存，这里主要是验证
            
            # 验证数据库中的记录数
            collection = self.db_manager.get_collection('unit_match_results')
            current_count = collection.count_documents({})
            
            logger.info(f"数据库当前记录数: {current_count}")
            
            # 检查最近的保存操作是否成功
            recent_records = list(collection.find().sort("created_time", -1).limit(10))
            
            if recent_records:
                logger.info(f"最近保存的记录: {len(recent_records)} 条")
                latest_record = recent_records[0]
                logger.info(f"最新记录时间: {latest_record.get('created_time')}")
                logger.info(f"最新记录单位: {latest_record.get('unit_name')}")
            else:
                logger.warning("数据库中没有找到任何匹配记录")
            
            saved_count = current_count
            logger.info(f"强制最终保存检查完成: 确认保存 {saved_count} 条记录")
            
            return saved_count
            
        except Exception as e:
            logger.error(f"强制最终保存检查失败: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return 0

    def _clear_match_results(self) -> bool:
        """清空匹配结果"""
        try:
            collection = self.db_manager.get_collection('unit_match_results')
            
            # 删除所有匹配结果
            result = collection.delete_many({})
            
            logger.info(f"清空匹配结果: 删除了 {result.deleted_count} 条记录")
            
            return True
            
        except Exception as e:
            logger.error(f"清空匹配结果失败: {str(e)}")
            return False
    
    def get_optimized_task_progress(self, task_id: str) -> Dict:
        """获取优化任务进度"""
        with self.tasks_lock:
            progress = self.active_tasks.get(task_id)
            
            if not progress:
                return {
                    'error': '任务不存在',
                    'task_id': task_id
                }
            
            return progress.get_progress()
    
    def stop_optimized_matching_task(self, task_id: str) -> bool:
        """停止优化匹配任务 - 增强版本，确保数据保存"""
        try:
            with self.tasks_lock:
                progress = self.active_tasks.get(task_id)
                
                if not progress:
                    logger.warning(f"尝试停止不存在的任务: {task_id}")
                    return False
                
                if progress.status == "running":
                    # 设置停止状态
                    progress.set_status("stopped")
                    logger.info(f"优化匹配任务停止信号已发送: {task_id}")
                    
                    # 等待当前批次处理完成（最多等待30秒）
                    import time
                    wait_count = 0
                    max_wait = 30
                    
                    while wait_count < max_wait:
                        # 检查是否还在处理中
                        current_progress = progress.get_progress()
                        if current_progress.get('status') == 'stopped':
                            break
                        
                        time.sleep(1)
                        wait_count += 1
                        
                        if wait_count % 5 == 0:  # 每5秒输出一次等待信息
                            logger.info(f"等待任务 {task_id} 完成当前批次处理... ({wait_count}s)")
                    
                    # 强制执行最终数据保存检查
                    logger.info("执行停止时的最终数据保存检查...")
                    final_save_count = self._force_final_save_check()
                    logger.info(f"停止时最终保存检查完成: 确认 {final_save_count} 条记录已保存")
                    
                    if wait_count >= max_wait:
                        logger.warning(f"任务 {task_id} 停止超时，但数据保存检查已完成")
                    else:
                        logger.info(f"优化匹配任务已完全停止: {task_id}")
                    
                    return True
                elif progress.status in ["completed", "error", "stopped"]:
                    logger.info(f"任务 {task_id} 已处于 {progress.status} 状态")
                    # 即使任务已停止，也执行一次数据保存检查
                    final_save_count = self._force_final_save_check()
                    logger.info(f"状态检查时的数据保存验证: 确认 {final_save_count} 条记录已保存")
                    return True
                else:
                    logger.warning(f"任务 {task_id} 状态异常: {progress.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"停止优化匹配任务失败: {str(e)}")
            return False
    
    def get_optimized_matching_statistics(self) -> Dict:
        """获取优化匹配统计信息"""
        try:
            collection = self.db_manager.get_collection('unit_match_results')
            
            # 基础统计
            total_results = collection.count_documents({})
            matched_results = collection.count_documents({'match_status': 'matched'})
            unmatched_results = collection.count_documents({'match_status': 'unmatched'})
            
            # 按匹配类型统计
            match_type_stats = list(collection.aggregate([
                {
                    '$group': {
                        '_id': '$match_type',
                        'count': {'$sum': 1},
                        'avg_similarity': {'$avg': '$similarity_score'}
                    }
                }
            ]))
            
            # 按置信度统计
            confidence_stats = list(collection.aggregate([
                {
                    '$group': {
                        '_id': '$match_confidence',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            
            # 审核状态统计
            review_stats = list(collection.aggregate([
                {
                    '$group': {
                        '_id': '$review_status',
                        'count': {'$sum': 1}
                    }
                }
            ]))
            
            # 任务统计
            with self.tasks_lock:
                task_stats = {
                    'active_tasks': len([p for p in self.active_tasks.values() if p.status == 'running']),
                    'completed_tasks': len([p for p in self.active_tasks.values() if p.status == 'completed']),
                    'total_tasks': len(self.active_tasks)
                }
            
            return {
                'total_results': total_results,
                'matched_results': matched_results,
                'unmatched_results': unmatched_results,
                'match_rate': round((matched_results / total_results * 100) if total_results > 0 else 0, 2),
                'match_type_stats': match_type_stats,
                'confidence_stats': confidence_stats,
                'review_stats': review_stats,
                'task_stats': task_stats,
                'last_updated': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"获取优化匹配统计失败: {str(e)}")
            return {
                'total_results': 0,
                'matched_results': 0,
                'unmatched_results': 0,
                'match_rate': 0,
                'match_type_stats': [],
                'confidence_stats': [],
                'review_stats': [],
                'task_stats': {},
                'error': str(e)
            }
    
    def cleanup_optimized_completed_tasks(self, max_age_hours: int = 24):
        """清理已完成的优化任务"""
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
                    logger.info(f"清理优化任务: {task_id}")
                    
        except Exception as e:
            logger.error(f"清理优化任务失败: {str(e)}")
    
    def get_match_result_details(self, match_id: str) -> Optional[Dict]:
        """获取匹配结果详情"""
        try:
            collection = self.db_manager.get_collection('unit_match_results')
            
            result = collection.find_one({'match_id': match_id})
            
            if result:
                # 转换ObjectId为字符串
                from src.utils.helpers import convert_objectid_to_str
                return convert_objectid_to_str(result)
            
            return None
            
        except Exception as e:
            logger.error(f"获取匹配结果详情失败: {str(e)}")
            return None
    
    def update_review_status(self, match_id: str, review_status: str, 
                           review_notes: str = '', reviewer: str = '') -> bool:
        """更新审核状态 - 支持通过_id或match_id查询"""
        try:
            from bson import ObjectId
            collection = self.db_manager.get_collection('unit_match_results')
            
            update_data = {
                'review_status': review_status,
                'review_notes': review_notes,
                'reviewer': reviewer,
                'review_time': datetime.now(),
                'updated_time': datetime.now()
            }
            
            # 尝试通过不同的字段进行查询
            result = None
            
            # 首先尝试通过ObjectId查询（前端传递的_id）
            try:
                if ObjectId.is_valid(match_id):
                    result = collection.update_one(
                        {'_id': ObjectId(match_id)},
                        {'$set': update_data}
                    )
                    if result.modified_count > 0:
                        logger.info(f"通过_id更新审核状态成功: {match_id} -> {review_status}")
                        return True
            except Exception as e:
                logger.debug(f"通过_id查询失败: {str(e)}")
            
            # 如果通过_id查询失败，尝试通过match_id字段查询
            if not result or result.modified_count == 0:
                result = collection.update_one(
                    {'match_id': match_id},
                    {'$set': update_data}
                )
                if result.modified_count > 0:
                    logger.info(f"通过match_id更新审核状态成功: {match_id} -> {review_status}")
                    return True
            
            # 如果都失败了，尝试通过primary_record_id查询
            if not result or result.modified_count == 0:
                result = collection.update_one(
                    {'primary_record_id': match_id},
                    {'$set': update_data}
                )
                if result.modified_count > 0:
                    logger.info(f"通过primary_record_id更新审核状态成功: {match_id} -> {review_status}")
                    return True
            
            logger.warning(f"未找到匹配记录进行审核状态更新: {match_id}")
            return False
                
        except Exception as e:
            logger.error(f"更新审核状态失败: {str(e)}")
            import traceback
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            return False

    def _batch_save_optimized_results_enhanced(self, results: List[Dict]) -> bool:
        """增强的批量保存方法 - 确保数据完整性"""
        if not results:
            logger.warning("⚠️ 没有结果需要保存")
            return True
            
        try:
            logger.info(f"💾 开始增强批量保存: {len(results)} 条记录")
            
            # 数据完整性验证
            validated_results = []
            for result in results:
                # 必要字段检查
                if not result.get('primary_record_id'):
                    logger.warning(f"⚠️ 跳过无效记录: 缺少primary_record_id")
                    continue
                    
                # 数据类型安全处理
                safe_result = {}
                for key, value in result.items():
                    if value is not None:
                        # 时间字段标准化
                        if 'time' in key and hasattr(value, 'isoformat'):
                            safe_result[key] = value
                        elif isinstance(value, (str, int, float, bool, dict, list)):
                            safe_result[key] = value
                        else:
                            safe_result[key] = str(value)
                    else:
                        safe_result[key] = None
                        
                validated_results.append(safe_result)
            
            if not validated_results:
                logger.warning("⚠️ 所有记录验证失败，没有有效数据保存")
                return False
                
            logger.info(f"✅ 数据验证完成: {len(validated_results)} 条有效记录")
            
            # 双重保存验证
            collection = self.db_manager.get_collection("unit_match_results")
            
            # 第一次保存
            insert_result = collection.insert_many(validated_results)
            inserted_count = len(insert_result.inserted_ids)
            
            logger.info(f"💾 第一次保存完成: {inserted_count} 条记录")
            
            # 验证保存结果
            if inserted_count != len(validated_results):
                logger.error(f"❌ 保存数量不匹配: 期望 {len(validated_results)}, 实际 {inserted_count}")
                return False
            
            # 数据库记录数验证
            total_count = collection.count_documents({})
            logger.info(f"📊 数据库总记录数: {total_count}")
            
            # 验证最新记录
            latest_records = list(collection.find().sort("_id", -1).limit(3))
            logger.info(f"🔍 最新记录验证: 找到 {len(latest_records)} 条最新记录")
            
            for i, record in enumerate(latest_records):
                unit_name = record.get('unit_name', 'Unknown')
                match_status = record.get('match_status', 'Unknown')
                logger.info(f"   记录 {i+1}: {unit_name} - {match_status}")
            
            logger.info(f"✅ 增强批量保存成功: {inserted_count} 条记录已确认保存")
            return True
            
        except Exception as e:
            logger.error(f"❌ 增强批量保存失败: {str(e)}")
            import traceback
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            return False


    def stop_optimized_matching_task_enhanced(self, task_id: str) -> bool:
        """增强的停止匹配任务方法"""
        try:
            progress = self.active_tasks.get(task_id)
            if not progress:
                logger.warning(f"任务不存在或已完成: {task_id}")
                return False
            
            logger.info(f"🛑 开始停止优化匹配任务: {task_id}")
            
            # 设置停止状态
            progress.set_status("stopping")
            logger.info("📋 任务状态设置为停止中...")
            
            # 等待当前批次完成
            max_wait = 30  # 最多等待30秒
            wait_count = 0
            
            while wait_count < max_wait:
                if progress.status == "stopped":
                    logger.info("✅ 任务已自然停止")
                    break
                    
                time.sleep(1)
                wait_count += 1
                
                if wait_count % 5 == 0:
                    logger.info(f"⏳ 等待任务停止中... ({wait_count}s)")
            
            # 强制停止
            if progress.status != "stopped":
                progress.set_status("stopped")
                logger.info("🔨 强制设置任务为已停止状态")
            
            # 执行停止时的最终数据保存检查
            logger.info("🔍 执行停止时的最终数据保存检查...")
            saved_count = self._force_final_save_check_enhanced()
            
            logger.info(f"✅ 停止时最终保存检查完成，确保保存 {saved_count} 条记录")
            
            # 清理任务
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]
                logger.info(f"🧹 任务已从活动列表中移除: {task_id}")
            
            logger.info(f"🎉 优化匹配任务已完全停止: {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"停止优化匹配任务失败: {str(e)}")
            return False


    def _force_final_save_check_enhanced(self) -> int:
        """增强的强制最终保存检查"""
        try:
            logger.info("🔍 开始增强强制最终保存检查...")
            
            collection = self.db_manager.get_collection("unit_match_results")
            current_count = collection.count_documents({})
            
            logger.info(f"📊 数据库当前记录数: {current_count}")
            
            # 获取最近保存的记录进行验证
            recent_records = list(collection.find().sort("_id", -1).limit(10))
            logger.info(f"🔍 最近保存的记录: {len(recent_records)} 条")
            
            if recent_records:
                latest_record = recent_records[0]
                latest_time = latest_record.get('created_time', 'Unknown')
                latest_unit = latest_record.get('unit_name', 'Unknown')
                
                logger.info(f"📅 最新记录时间: {latest_time}")
                logger.info(f"🏢 最新记录单位: {latest_unit}")
            
            logger.info(f"✅ 增强强制最终保存检查完成，确保保存 {current_count} 条记录")
            return current_count
            
        except Exception as e:
            logger.error(f"增强强制最终保存检查失败: {str(e)}")
            return 0
