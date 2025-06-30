#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强关联处理器模块
基于消防业务特点的智能关联策略
处理建筑级别和单位级别的多对多关系
"""

import logging
import uuid
import json
from typing import Dict, List, Optional, Union, Tuple, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from collections import defaultdict

from .exact_matcher import ExactMatcher, MatchResult
from .fuzzy_matcher import FuzzyMatcher, FuzzyMatchResult
from src.utils.helpers import batch_iterator, generate_match_id, format_timestamp

logger = logging.getLogger(__name__)


class AssociationStrategy:
    """关联策略枚举"""
    BUILDING_BASED = "building_based"      # 建筑级关联（一对一）
    UNIT_BASED = "unit_based"             # 单位级关联（一对多）
    HYBRID = "hybrid"                     # 混合策略（智能选择）


class AssociationProgress:
    """增强关联任务的进度跟踪"""
    def __init__(self, task_id: str, total_records: int, strategy: str):
        self.task_id = task_id
        self.total_records = total_records
        self.strategy = strategy
        self.processed_records = 0
        self.associations_found = 0
        self.start_time = datetime.now()
        self.status = "running"  # running, completed, error, stopped
        self.current_step = "initializing" # initializing, processing, saving, completed
        self.lock = Lock()

    def update_progress(self, processed: int, found: int):
        with self.lock:
            self.processed_records += processed
            self.associations_found += found
    
    def set_step(self, step: str):
        with self.lock:
            self.current_step = step

    def get_progress(self) -> Dict:
        with self.lock:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            progress_percent = (self.processed_records / self.total_records * 100) if self.total_records > 0 else 0
            
            # 估算剩余时间
            if self.processed_records > 0 and elapsed_time > 0:
                time_per_record = elapsed_time / self.processed_records
                remaining_records = self.total_records - self.processed_records
                estimated_remaining_time = remaining_records * time_per_record
            else:
                estimated_remaining_time = 0
                
            return {
                'task_id': self.task_id,
                'status': self.status,
                'current_step': self.current_step,
                'total_records': self.total_records,
                'processed_records': self.processed_records,
                'associations_found': self.associations_found,
                'progress_percent': round(progress_percent, 2),
                'elapsed_time_seconds': round(elapsed_time, 2),
                'estimated_remaining_seconds': round(estimated_remaining_time, 2)
            }

    def complete(self):
        with self.lock:
            self.status = "completed"
            self.current_step = "finished"

    def fail(self):
        with self.lock:
            self.status = "error"
            self.current_step = "failed"


class AssociationResult:
    """关联结果数据类"""
    
    def __init__(self):
        self.primary_record_id = None
        self.primary_system = "inspection"
        self.primary_unit_name = ""
        self.primary_unit_address = ""
        self.primary_building_id = ""
        self.primary_legal_person = ""
        self.primary_security_tel = ""
        self.primary_security_manager = ""
        self.primary_credit_code = ""
        
        # 关联的监督检查记录（可能多个）
        self.associated_records = []
        
        # 关联信息
        self.association_strategy = ""
        self.association_confidence = 0.0
        self.association_details = {}
        self.association_id = str(uuid.uuid4())  # 生成唯一ID
        
        # 数据质量评估
        self.data_quality_score = 0.0
        self.data_completeness = {}
        
        # 业务分析
        self.unit_building_count = 1  # 该单位的建筑数量
        self.supervision_record_count = 0  # 监督检查记录数量
        self.latest_inspection_date = None
        
    def add_associated_record(self, record: Dict, match_type: str, similarity: float):
        """添加关联记录"""
        self.associated_records.append({
            'record_id': record.get('_id'),
            'unit_name': record.get('dwmc', ''),
            'unit_address': record.get('dwdz', ''),
            'legal_person': record.get('fddbr', ''),
            'contact_phone': record.get('lxdh', ''),
            'security_manager': record.get('xfaqglr', ''),
            'credit_code': record.get('tyshxydm', ''),
            'registration_date': record.get('djrq', ''),
            'business_scope': record.get('jyfw', ''),
            'match_type': match_type,
            'similarity_score': similarity,
            'inspection_date': record.get('jcrq', ''),
            'inspector': record.get('jcy', ''),
            'inspection_result': record.get('jcjg', '')
        })
        self.supervision_record_count = len(self.associated_records)
    
    def calculate_data_quality(self):
        """计算数据质量评分"""
        primary_fields = [
            self.primary_unit_name, self.primary_unit_address,
            self.primary_legal_person, self.primary_credit_code
        ]
        
        # 基准数据完整度
        primary_completeness = sum(1 for field in primary_fields if field and str(field).strip()) / len(primary_fields)
        
        # 关联数据完整度
        if self.associated_records:
            associated_completeness = 0
            for record in self.associated_records:
                record_fields = [
                    record['unit_name'], record['unit_address'],
                    record['legal_person'], record['credit_code']
                ]
                record_completeness = sum(1 for field in record_fields if field and str(field).strip()) / len(record_fields)
                associated_completeness += record_completeness
            associated_completeness /= len(self.associated_records)
        else:
            associated_completeness = 0
        
        # 综合质量评分
        self.data_quality_score = (primary_completeness * 0.6 + associated_completeness * 0.4)
        self.data_completeness = {
            'primary_completeness': primary_completeness,
            'associated_completeness': associated_completeness,
            'total_score': self.data_quality_score
        }
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        self.calculate_data_quality()
        
        return {
            # 基准记录信息（安全隐患排查系统）
            'primary_record_id': self.primary_record_id,
            'primary_system': self.primary_system,
            'primary_unit_name': self.primary_unit_name,
            'primary_unit_address': self.primary_unit_address,
            'primary_building_id': self.primary_building_id,
            'primary_legal_person': self.primary_legal_person,
            'primary_security_tel': self.primary_security_tel,
            'primary_security_manager': self.primary_security_manager,
            'primary_credit_code': self.primary_credit_code,
            
            # 关联记录信息
            'associated_records': self.associated_records,
            'supervision_record_count': self.supervision_record_count,
            
            # 关联分析
            'association_strategy': self.association_strategy,
            'association_confidence': self.association_confidence,
            'association_details': self.association_details,
            
            # 数据质量
            'data_quality_score': round(self.data_quality_score, 3),
            'data_completeness': self.data_completeness,
            
            # 业务分析
            'unit_building_count': self.unit_building_count,
            'latest_inspection_date': self.latest_inspection_date,
            
            # 元数据
            'created_time': datetime.now().isoformat(),
            'association_id': self.association_id
        }


class EnhancedAssociationProcessor:
    """增强关联处理器"""
    
    def __init__(self, db_manager, config: Dict):
        self.db_manager = db_manager
        self.config = config
        
        # 初始化匹配器
        self.exact_matcher = ExactMatcher(config)
        self.fuzzy_matcher = FuzzyMatcher(config)
        
        # 关联策略配置
        self.association_config = config.get('association', {})
        self.default_strategy = self.association_config.get('default_strategy', AssociationStrategy.HYBRID)
        self.confidence_threshold = self.association_config.get('confidence_threshold', 0.75)
        
        # 批处理配置
        self.batch_size = config.get('batch_processing', {}).get('batch_size', 100)
        self.max_workers = config.get('batch_processing', {}).get('max_workers', 4)
        
        # 任务管理
        self.active_tasks = {}
        self.tasks_lock = Lock()
    
    def _safe_str(self, value, default: str = '') -> str:
        """安全地将值转换为字符串并去除空白"""
        if value is None:
            return default
        return str(value).strip()
    
    def start_enhanced_association_task(self, strategy: str = AssociationStrategy.HYBRID,
                                      clear_existing: bool = False) -> str:
        """启动增强关联任务"""
        task_id = str(uuid.uuid4())
        
        try:
            if clear_existing:
                self._clear_association_results()
                logger.info("已清空现有关联结果")
            
            # 获取数据统计
            inspection_count = self.db_manager.get_collection_count('xfaqpc_jzdwxx')
            supervision_count = self.db_manager.get_collection_count('xxj_shdwjbxx')
            
            logger.info(f"开始增强关联任务: 安全排查{inspection_count}条, 监督检查{supervision_count}条")
            
            # 创建并注册进度跟踪器
            progress = AssociationProgress(task_id, inspection_count + supervision_count, strategy)
            with self.tasks_lock:
                self.active_tasks[task_id] = progress

            # 启动异步任务
            from threading import Thread
            task_thread = Thread(
                target=self._execute_enhanced_association_task,
                args=(task_id, strategy),
                daemon=True
            )
            task_thread.start()
            
            return task_id
            
        except Exception as e:
            logger.error(f"启动增强关联任务失败: {str(e)}")
            raise
    
    def _execute_enhanced_association_task(self, task_id: str, strategy: str):
        """
        执行增强关联任务 - [最终修复] 使用聚合管道，将计算完全交给数据库
        """
        progress = self.active_tasks.get(task_id)
        if not progress:
            logger.error(f"无法找到任务 {task_id} 的进度跟踪器。")
            return

        try:
            logger.info(f"执行增强关联任务: {task_id}, 策略: {strategy}")
            progress.set_step("aggregating_data")
            
            source_collection = self.db_manager.get_collection('unit_match_results')
            
            # 构建强大的聚合管道
            pipeline = []

            # 策略1: 基于建筑ID进行分组
            if strategy in [AssociationStrategy.BUILDING_BASED, AssociationStrategy.HYBRID]:
                pipeline.extend([
                    {'$match': {'building_id': {'$ne': None, '$ne': ''}}},
                    {'$group': {
                        '_id': '$building_id',
                        'records': {'$push': '$$ROOT'},
                        'count': {'$sum': 1}
                    }},
                    {'$match': {'count': {'$gt': 1}}},
                    {
                        '$project': {
                            '_id': 0,
                            'association_id': {'$toString': '$_id'},
                            'association_strategy': 'building_based',
                            'primary_record': {'$arrayElemAt': ['$records', 0]},
                            'supervision_record_count': '$count',
                            'associated_records': '$records'
                        }
                    },
                    { # 第二个 $project 阶段，基于上一步的结果进行计算
                        '$project': {
                            '_id': 0,
                            'association_id': '$association_id',
                            'association_strategy': '$association_strategy',
                            'primary_unit_name': '$primary_record.unit_name',
                            'primary_unit_address': '$primary_record.unit_address',
                            'primary_legal_person': '$primary_record.contact_person',
                            'primary_credit_code': '$primary_record.primary_credit_code',
                            'supervision_record_count': '$supervision_record_count',
                            'unit_building_count': {'$size': '$associated_records'},
                            'association_confidence': {'$ifNull': [{'$max': '$associated_records.similarity_score'}, 0]},
                            'data_quality_score': {
                                '$let': {
                                    'vars': {
                                        'total_fields': {'$multiply': [{'$size': '$associated_records'}, 4]},
                                        'filled_fields': {
                                            '$reduce': {
                                                'input': '$associated_records',
                                                'initialValue': 0,
                                                'in': {
                                                    '$add': [
                                                        '$$value',
                                                        {'$cond': [{'$ne': ['$$this.unit_name', '']}, 1, 0]},
                                                        {'$cond': [{'$ne': ['$$this.unit_address', '']}, 1, 0]},
                                                        {'$cond': [{'$ne': ['$$this.contact_person', '']}, 1, 0]},
                                                        {'$cond': [{'$ne': ['$$this.primary_credit_code', '']}, 1, 0]}
                                                    ]
                                                }
                                            }
                                        }
                                    },
                                    'in': {'$cond': [{'$eq': ['$$total_fields', 0]}, 0, {'$divide': ['$$filled_fields', '$$total_fields']}]}
                                }
                            },
                            'associated_records': {
                                '$map': {
                                    'input': '$associated_records',
                                    'as': 'rec',
                                    'in': {
                                        '_id': '$$rec._id',
                                        'unit_name': '$$rec.unit_name',
                                        'unit_address': '$$rec.unit_address',
                                        'legal_person': '$$rec.contact_person',
                                        'credit_code': '$$rec.primary_credit_code',
                                        'match_type': '$$rec.match_type',
                                        'similarity_score': '$$rec.similarity_score',
                                        'inspection_date': '$$rec.created_time'
                                    }
                                }
                            },
                            'created_time': '$$NOW'
                        }
                    }
                ])

            # 策略2: 基于单位标识（名称和信用代码）进行分组
            if strategy in [AssociationStrategy.UNIT_BASED, AssociationStrategy.HYBRID]:
                # 此处可以构建更复杂的管道来合并 unit_based 的结果
                # 为简化，我们先专注于修复和实现最核心的模式
                pass

            # 注意: $out必须是管道的最后一个阶段
            pipeline.append({
                '$merge': {'into': 'enhanced_association_results', 'on': 'association_id', 'whenMatched': 'replace', 'whenNotMatched': 'insert'}
            })
            
            logger.info("正在执行数据库端聚合... 这可能需要一些时间。")
            source_collection.aggregate(pipeline, allowDiskUse=True)
            logger.info("数据库端聚合完成，结果已写入 'enhanced_association_results' 集合。")

            progress.complete()
            logger.info(f"增强关联任务完成: {task_id}")

        except Exception as e:
            progress.fail()
            logger.error(f"执行增强关联任务失败 {task_id}: {e}", exc_info=True)
            
    # _process_association_for_record 方法可以被废弃或简化，因为主要逻辑已移至聚合管道
    def _process_association_for_record(self, base_match_record: Dict, strategy: str) -> Optional[AssociationResult]:
        # 此方法在新的聚合模型下不再被直接调用
        pass

    def _calculate_association_confidence(self, inspection_record: Dict, supervision_records: List[Dict]) -> float:
        """计算关联置信度"""
        if not supervision_records:
            return 0.0
        
        confidences = []
        
        for sup_record in supervision_records:
            _, similarity = self._determine_association_type(inspection_record, sup_record)
            confidences.append(similarity)
        
        # 返回最高置信度
        return max(confidences) if confidences else 0.0
    
    def _determine_association_type(self, inspection_record: Dict, supervision_record: Dict) -> Tuple[str, float]:
        """确定关联类型和相似度"""
        # 信用代码精确匹配
        inspection_credit = self._safe_str(inspection_record.get('CREDIT_CODE'))
        supervision_credit = self._safe_str(supervision_record.get('tyshxydm'))
        
        if inspection_credit and supervision_credit and inspection_credit == supervision_credit:
            return "exact_credit_code", 1.0
        
        # 单位名称匹配
        inspection_name = self._safe_str(inspection_record.get('UNIT_NAME'))
        supervision_name = self._safe_str(supervision_record.get('dwmc'))
        
        if inspection_name and supervision_name:
            name_similarity = self._calculate_name_similarity(inspection_name, supervision_name)
            if name_similarity >= 0.9:
                return "exact_unit_name", name_similarity
            elif name_similarity >= 0.7:
                return "fuzzy_unit_name", name_similarity
        
        # 综合模糊匹配
        overall_similarity = self._calculate_overall_similarity(inspection_record, supervision_record)
        
        if overall_similarity >= 0.8:
            return "fuzzy_comprehensive", overall_similarity
        elif overall_similarity >= 0.6:
            return "fuzzy_partial", overall_similarity
        else:
            return "weak_association", overall_similarity
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """计算名称相似度"""
        if not name1 or not name2:
            return 0.0
        
        name1 = self._normalize_unit_name(name1)
        name2 = self._normalize_unit_name(name2)
        
        from difflib import SequenceMatcher
        return SequenceMatcher(None, name1, name2).ratio()
    
    def _calculate_overall_similarity(self, inspection_record: Dict, supervision_record: Dict) -> float:
        """计算综合相似度"""
        scores = []
        weights = []
        
        # 单位名称相似度 (权重: 0.4)
        name_sim = self._calculate_name_similarity(
            inspection_record.get('UNIT_NAME', ''),
            supervision_record.get('dwmc', '')
        )
        scores.append(name_sim)
        weights.append(0.4)
        
        # 法定代表人相似度 (权重: 0.3)
        legal_sim = self._calculate_name_similarity(
            inspection_record.get('LEGAL_PEOPLE', ''),
            supervision_record.get('fddbr', '')
        )
        scores.append(legal_sim)
        weights.append(0.3)
        
        # 地址相似度 (权重: 0.3)
        addr_sim = self._calculate_address_similarity(
            inspection_record.get('UNIT_ADDRESS', ''),
            supervision_record.get('dwdz', '')
        )
        scores.append(addr_sim)
        weights.append(0.3)
        
        # 加权平均
        if sum(weights) > 0:
            weighted_score = sum(s * w for s, w in zip(scores, weights)) / sum(weights)
            return weighted_score
        else:
            return 0.0
    
    def _calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """计算地址相似度"""
        if not addr1 or not addr2:
            return 0.0
        
        # 标准化地址
        addr1 = self._normalize_address(addr1)
        addr2 = self._normalize_address(addr2)
        
        from difflib import SequenceMatcher
        return SequenceMatcher(None, addr1, addr2).ratio()
    
    def _normalize_address(self, address: str) -> str:
        """标准化地址"""
        if not address:
            return ""
        
        # 移除空白字符
        address = address.strip()
        
        # 统一行政区划名称
        replacements = {
            '县': '区',
            '市区': '区',
            '开发区': '区'
        }
        
        for old, new in replacements.items():
            address = address.replace(old, new)
        
        return address
    
    def _batch_save_association_results(self, results: List[Dict]) -> bool:
        """批量保存关联结果"""
        try:
            if not results:
                return True
            
            collection = self.db_manager.get_collection('enhanced_association_results')
            
            # 批量插入
            insert_result = collection.insert_many(results)
            
            logger.info(f"批量保存关联结果成功: {len(insert_result.inserted_ids)}条")
            return True
            
        except Exception as e:
            logger.error(f"批量保存关联结果失败: {str(e)}")
            return False
    
    def _clear_association_results(self) -> bool:
        """清空关联结果"""
        try:
            collection = self.db_manager.get_collection('enhanced_association_results')
            result = collection.delete_many({})
            
            logger.info(f"清空关联结果成功: {result.deleted_count}条")
            return True
            
        except Exception as e:
            logger.error(f"清空关联结果失败: {str(e)}")
            return False
    
    def get_association_statistics(self) -> Dict:
        """获取真实的关联统计信息"""
        try:
            collection = self.db_manager.get_collection('enhanced_association_results')
            
            if collection.count_documents({}) == 0:
                return {
                    'total_associations': 0,
                    'with_supervision_records': 0,
                    'association_rate': 0.0,
                    'avg_supervision_records': 0.0,
                    'avg_data_quality': 0.0,
                }

            pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'total_associations': {'$sum': 1},
                        'with_supervision_records': {
                            '$sum': {'$cond': [{'$gt': ['$supervision_record_count', 0]}, 1, 0]}
                        },
                        'avg_supervision_records': {'$avg': '$supervision_record_count'},
                        'avg_data_quality': {'$avg': '$data_quality_score'},
                    }
                }
            ]
            
            result = list(collection.aggregate(pipeline))
            
            if result:
                stats = result[0]
                stats.pop('_id', None)
                
                # 计算关联率
                if stats.get('total_associations', 0) > 0:
                    stats['association_rate'] = round(
                        stats['with_supervision_records'] / stats['total_associations'] * 100, 2
                    )
                else:
                    stats['association_rate'] = 0.0
                
                return stats
            else:
                return {} # 如果没有结果，返回空字典
                
        except Exception as e:
            logger.error(f"获取关联统计失败: {str(e)}")
            return {'error': str(e)}

    def get_association_task_progress(self, task_id: str) -> Optional[Dict]:
        """获取增强关联任务的进度"""
        with self.tasks_lock:
            progress = self.active_tasks.get(task_id)
            if progress:
                return progress.get_progress()
        return None