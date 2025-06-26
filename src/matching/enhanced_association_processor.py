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
        """执行增强关联任务"""
        try:
            logger.info(f"执行增强关联任务: {task_id}, 策略: {strategy}")
            
            # 步骤1: 分析数据特征
            data_analysis = self._analyze_data_characteristics()
            logger.info(f"数据特征分析完成: {data_analysis}")
            
            # 步骤2: 构建单位聚合视图
            unit_aggregation = self._build_unit_aggregation()
            logger.info(f"单位聚合完成，共{len(unit_aggregation)}个独立单位")
            
            # 步骤3: 执行智能关联
            association_results = []
            
            for unit_key, unit_data in unit_aggregation.items():
                try:
                    result = self._process_unit_association(unit_data, strategy)
                    if result:
                        association_results.append(result.to_dict())
                        
                        # 批量保存
                        if len(association_results) >= self.batch_size:
                            self._batch_save_association_results(association_results)
                            association_results = []
                            
                except Exception as e:
                    logger.error(f"处理单位关联失败 {unit_key}: {str(e)}")
                    continue
            
            # 保存剩余结果
            if association_results:
                self._batch_save_association_results(association_results)
            
            logger.info(f"增强关联任务完成: {task_id}")
            
        except Exception as e:
            logger.error(f"执行增强关联任务失败 {task_id}: {str(e)}")
    
    def _analyze_data_characteristics(self) -> Dict:
        """分析数据特征"""
        try:
            # 分析安全排查系统
            inspection_analysis = self._analyze_inspection_data()
            
            # 分析监督检查系统
            supervision_analysis = self._analyze_supervision_data()
            
            return {
                'inspection': inspection_analysis,
                'supervision': supervision_analysis,
                'analysis_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"数据特征分析失败: {str(e)}")
            return {}
    
    def _analyze_inspection_data(self) -> Dict:
        """分析安全隐患排查系统数据"""
        pipeline = [
            {
                '$group': {
                    '_id': '$UNIT_NAME',
                    'building_count': {'$sum': 1},
                    'addresses': {'$addToSet': '$UNIT_ADDRESS'},
                    'credit_codes': {'$addToSet': '$CREDIT_CODE'},
                    'legal_persons': {'$addToSet': '$LEGAL_PEOPLE'}
                }
            },
            {
                '$project': {
                    'unit_name': '$_id',
                    'building_count': 1,
                    'address_count': {'$size': '$addresses'},
                    'credit_code_count': {'$size': '$credit_codes'},
                    'legal_person_count': {'$size': '$legal_persons'}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_units': {'$sum': 1},
                    'multi_building_units': {
                        '$sum': {'$cond': [{'$gt': ['$building_count', 1]}, 1, 0]}
                    },
                    'avg_building_per_unit': {'$avg': '$building_count'},
                    'max_building_per_unit': {'$max': '$building_count'}
                }
            }
        ]
        
        try:
            result = list(self.db_manager.get_collection('xfaqpc_jzdwxx').aggregate(pipeline))
            return result[0] if result else {}
        except Exception as e:
            logger.error(f"分析安全排查数据失败: {str(e)}")
            return {}
    
    def _analyze_supervision_data(self) -> Dict:
        """分析消防监督检查系统数据"""
        pipeline = [
            {
                '$group': {
                    '_id': '$dwmc',
                    'record_count': {'$sum': 1},
                    'addresses': {'$addToSet': '$dwdz'},
                    'credit_codes': {'$addToSet': '$tyshxydm'},
                    'legal_persons': {'$addToSet': '$fddbr'}
                }
            },
            {
                '$project': {
                    'unit_name': '$_id',
                    'record_count': 1,
                    'address_count': {'$size': '$addresses'},
                    'credit_code_count': {'$size': '$credit_codes'},
                    'legal_person_count': {'$size': '$legal_persons'}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_units': {'$sum': 1},
                    'duplicate_record_units': {
                        '$sum': {'$cond': [{'$gt': ['$record_count', 1]}, 1, 0]}
                    },
                    'avg_records_per_unit': {'$avg': '$record_count'},
                    'max_records_per_unit': {'$max': '$record_count'}
                }
            }
        ]
        
        try:
            result = list(self.db_manager.get_collection('xxj_shdwjbxx').aggregate(pipeline))
            return result[0] if result else {}
        except Exception as e:
            logger.error(f"分析监督检查数据失败: {str(e)}")
            return {}
    
    def _build_unit_aggregation(self) -> Dict:
        """构建单位聚合视图"""
        unit_aggregation = defaultdict(lambda: {
            'inspection_records': [],
            'supervision_records': [],
            'primary_credit_code': None,
            'primary_unit_name': None
        })
        
        try:
            # 聚合安全排查系统记录
            inspection_records = self.db_manager.get_collection('xfaqpc_jzdwxx').find({})
            
            for record in inspection_records:
                # 安全获取并转换为字符串
                unit_name = self._safe_str(record.get('UNIT_NAME'))
                credit_code = self._safe_str(record.get('CREDIT_CODE'))
                
                # 使用信用代码作为主键，单位名称作为备选
                unit_key = credit_code if credit_code else unit_name
                
                if unit_key:
                    unit_data = unit_aggregation[unit_key]
                    unit_data['inspection_records'].append(record)
                    
                    # 设置主要标识
                    if not unit_data['primary_credit_code'] and credit_code:
                        unit_data['primary_credit_code'] = credit_code
                    if not unit_data['primary_unit_name'] and unit_name:
                        unit_data['primary_unit_name'] = unit_name
            
            # 聚合监督检查系统记录
            supervision_records = self.db_manager.get_collection('xxj_shdwjbxx').find({})
            
            for record in supervision_records:
                # 安全获取并转换为字符串
                unit_name = self._safe_str(record.get('dwmc'))
                credit_code = self._safe_str(record.get('tyshxydm'))
                
                # 尝试匹配到已有单位
                matched_key = None
                
                # 优先按信用代码匹配
                if credit_code and credit_code in unit_aggregation:
                    matched_key = credit_code
                else:
                    # 按单位名称匹配
                    for key, data in unit_aggregation.items():
                        if (data['primary_unit_name'] and 
                            self._unit_names_similar(unit_name, data['primary_unit_name'])):
                            matched_key = key
                            break
                
                if matched_key:
                    unit_aggregation[matched_key]['supervision_records'].append(record)
            
            return dict(unit_aggregation)
            
        except Exception as e:
            logger.error(f"构建单位聚合视图失败: {str(e)}")
            return {}
    
    def _unit_names_similar(self, name1: str, name2: str, threshold: float = 0.8) -> bool:
        """判断单位名称是否相似"""
        if not name1 or not name2:
            return False
        
        # 标准化处理
        name1 = self._normalize_unit_name(name1)
        name2 = self._normalize_unit_name(name2)
        
        # 精确匹配
        if name1 == name2:
            return True
        
        # 相似度匹配
        from difflib import SequenceMatcher
        similarity = SequenceMatcher(None, name1, name2).ratio()
        
        return similarity >= threshold
    
    def _normalize_unit_name(self, name: str) -> str:
        """标准化单位名称"""
        if not name:
            return ""
        
        # 移除空白字符
        name = name.strip()
        
        # 统一括号
        name = name.replace('（', '(').replace('）', ')')
        
        # 统一公司类型简称
        replacements = {
            '有限责任公司': '有限公司',
            '股份有限公司': '股份公司',
            '个人独资企业': '独资企业'
        }
        
        for old, new in replacements.items():
            name = name.replace(old, new)
        
        return name
    
    def _process_unit_association(self, unit_data: Dict, strategy: str) -> Optional[AssociationResult]:
        """处理单位关联"""
        try:
            inspection_records = unit_data.get('inspection_records', [])
            supervision_records = unit_data.get('supervision_records', [])
            
            if not inspection_records:
                return None
            
            # 选择主要的安全排查记录（建筑级别关联时选择第一个，单位级别时选择最完整的）
            if strategy == AssociationStrategy.BUILDING_BASED:
                primary_record = inspection_records[0]
            else:
                primary_record = self._select_primary_inspection_record(inspection_records)
            
            # 创建关联结果
            result = AssociationResult()
            
            # 填充基准记录信息
            result.primary_record_id = primary_record.get('_id')
            result.primary_unit_name = primary_record.get('UNIT_NAME', '')
            result.primary_unit_address = primary_record.get('UNIT_ADDRESS', '')
            result.primary_building_id = primary_record.get('BUILDING_ID', '')
            result.primary_legal_person = primary_record.get('LEGAL_PEOPLE', '')
            result.primary_security_tel = primary_record.get('SECURITY_TEL', '')
            result.primary_security_manager = primary_record.get('SECURITY_PEOPLE', '')
            result.primary_credit_code = primary_record.get('CREDIT_CODE', '')
            result.unit_building_count = len(inspection_records)
            
            # 处理关联的监督检查记录
            if supervision_records:
                for sup_record in supervision_records:
                    # 计算匹配类型和相似度
                    match_type, similarity = self._determine_association_type(primary_record, sup_record)
                    result.add_associated_record(sup_record, match_type, similarity)
                
                # 设置关联策略和置信度
                result.association_strategy = strategy
                result.association_confidence = self._calculate_association_confidence(
                    primary_record, supervision_records
                )
                
                # 设置最新检查日期
                latest_date = self._get_latest_inspection_date(supervision_records)
                result.latest_inspection_date = latest_date
            
            # 设置关联详情
            result.association_details = {
                'inspection_building_count': len(inspection_records),
                'supervision_record_count': len(supervision_records),
                'association_method': self._get_association_method(primary_record, supervision_records),
                'data_consistency_check': self._check_cross_system_consistency(primary_record, supervision_records)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"处理单位关联失败: {str(e)}")
            return None
    
    def _select_primary_inspection_record(self, records: List[Dict]) -> Dict:
        """选择主要的安全排查记录"""
        if len(records) == 1:
            return records[0]
        
        # 按数据完整度评分选择
        best_record = records[0]
        best_score = 0
        
        for record in records:
            score = 0
            fields = ['UNIT_NAME', 'UNIT_ADDRESS', 'LEGAL_PEOPLE', 'CREDIT_CODE', 'SECURITY_PEOPLE']
            
            for field in fields:
                if record.get(field) and str(record.get(field)).strip():
                    score += 1
            
            if score > best_score:
                best_score = score
                best_record = record
        
        return best_record
    
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
    
    def _get_latest_inspection_date(self, supervision_records: List[Dict]) -> Optional[str]:
        """获取最新检查日期"""
        dates = []
        
        for record in supervision_records:
            date_str = record.get('jcrq', '')
            if date_str:
                try:
                    # 尝试解析日期
                    from datetime import datetime
                    date_obj = datetime.strptime(str(date_str), '%Y-%m-%d')
                    dates.append(date_obj)
                except:
                    continue
        
        if dates:
            latest_date = max(dates)
            return latest_date.strftime('%Y-%m-%d')
        
        return None
    
    def _get_association_method(self, inspection_record: Dict, supervision_records: List[Dict]) -> str:
        """获取关联方法描述"""
        if not supervision_records:
            return "no_association"
        
        methods = []
        
        for sup_record in supervision_records:
            match_type, _ = self._determine_association_type(inspection_record, sup_record)
            methods.append(match_type)
        
        # 统计方法分布
        method_counts = {}
        for method in methods:
            method_counts[method] = method_counts.get(method, 0) + 1
        
        # 返回主要方法
        main_method = max(method_counts.items(), key=lambda x: x[1])[0]
        
        return f"{main_method} ({method_counts[main_method]}/{len(supervision_records)})"
    
    def _check_cross_system_consistency(self, inspection_record: Dict, supervision_records: List[Dict]) -> Dict:
        """检查跨系统数据一致性"""
        consistency = {
            'unit_name_consistent': True,
            'legal_person_consistent': True,
            'credit_code_consistent': True,
            'inconsistency_details': []
        }
        
        inspection_name = self._safe_str(inspection_record.get('UNIT_NAME'))
        inspection_legal = self._safe_str(inspection_record.get('LEGAL_PEOPLE'))
        inspection_credit = self._safe_str(inspection_record.get('CREDIT_CODE'))
        
        for sup_record in supervision_records:
            sup_name = self._safe_str(sup_record.get('dwmc'))
            sup_legal = self._safe_str(sup_record.get('fddbr'))
            sup_credit = self._safe_str(sup_record.get('tyshxydm'))
            
            # 检查单位名称一致性
            if inspection_name and sup_name:
                name_similarity = self._calculate_name_similarity(inspection_name, sup_name)
                if name_similarity < 0.8:
                    consistency['unit_name_consistent'] = False
                    consistency['inconsistency_details'].append(f"单位名称不一致: {inspection_name} vs {sup_name}")
            
            # 检查法定代表人一致性
            if inspection_legal and sup_legal:
                legal_similarity = self._calculate_name_similarity(inspection_legal, sup_legal)
                if legal_similarity < 0.8:
                    consistency['legal_person_consistent'] = False
                    consistency['inconsistency_details'].append(f"法定代表人不一致: {inspection_legal} vs {sup_legal}")
            
            # 检查信用代码一致性
            if inspection_credit and sup_credit and inspection_credit != sup_credit:
                consistency['credit_code_consistent'] = False
                consistency['inconsistency_details'].append(f"信用代码不一致: {inspection_credit} vs {sup_credit}")
        
        return consistency
    
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
        """获取关联统计信息"""
        try:
            collection = self.db_manager.get_collection('enhanced_association_results')
            
            pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'total_associations': {'$sum': 1},
                        'with_supervision_records': {
                            '$sum': {'$cond': [{'$gt': ['$supervision_record_count', 0]}, 1, 0]}
                        },
                        'without_supervision_records': {
                            '$sum': {'$cond': [{'$eq': ['$supervision_record_count', 0]}, 1, 0]}
                        },
                        'avg_supervision_records': {'$avg': '$supervision_record_count'},
                        'avg_data_quality': {'$avg': '$data_quality_score'},
                        'avg_association_confidence': {'$avg': '$association_confidence'},
                        'total_buildings': {'$sum': '$unit_building_count'},
                        'total_supervision_records': {'$sum': '$supervision_record_count'}
                    }
                }
            ]
            
            result = list(collection.aggregate(pipeline))
            
            if result:
                stats = result[0]
                stats.pop('_id', None)
                
                # 计算关联率
                if stats['total_associations'] > 0:
                    stats['association_rate'] = round(
                        stats['with_supervision_records'] / stats['total_associations'] * 100, 2
                    )
                else:
                    stats['association_rate'] = 0.0
                
                return stats
            else:
                return {
                    'total_associations': 0,
                    'with_supervision_records': 0,
                    'without_supervision_records': 0,
                    'association_rate': 0.0,
                    'avg_supervision_records': 0.0,
                    'avg_data_quality': 0.0,
                    'avg_association_confidence': 0.0,
                    'total_buildings': 0,
                    'total_supervision_records': 0
                }
                
        except Exception as e:
            logger.error(f"获取关联统计失败: {str(e)}")
            return {}