#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双重验证匹配器
实现主要字段先验证，次要字段后验证的严格匹配策略
确保最终保留阈值35%以上，过滤脏数据
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import time
from loguru import logger

from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.enhanced_fuzzy_matcher import EnhancedFuzzyMatcher
from src.matching.intelligent_unit_name_matcher import IntelligentUnitNameMatcher
from src.matching.content_field_analyzer import ContentFieldAnalyzer

@dataclass
class FieldValidationConfig:
    """字段验证配置"""
    field_name: str
    source_field: str
    target_field: str
    weight: float
    threshold: float
    is_primary: bool = False
    match_type: str = 'string'  # string, address, unit_name

@dataclass
class DualValidationResult:
    """双重验证结果"""
    is_valid: bool
    final_score: float
    primary_field_score: float
    secondary_field_score: float
    validation_details: Dict[str, Any]
    rejection_reason: Optional[str] = None
    passes_threshold: bool = False

class DualValidationMatcher:
    """
    双重验证匹配器
    
    匹配策略：
    1. 主要字段必须先达到阈值
    2. 次要字段只有在主要字段达标后才验证
    3. 两个字段都达标才计算最终得分
    4. 最终得分低于35%直接拒绝
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化双重验证匹配器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 初始化各种匹配器
        try:
            from src.utils.config import ConfigManager
            config_manager = ConfigManager()
            self.fuzzy_matcher = FuzzyMatcher(config_manager)
            self.enhanced_fuzzy_matcher = EnhancedFuzzyMatcher(config_manager)
            self.unit_name_matcher = IntelligentUnitNameMatcher()
        except Exception as e:
            logger.warning(f"初始化匹配器失败，使用简化配置: {str(e)}")
            # 使用简化配置
            simple_config = {'fuzzy_match': {'similarity_threshold': 0.6}}
            self.fuzzy_matcher = None
            self.enhanced_fuzzy_matcher = None
            self.unit_name_matcher = IntelligentUnitNameMatcher()
        
        # 初始化内容分析器
        self.content_analyzer = ContentFieldAnalyzer()
        
        # 验证配置
        self.validation_configs: List[FieldValidationConfig] = []
        
        # 全局阈值设置
        self.final_threshold = 0.35  # 最终保留阈值35%
        self.primary_field_threshold = 0.25  # 主要字段阈值25%（进一步降低，符合实际数据质量）
        self.secondary_field_threshold = 0.25  # 次要字段阈值25%（进一步降低，符合实际数据质量）
        
        logger.info(f"🔧 双重验证匹配器初始化完成")
        logger.info(f"   - 最终保留阈值: {self.final_threshold:.1%}")
        logger.info(f"   - 主要字段阈值: {self.primary_field_threshold:.1%}")
        logger.info(f"   - 次要字段阈值: {self.secondary_field_threshold:.1%}")
    
    def configure_fields(self, mappings: List[Dict[str, Any]], 
                        source_data: List[Dict] = None, 
                        target_data: List[Dict] = None) -> None:
        """
        配置字段验证规则（基于内容分析）
        
        Args:
            mappings: 字段映射配置列表
            source_data: 源数据样本（用于内容分析）
            target_data: 目标数据样本（用于内容分析）
        """
        self.validation_configs.clear()
        
        for mapping in mappings:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            weight = mapping.get('weight', 1.0)
            
            # 【新逻辑】基于内容分析字段类型
            is_primary = False
            match_type = 'string'
            threshold = self.secondary_field_threshold
            
            # 如果有样本数据，进行内容分析
            if source_data and target_data:
                # 提取字段值样本
                source_values = [record.get(source_field, '') for record in source_data[:10] if record.get(source_field)]
                target_values = [record.get(target_field, '') for record in target_data[:10] if record.get(target_field)]
                
                # 分析源字段类型
                source_analysis = self.content_analyzer.analyze_field_type(source_values)
                target_analysis = self.content_analyzer.analyze_field_type(target_values)
                
                source_type = source_analysis.get('field_type', 'unknown')
                target_type = target_analysis.get('field_type', 'unknown')
                source_confidence = source_analysis.get('confidence', 0.0)
                target_confidence = target_analysis.get('confidence', 0.0)
                
                # 基于内容分析结果判断字段类型
                if source_type == 'company_name' and target_type == 'company_name':
                    is_primary = True
                    match_type = 'unit_name'
                    threshold = self.primary_field_threshold
                    logger.info(f"🎯 内容分析识别主要字段: {source_field} -> {target_field}")
                    logger.info(f"   源字段置信度: {source_confidence:.2f}, 目标字段置信度: {target_confidence:.2f}")
                    logger.info(f"   权重: {weight}, 阈值: {threshold:.1%}")
                    
                elif source_type == 'address' and target_type == 'address':
                    is_primary = False
                    match_type = 'address'
                    threshold = self.secondary_field_threshold
                    logger.info(f"🏠 内容分析识别次要字段: {source_field} -> {target_field}")
                    logger.info(f"   源字段置信度: {source_confidence:.2f}, 目标字段置信度: {target_confidence:.2f}")
                    logger.info(f"   权重: {weight}, 阈值: {threshold:.1%}")
                    
                elif source_type != target_type and source_type != 'unknown' and target_type != 'unknown':
                    logger.warning(f"⚠️  字段类型不匹配: {source_field}({source_type}) -> {target_field}({target_type})")
                    logger.warning(f"   这可能导致匹配效果不佳")
                    
                else:
                    logger.info(f"🔍 无法确定字段类型: {source_field} -> {target_field}")
                    logger.info(f"   源字段类型: {source_type}({source_confidence:.2f}), 目标字段类型: {target_type}({target_confidence:.2f})")
                    
            else:
                # 【降级逻辑】如果没有样本数据，仍然使用字段名判断（但会记录警告）
                logger.warning(f"⚠️  缺少样本数据，降级使用字段名判断: {source_field} -> {target_field}")
                
                # 单位名称字段 - 主要字段
                if any(keyword in source_field.lower() for keyword in ['unit', 'name', 'dwmc', '单位', '名称', 'company', 'jgmc']):
                    is_primary = True
                    match_type = 'unit_name'
                    threshold = self.primary_field_threshold
                    logger.info(f"🎯 字段名识别主要字段: {source_field} -> {target_field} (权重: {weight}, 阈值: {threshold:.1%})")
                
                # 地址字段 - 次要字段
                elif any(keyword in source_field.lower() for keyword in ['address', 'addr', 'dwdz', '地址', '位置', 'dz']):
                    is_primary = False
                    match_type = 'address'
                    threshold = self.secondary_field_threshold
                    logger.info(f"🏠 字段名识别次要字段: {source_field} -> {target_field} (权重: {weight}, 阈值: {threshold:.1%})")
            
            config = FieldValidationConfig(
                field_name=f"{source_field}->{target_field}",
                source_field=source_field,
                target_field=target_field,
                weight=weight,
                threshold=threshold,
                is_primary=is_primary,
                match_type=match_type
            )
            
            self.validation_configs.append(config)
        
        # 验证配置完整性
        primary_fields = [c for c in self.validation_configs if c.is_primary]
        secondary_fields = [c for c in self.validation_configs if not c.is_primary]
        
        logger.info(f"✅ 字段配置完成: {len(primary_fields)}个主要字段, {len(secondary_fields)}个次要字段")
    
    def validate_match(self, source_record: Dict[str, Any], target_record: Dict[str, Any]) -> DualValidationResult:
        """
        执行双重验证匹配
        
        Args:
            source_record: 源记录
            target_record: 目标记录
            
        Returns:
            DualValidationResult: 验证结果
        """
        # 【加强安全检查】确保记录不为None
        if source_record is None or target_record is None:
            error_msg = f"双重验证匹配器输入验证失败: source_record={source_record is not None}, target_record={target_record is not None}"
            logger.error(error_msg)
            return DualValidationResult(
                is_valid=False,
                final_score=0.0,
                primary_field_score=0.0,
                secondary_field_score=0.0,
                validation_details={},
                rejection_reason="记录为空",
                passes_threshold=False
            )
        
        # 【加强类型检查】确保记录是字典类型
        if not isinstance(source_record, dict) or not isinstance(target_record, dict):
            error_msg = f"双重验证匹配器类型验证失败: source_type={type(source_record)}, target_type={type(target_record)}"
            logger.error(error_msg)
            return DualValidationResult(
                is_valid=False,
                final_score=0.0,
                primary_field_score=0.0,
                secondary_field_score=0.0,
                validation_details={},
                rejection_reason="记录类型错误",
                passes_threshold=False
            )
        start_time = time.time()
        
        # 第一步：验证主要字段
        primary_validation = self._validate_primary_fields(source_record, target_record)
        
        if not primary_validation['is_valid']:
            return DualValidationResult(
                is_valid=False,
                final_score=0.0,
                primary_field_score=primary_validation['score'],
                secondary_field_score=0.0,
                validation_details={
                    'primary_validation': primary_validation,
                    'secondary_validation': None,
                    'processing_time': time.time() - start_time
                },
                rejection_reason=primary_validation['rejection_reason'],
                passes_threshold=False
            )
        
        # 第二步：验证次要字段
        secondary_validation = self._validate_secondary_fields(source_record, target_record)
        
        if not secondary_validation['is_valid']:
            return DualValidationResult(
                is_valid=False,
                final_score=0.0,
                primary_field_score=primary_validation['score'],
                secondary_field_score=secondary_validation['score'],
                validation_details={
                    'primary_validation': primary_validation,
                    'secondary_validation': secondary_validation,
                    'processing_time': time.time() - start_time
                },
                rejection_reason=secondary_validation['rejection_reason'],
                passes_threshold=False
            )
        
        # 第三步：计算最终得分
        final_score = self._calculate_final_score(primary_validation, secondary_validation)
        
        # 第四步：应用最终阈值和边界情况检查
        is_final_valid = final_score >= self.final_threshold
        rejection_reason = None
        
        if not is_final_valid:
            rejection_reason = f"最终得分{final_score:.1%}低于保留阈值{self.final_threshold:.1%}"
        else:
            # 边界情况检查：避免信息量过少的匹配
            boundary_check_result = self._check_boundary_conditions(
                source_record, target_record, primary_validation, secondary_validation
            )
            if not boundary_check_result['is_valid']:
                is_final_valid = False
                rejection_reason = boundary_check_result['reason']
        
        result = DualValidationResult(
            is_valid=is_final_valid,
            final_score=final_score,
            primary_field_score=primary_validation['score'],
            secondary_field_score=secondary_validation['score'],
            validation_details={
                'primary_validation': primary_validation,
                'secondary_validation': secondary_validation,
                'final_score_calculation': {
                    'primary_weighted': primary_validation['score'] * primary_validation['total_weight'],
                    'secondary_weighted': secondary_validation['score'] * secondary_validation['total_weight'],
                    'total_weight': primary_validation['total_weight'] + secondary_validation['total_weight']
                },
                'processing_time': time.time() - start_time
            },
            rejection_reason=rejection_reason,
            passes_threshold=is_final_valid
        )
        
        # 记录验证结果
        if is_final_valid:
            logger.info(f"✅ 双重验证通过: 最终得分{final_score:.1%} (主要:{primary_validation['score']:.1%}, 次要:{secondary_validation['score']:.1%})")
        else:
            logger.info(f"❌ 双重验证失败: {rejection_reason}")
        
        return result
    
    def _validate_primary_fields(self, source_record: Dict[str, Any], target_record: Dict[str, Any]) -> Dict[str, Any]:
        """验证主要字段"""
        primary_configs = [c for c in self.validation_configs if c.is_primary]
        
        if not primary_configs:
            return {
                'is_valid': True,
                'score': 1.0,
                'total_weight': 0.0,
                'field_scores': {},
                'rejection_reason': None
            }
        
        total_score = 0.0
        total_weight = 0.0
        field_scores = {}
        failed_fields = []
        
        for config in primary_configs:
            source_value = str((source_record or {}).get(config.source_field, '')).strip()
            target_value = str((target_record or {}).get(config.target_field, '')).strip()
            
            if not source_value or not target_value:
                failed_fields.append(f"{config.field_name}(空值)")
                continue
            
            # 计算字段相似度
            field_score = self._calculate_field_similarity(
                source_value, target_value, config.match_type
            )
            
            field_scores[config.field_name] = field_score
            
            # 检查是否达到阈值（考虑同义词情况）
            effective_threshold = config.threshold
            
            # 如果是单位名称字段，检查是否包含同义词
            if config.match_type == 'unit_name':
                source_value = str((source_record or {}).get(config.source_field, '')).strip()
                target_value = str((target_record or {}).get(config.target_field, '')).strip()
                
                # 检查是否包含同义词对
                synonym_pairs = [
                    ('养老院', '护理院'),
                    ('老年护理院', '养老院'),
                    ('护理中心', '养老院'),
                ]
                
                for syn1, syn2 in synonym_pairs:
                    if ((syn1 in source_value and syn2 in target_value) or 
                        (syn2 in source_value and syn1 in target_value)):
                        # 对于同义词情况，降低阈值要求
                        effective_threshold = max(0.15, config.threshold * 0.4)  # 降低到原阈值的40%，但不低于15%
                        logger.debug(f"🔄 检测到同义词对 '{syn1}' <-> '{syn2}'，降低阈值至 {effective_threshold:.3f}")
                        break
            
            if field_score >= effective_threshold:
                total_score += field_score * config.weight
                total_weight += config.weight
                logger.debug(f"✅ 主要字段通过: {config.field_name} = {field_score:.3f} >= {effective_threshold:.3f}")
            else:
                failed_fields.append(f"{config.field_name}({field_score:.1%}<{effective_threshold:.1%})")
                logger.debug(f"❌ 主要字段失败: {config.field_name} = {field_score:.3f} < {effective_threshold:.3f}")
        
        # 计算主要字段平均得分
        avg_score = total_score / total_weight if total_weight > 0 else 0.0
        
        # 【新增优化逻辑】当关键映射字段有两个以上时，所有字段必须达到配置的阈值以上
        # 从配置中读取关键字段验证参数
        critical_validation_config = getattr(self, 'critical_validation_config', {
            'enabled': True,
            'minimum_threshold': 0.4,
            'minimum_field_count': 2
        })
        
        critical_field_threshold = critical_validation_config.get('minimum_threshold', 0.4)
        minimum_field_count = critical_validation_config.get('minimum_field_count', 2)
        validation_enabled = critical_validation_config.get('enabled', True)
        
        # 检查是否启用关键字段验证且满足最少字段数量要求
        if validation_enabled and len(primary_configs) >= minimum_field_count:
            # 检查所有关键字段是否都达到配置的阈值以上
            low_score_fields = []
            for config in primary_configs:
                field_score = field_scores.get(config.field_name, 0.0)
                if field_score < critical_field_threshold:
                    low_score_fields.append(f"{config.field_name}({field_score:.3f}<{critical_field_threshold})")
            
            # 如果有字段低于阈值，则整体匹配失败
            if low_score_fields:
                is_valid = False
                rejection_reason = f"关键字段评分不足(需≥{critical_field_threshold}): {', '.join(low_score_fields)}"
                logger.info(f"🚫 多关键字段匹配失败: {rejection_reason}")
            else:
                # 所有关键字段都达标，再检查原有的失败字段逻辑
                is_valid = len(failed_fields) == 0 and avg_score > 0
                if failed_fields:
                    rejection_reason = f"主要字段验证失败: {', '.join(failed_fields)}"
                else:
                    logger.info(f"✅ 多关键字段匹配成功: 所有{len(primary_configs)}个关键字段均≥{critical_field_threshold}")
        else:
            # 单个关键字段或未启用验证时，使用原有逻辑
            is_valid = len(failed_fields) == 0 and avg_score > 0
            if failed_fields:
                rejection_reason = f"主要字段验证失败: {', '.join(failed_fields)}"
        
        return {
            'is_valid': is_valid,
            'score': avg_score,
            'total_weight': total_weight,
            'field_scores': field_scores,
            'failed_fields': failed_fields,
            'rejection_reason': rejection_reason
        }
    
    def _validate_secondary_fields(self, source_record: Dict[str, Any], target_record: Dict[str, Any]) -> Dict[str, Any]:
        """验证次要字段"""
        secondary_configs = [c for c in self.validation_configs if not c.is_primary]
        
        if not secondary_configs:
            return {
                'is_valid': True,
                'score': 1.0,
                'total_weight': 0.0,
                'field_scores': {},
                'rejection_reason': None
            }
        
        total_score = 0.0
        total_weight = 0.0
        field_scores = {}
        failed_fields = []
        
        for config in secondary_configs:
            source_value = str((source_record or {}).get(config.source_field, '')).strip()
            target_value = str((target_record or {}).get(config.target_field, '')).strip()
            
            if not source_value or not target_value:
                failed_fields.append(f"{config.field_name}(空值)")
                continue
            
            # 计算字段相似度
            field_score = self._calculate_field_similarity(
                source_value, target_value, config.match_type
            )
            
            field_scores[config.field_name] = field_score
            
            # 检查是否达到阈值
            if field_score >= config.threshold:
                total_score += field_score * config.weight
                total_weight += config.weight
                logger.debug(f"✅ 次要字段通过: {config.field_name} = {field_score:.3f} >= {config.threshold:.3f}")
            else:
                failed_fields.append(f"{config.field_name}({field_score:.1%}<{config.threshold:.1%})")
                logger.debug(f"❌ 次要字段失败: {config.field_name} = {field_score:.3f} < {config.threshold:.3f}")
        
        # 计算次要字段平均得分
        avg_score = total_score / total_weight if total_weight > 0 else 0.0
        is_valid = len(failed_fields) == 0 and avg_score > 0
        
        rejection_reason = None
        if failed_fields:
            rejection_reason = f"次要字段验证失败: {', '.join(failed_fields)}"
        
        return {
            'is_valid': is_valid,
            'score': avg_score,
            'total_weight': total_weight,
            'field_scores': field_scores,
            'failed_fields': failed_fields,
            'rejection_reason': rejection_reason
        }
    
    def _calculate_field_similarity(self, value1: str, value2: str, match_type: str) -> float:
        """
        计算字段相似度
        
        Args:
            value1: 值1
            value2: 值2
            match_type: 匹配类型 (string, address, unit_name)
            
        Returns:
            float: 相似度 (0-1)
        """
        try:
            if match_type == 'unit_name':
                # 使用智能单位名称匹配器
                return self.unit_name_matcher.calculate_similarity(value1, value2)
            elif match_type == 'address':
                # 使用专门的地址相似度计算
                return self._calculate_address_similarity(value1, value2)
            else:
                # 使用增强模糊匹配器
                if self.enhanced_fuzzy_matcher:
                    return self.enhanced_fuzzy_matcher.calculate_similarity(value1, value2)
                else:
                    # 简单的字符串相似度计算
                    return self._simple_similarity(value1, value2)
        except Exception as e:
            logger.warning(f"计算字段相似度失败: {str(e)}")
            return 0.0
    
    def _calculate_final_score(self, primary_validation: Dict[str, Any], secondary_validation: Dict[str, Any]) -> float:
        """
        计算最终得分
        
        Args:
            primary_validation: 主要字段验证结果
            secondary_validation: 次要字段验证结果
            
        Returns:
            float: 最终得分 (0-1)
        """
        primary_weighted = primary_validation['score'] * primary_validation['total_weight']
        secondary_weighted = secondary_validation['score'] * secondary_validation['total_weight']
        total_weight = primary_validation['total_weight'] + secondary_validation['total_weight']
        
        if total_weight == 0:
            return 0.0
        
        final_score = (primary_weighted + secondary_weighted) / total_weight
        
        logger.debug(f"🧮 最终得分计算: "
                    f"主要({primary_validation['score']:.3f}×{primary_validation['total_weight']:.1f}) + "
                    f"次要({secondary_validation['score']:.3f}×{secondary_validation['total_weight']:.1f}) "
                    f"/ {total_weight:.1f} = {final_score:.3f}")
        
        return final_score

    def batch_validate(self, source_records: List[Dict[str, Any]], 
                      target_records: List[Dict[str, Any]]) -> List[Tuple[int, int, DualValidationResult]]:
        """
        批量验证匹配
        
        Args:
            source_records: 源记录列表
            target_records: 目标记录列表
            
        Returns:
            List[Tuple[int, int, DualValidationResult]]: (源索引, 目标索引, 验证结果)
        """
        results = []
        total_pairs = len(source_records) * len(target_records)
        processed = 0
        
        logger.info(f"🚀 开始批量验证: {len(source_records)}条源记录 × {len(target_records)}条目标记录 = {total_pairs}对")
        
        for i, source_record in enumerate(source_records):
            for j, target_record in enumerate(target_records):
                validation_result = self.validate_match(source_record, target_record)
                
                if validation_result.is_valid:
                    results.append((i, j, validation_result))
                
                processed += 1
                if processed % 1000 == 0:
                    logger.info(f"📊 批量验证进度: {processed}/{total_pairs} ({processed/total_pairs:.1%})")
        
        logger.info(f"✅ 批量验证完成: {len(results)}个有效匹配 / {total_pairs}对 ({len(results)/total_pairs:.1%})")
        
        return results
    
    def _simple_similarity(self, value1: str, value2: str) -> float:
        """
        简单的字符串相似度计算（降级方案）
        
        Args:
            value1: 字符串1
            value2: 字符串2
            
        Returns:
            float: 相似度 (0-1)
        """
        if not value1 or not value2:
            return 0.0
        
        # 转换为小写并去除空格
        v1 = value1.lower().replace(' ', '')
        v2 = value2.lower().replace(' ', '')
        
        if v1 == v2:
            return 1.0
        
        # 计算最长公共子序列长度
        def lcs_length(s1, s2):
            m, n = len(s1), len(s2)
            dp = [[0] * (n + 1) for _ in range(m + 1)]
            
            for i in range(1, m + 1):
                for j in range(1, n + 1):
                    if s1[i-1] == s2[j-1]:
                        dp[i][j] = dp[i-1][j-1] + 1
                    else:
                        dp[i][j] = max(dp[i-1][j], dp[i][j-1])
            
            return dp[m][n]
        
        # 基于最长公共子序列的相似度
        lcs_len = lcs_length(v1, v2)
        max_len = max(len(v1), len(v2))
        
        if max_len == 0:
            return 0.0
        
        return lcs_len / max_len
    
    def _calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """
        计算地址相似度（更严格的地址匹配逻辑）
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            float: 相似度 (0-1)
        """
        if not addr1 or not addr2:
            return 0.0
        
        # 标准化地址
        def normalize_address(addr):
            # 移除常见的地址后缀
            suffixes = ['号', '室', '楼', '层', '单元', '栋', '幢']
            normalized = addr.strip()
            for suffix in suffixes:
                if normalized.endswith(suffix):
                    normalized = normalized[:-len(suffix)]
            return normalized.lower().replace(' ', '')
        
        norm_addr1 = normalize_address(addr1)
        norm_addr2 = normalize_address(addr2)
        
        # 完全匹配
        if norm_addr1 == norm_addr2:
            return 1.0
        
        # 提取关键地址组件
        def extract_address_components(addr):
            components = {
                'province': '',
                'city': '',
                'district': '',
                'street': '',
                'number': ''
            }
            
            # 简单的地址组件提取
            import re
            
            # 提取省份
            province_match = re.search(r'(.*?省)', addr)
            if province_match:
                components['province'] = province_match.group(1)
            
            # 提取市
            city_match = re.search(r'(.*?市)', addr)
            if city_match:
                components['city'] = city_match.group(1)
            
            # 提取区县
            district_match = re.search(r'(.*?[区县])', addr)
            if district_match:
                components['district'] = district_match.group(1)
            
            # 提取街道
            street_match = re.search(r'(.*?[路街道巷弄])', addr)
            if street_match:
                components['street'] = street_match.group(1)
            
            # 提取门牌号
            number_match = re.search(r'(\d+号?)', addr)
            if number_match:
                components['number'] = number_match.group(1)
            
            return components
        
        comp1 = extract_address_components(addr1)
        comp2 = extract_address_components(addr2)
        
        # 计算各组件相似度
        component_scores = {}
        component_weights = {
            'province': 0.1,
            'city': 0.2,
            'district': 0.25,
            'street': 0.3,
            'number': 0.35  # 门牌号最重要
        }
        
        total_score = 0.0
        total_weight = 0.0
        
        for component, weight in component_weights.items():
            val1 = comp1.get(component, '')
            val2 = comp2.get(component, '')
            
            if val1 and val2:
                # 计算组件相似度
                if val1 == val2:
                    comp_sim = 1.0
                else:
                    comp_sim = self._simple_similarity(val1, val2)
                
                component_scores[component] = comp_sim
                total_score += comp_sim * weight
                total_weight += weight
        
        # 如果没有匹配的组件，使用简单字符串相似度
        if total_weight == 0:
            return self._simple_similarity(norm_addr1, norm_addr2)
        
        # 计算加权平均相似度
        weighted_similarity = total_score / total_weight
        
        # 应用严格性惩罚：如果门牌号不匹配，大幅降低相似度
        if comp1.get('number') and comp2.get('number'):
            if comp1['number'] != comp2['number']:
                # 门牌号不匹配，相似度降低50%
                weighted_similarity *= 0.5
                logger.debug(f"地址门牌号不匹配，相似度降低: {comp1['number']} vs {comp2['number']}")
        
        # 应用最终阈值：地址相似度低于60%认为不匹配
        if weighted_similarity < 0.6:
            weighted_similarity *= 0.5  # 进一步降低
        
        logger.debug(f"地址相似度计算: {addr1} <-> {addr2} = {weighted_similarity:.3f}")
        logger.debug(f"  组件得分: {component_scores}")
        
        return min(weighted_similarity, 1.0)
    
    def _check_boundary_conditions(self, source_record: Dict[str, Any], target_record: Dict[str, Any],
                                 primary_validation: Dict[str, Any], secondary_validation: Dict[str, Any]) -> Dict[str, Any]:
        """
        检查边界情况，避免信息量过少或质量过低的匹配
        
        Args:
            source_record: 源记录
            target_record: 目标记录
            primary_validation: 主要字段验证结果
            secondary_validation: 次要字段验证结果
            
        Returns:
            Dict[str, Any]: 边界检查结果
        """
        # 获取主要字段和次要字段的值
        primary_configs = [c for c in self.validation_configs if c.is_primary]
        secondary_configs = [c for c in self.validation_configs if not c.is_primary]
        
        # 检查1：信息量过少（字段值太短）
        for config in primary_configs:
            source_value = str((source_record or {}).get(config.source_field, '')).strip()
            target_value = str((target_record or {}).get(config.target_field, '')).strip()
            
            # 放宽条件：只有当字段值少于2个字符时才认为信息量不足
            if len(source_value) < 2 or len(target_value) < 2:
                return {
                    'is_valid': False,
                    'reason': f'主要字段信息量严重不足: "{source_value}" vs "{target_value}" (长度<2)'
                }
        
        # 检查2：单位名称语义冲突检查
        for config in primary_configs:
            if config.match_type == 'unit_name':
                source_value = str((source_record or {}).get(config.source_field, '')).strip()
                target_value = str((target_record or {}).get(config.target_field, '')).strip()
                
                # 检查是否存在语义冲突（如：养老院 vs 护理院）
                conflict_result = self._check_unit_name_semantic_conflict(source_value, target_value)
                if conflict_result['has_conflict']:
                    # 如果存在语义冲突但相似度很高，可能是同义词，需要更严格的验证
                    field_scores = (primary_validation or {}).get('field_scores', {}) or {}
                    field_score = field_scores.get(config.field_name, 0.0)
                    if field_score < 0.5:  # 放宽语义冲突检查，降低到50%
                        return {
                            'is_valid': False,
                            'reason': f'单位名称语义冲突: {conflict_result["reason"]} (相似度{field_score:.1%}<50%)'
                        }
        
        # 检查3：地址精确度检查
        for config in secondary_configs:
            if config.match_type == 'address':
                source_value = str((source_record or {}).get(config.source_field, '')).strip()
                target_value = str((target_record or {}).get(config.target_field, '')).strip()
                
                # 检查地址精确度
                precision_result = self._check_address_precision(source_value, target_value)
                if not precision_result['is_precise']:
                    return {
                        'is_valid': False,
                        'reason': f'地址精确度不足: {precision_result["reason"]}'
                    }
        
        return {'is_valid': True, 'reason': None}
    
    def _check_unit_name_semantic_conflict(self, name1: str, name2: str) -> Dict[str, Any]:
        """
        检查单位名称语义冲突
        
        Args:
            name1: 单位名称1
            name2: 单位名称2
            
        Returns:
            Dict[str, Any]: 冲突检查结果
        """
        # 定义可能冲突的词汇对
        conflict_pairs = [
            # ('养老院', '护理院'),  # 这两个是同义词，不应该视为冲突
            ('公司', '院'),       # 公司 vs 院所，差异较大
            ('集团', '分公司'),   # 集团 vs 分公司，层级不同
            ('总部', '分部'),     # 总部 vs 分部，层级不同
            ('医院', '诊所'),     # 医院 vs 诊所，规模不同
        ]
        
        name1_lower = name1.lower()
        name2_lower = name2.lower()
        
        for word1, word2 in conflict_pairs:
            if (word1 in name1_lower and word2 in name2_lower) or (word2 in name1_lower and word1 in name2_lower):
                return {
                    'has_conflict': True,
                    'reason': f'检测到语义冲突词汇: "{word1}" vs "{word2}"'
                }
        
        return {'has_conflict': False, 'reason': None}
    
    def _check_address_precision(self, addr1: str, addr2: str) -> Dict[str, Any]:
        """
        检查地址精确度
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            Dict[str, Any]: 精确度检查结果
        """
        import re
        
        # 提取门牌号
        def extract_house_number(addr):
            match = re.search(r'(\d+)号?', addr)
            return match.group(1) if match else None
        
        num1 = extract_house_number(addr1)
        num2 = extract_house_number(addr2)
        
        # 如果两个地址都有门牌号但不一致，检查差异程度
        if num1 and num2 and num1 != num2:
            try:
                diff = abs(int(num1) - int(num2))
                # 放宽条件：如果门牌号差异超过50，认为是不同地址
                if diff > 50:
                    return {
                        'is_precise': False,
                        'reason': f'门牌号差异过大: {num1}号 vs {num2}号 (差异{diff})'
                    }
            except ValueError:
                # 门牌号不是纯数字，直接比较
                if num1 != num2:
                    return {
                        'is_precise': False,
                        'reason': f'门牌号不匹配: {num1} vs {num2}'
                    }
        
        # 检查街道名称
        def extract_street_name(addr):
            match = re.search(r'([^市区县]*?[路街道巷弄])', addr)
            return match.group(1) if match else None
        
        street1 = extract_street_name(addr1)
        street2 = extract_street_name(addr2)
        
        # 如果街道名称完全不同，认为精确度不足
        if street1 and street2 and street1 != street2:
            # 计算街道名称相似度
            street_sim = self._simple_similarity(street1, street2)
            if street_sim < 0.7:  # 街道相似度低于70%
                return {
                    'is_precise': False,
                    'reason': f'街道名称差异过大: "{street1}" vs "{street2}" (相似度{street_sim:.1%})'
                }
        
        return {'is_precise': True, 'reason': None}
