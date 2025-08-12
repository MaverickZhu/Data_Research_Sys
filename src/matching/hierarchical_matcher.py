"""
分层匹配处理器
实现基于主要字段和辅助字段的智能分层匹配逻辑
"""

import logging
import time
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class FieldMapping:
    """字段映射配置"""
    source_field: str
    target_field: str
    target_table: str
    field_priority: str  # 'primary' or 'secondary'
    weight: float
    threshold_config: Optional[Dict[str, float]] = None
    similarity_score: float = 0.0

@dataclass
class MatchResult:
    """匹配结果"""
    candidate: Dict[str, Any]
    final_score: float
    primary_score: float
    secondary_score: Optional[float]
    match_type: str
    field_scores: Dict[str, float]
    confidence_level: str
    
class HierarchicalMatcher:
    """分层匹配处理器"""
    
    def __init__(self, mapping_config: List[Dict[str, Any]], similarity_calculator=None):
        """
        初始化分层匹配器
        
        Args:
            mapping_config: 字段映射配置列表
            similarity_calculator: 相似度计算器实例
        """
        self.mapping_config = mapping_config
        self.similarity_calculator = similarity_calculator
        
        # 解析映射配置
        self.primary_fields = []
        self.secondary_fields = []
        self.field_mappings = []
        self.threshold_config = {
            'high_threshold': 0.9,
            'medium_threshold': 0.6,
            'low_threshold': 0.3
        }
        
        self._parse_mapping_config()
        
        logger.info(f"分层匹配器初始化完成: 主要字段={len(self.primary_fields)}, 辅助字段={len(self.secondary_fields)}")
    
    def _parse_mapping_config(self):
        """解析映射配置"""
        for config in self.mapping_config:
            field_mapping = FieldMapping(
                source_field=config.get('source_field'),
                target_field=config.get('target_field'),
                target_table=config.get('target_table'),
                field_priority=config.get('field_priority', 'primary'),
                weight=config.get('weight', 0.5),
                threshold_config=config.get('threshold_config'),
                similarity_score=config.get('similarity_score', 0.0)
            )
            
            self.field_mappings.append(field_mapping)
            
            if field_mapping.field_priority == 'primary':
                self.primary_fields.append(field_mapping)
                # 更新阈值配置（使用任何一个主要字段的配置，优先使用更严格的阈值）
                if field_mapping.threshold_config:
                    if not hasattr(self, '_threshold_configs'):
                        self._threshold_configs = []
                    self._threshold_configs.append(field_mapping.threshold_config)
            else:
                self.secondary_fields.append(field_mapping)
        
        # 合并多个主要字段的阈值配置（取平均值或使用最严格的配置）
        if hasattr(self, '_threshold_configs') and self._threshold_configs:
            self._merge_threshold_configs()
    
    def _merge_threshold_configs(self):
        """合并多个主要字段的阈值配置"""
        if not self._threshold_configs:
            return
            
        # 计算所有主要字段阈值的平均值
        high_thresholds = [config.get('high_threshold', 0.9) for config in self._threshold_configs]
        medium_thresholds = [config.get('medium_threshold', 0.6) for config in self._threshold_configs]
        low_thresholds = [config.get('low_threshold', 0.3) for config in self._threshold_configs]
        
        # 使用平均值作为最终阈值配置
        self.threshold_config = {
            'high_threshold': sum(high_thresholds) / len(high_thresholds),
            'medium_threshold': sum(medium_thresholds) / len(medium_thresholds),
            'low_threshold': sum(low_thresholds) / len(low_thresholds)
        }
        
        logger.info(f"合并阈值配置: 高阈值={self.threshold_config['high_threshold']:.2f}, "
                   f"中等阈值={self.threshold_config['medium_threshold']:.2f}, "
                   f"基于{len(self._threshold_configs)}个主要字段")
    
    def match_record(self, source_record: Dict[str, Any], candidate_records: List[Dict[str, Any]]) -> List[MatchResult]:
        """
        分层匹配单条记录
        
        Args:
            source_record: 源记录
            candidate_records: 候选记录列表
            
        Returns:
            排序后的匹配结果列表
        """
        results = []
        
        for candidate in candidate_records:
            try:
                match_result = self._match_single_candidate(source_record, candidate)
                if match_result:
                    results.append(match_result)
            except Exception as e:
                logger.warning(f"匹配候选记录时出错: {e}")
                continue
        
        # 按最终得分排序
        results.sort(key=lambda x: x.final_score, reverse=True)
        return results
    
    def _match_single_candidate(self, source_record: Dict[str, Any], candidate: Dict[str, Any]) -> Optional[MatchResult]:
        """
        匹配单个候选记录
        
        Args:
            source_record: 源记录
            candidate: 候选记录
            
        Returns:
            匹配结果或None
        """
        # 第一层：计算主要字段得分
        primary_score, primary_field_scores = self._calculate_primary_score(source_record, candidate)
        
        # 根据主要字段得分决定匹配策略
        if primary_score >= self.threshold_config['high_threshold']:
            # 高阈值：直接通过，不计算辅助字段
            final_score = primary_score
            secondary_score = None
            match_type = "primary_high_confidence"
            confidence_level = "high"
            field_scores = primary_field_scores
            
            logger.debug(f"高置信度匹配: 主要得分={primary_score:.3f} >= 高阈值={self.threshold_config['high_threshold']}")
            
        elif primary_score >= self.threshold_config['medium_threshold']:
            # 中等阈值：计算辅助字段
            secondary_score, secondary_field_scores = self._calculate_secondary_score(source_record, candidate)
            final_score = self._combine_scores(primary_score, secondary_score)
            match_type = "primary_secondary_combined"
            confidence_level = "medium"
            
            # 合并字段得分
            field_scores = {**primary_field_scores, **secondary_field_scores}
            
            logger.debug(f"中等置信度匹配: 主要得分={primary_score:.3f}, 辅助得分={secondary_score:.3f}, 最终得分={final_score:.3f}")
            
        else:
            # 低阈值：主要字段不匹配，跳过
            logger.debug(f"低置信度跳过: 主要得分={primary_score:.3f} < 中等阈值={self.threshold_config['medium_threshold']}")
            return None
        
        return MatchResult(
            candidate=candidate,
            final_score=final_score,
            primary_score=primary_score,
            secondary_score=secondary_score,
            match_type=match_type,
            field_scores=field_scores,
            confidence_level=confidence_level
        )
    
    def _calculate_primary_score(self, source_record: Dict[str, Any], candidate: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
        """
        计算主要字段得分
        
        Args:
            source_record: 源记录
            candidate: 候选记录
            
        Returns:
            (主要字段得分, 字段级别得分字典)
        """
        if not self.primary_fields:
            return 0.0, {}
        
        total_weighted_score = 0.0
        total_weight = 0.0
        field_scores = {}
        
        for field_mapping in self.primary_fields:
            source_value = source_record.get(field_mapping.source_field, '')
            target_value = candidate.get(field_mapping.target_field, '')
            
            # 计算字段相似度
            similarity = self._calculate_field_similarity(source_value, target_value)
            field_scores[f"{field_mapping.source_field}->{field_mapping.target_field}"] = similarity
            
            # 加权计算
            weighted_score = similarity * field_mapping.weight
            total_weighted_score += weighted_score
            total_weight += field_mapping.weight
        
        # 计算平均加权得分
        primary_score = total_weighted_score / total_weight if total_weight > 0 else 0.0
        
        return primary_score, field_scores
    
    def _calculate_secondary_score(self, source_record: Dict[str, Any], candidate: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
        """
        计算辅助字段得分
        
        Args:
            source_record: 源记录
            candidate: 候选记录
            
        Returns:
            (辅助字段得分, 字段级别得分字典)
        """
        if not self.secondary_fields:
            return 0.0, {}
        
        total_weighted_score = 0.0
        total_weight = 0.0
        field_scores = {}
        
        for field_mapping in self.secondary_fields:
            source_value = source_record.get(field_mapping.source_field, '')
            target_value = candidate.get(field_mapping.target_field, '')
            
            # 计算字段相似度
            similarity = self._calculate_field_similarity(source_value, target_value)
            field_scores[f"{field_mapping.source_field}->{field_mapping.target_field}"] = similarity
            
            # 加权计算
            weighted_score = similarity * field_mapping.weight
            total_weighted_score += weighted_score
            total_weight += field_mapping.weight
        
        # 计算平均加权得分
        secondary_score = total_weighted_score / total_weight if total_weight > 0 else 0.0
        
        return secondary_score, field_scores
    
    def _calculate_field_similarity(self, source_value: Any, target_value: Any) -> float:
        """
        计算字段相似度
        
        Args:
            source_value: 源字段值
            target_value: 目标字段值
            
        Returns:
            相似度得分 (0-1)
        """
        if self.similarity_calculator:
            try:
                return self.similarity_calculator.calculate_string_similarity(str(source_value), str(target_value))
            except Exception as e:
                logger.warning(f"相似度计算失败: {e}")
        
        # 简单的字符串相似度计算
        return self._simple_string_similarity(str(source_value), str(target_value))
    
    def _simple_string_similarity(self, str1: str, str2: str) -> float:
        """
        简单的字符串相似度计算
        
        Args:
            str1: 字符串1
            str2: 字符串2
            
        Returns:
            相似度得分 (0-1)
        """
        if not str1 or not str2:
            return 0.0
        
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()
        
        # 完全匹配
        if str1 == str2:
            return 1.0
        
        # 包含关系
        if str1 in str2 or str2 in str1:
            return 0.8
        
        # 简单的编辑距离相似度
        max_len = max(len(str1), len(str2))
        if max_len == 0:
            return 1.0
        
        # 计算编辑距离
        distance = self._levenshtein_distance(str1, str2)
        similarity = max(0.0, (max_len - distance) / max_len)
        
        return similarity
    
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """计算编辑距离"""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
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
    
    def _combine_scores(self, primary_score: float, secondary_score: float) -> float:
        """
        合并主要字段和辅助字段得分
        
        Args:
            primary_score: 主要字段得分
            secondary_score: 辅助字段得分
            
        Returns:
            合并后的最终得分
        """
        # 主要字段权重更高 (70% vs 30%)
        primary_weight = 0.7
        secondary_weight = 0.3
        
        final_score = (primary_score * primary_weight) + (secondary_score * secondary_weight)
        return min(1.0, final_score)  # 确保不超过1.0
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        获取性能统计信息
        
        Returns:
            性能统计字典
        """
        return {
            'total_fields': len(self.field_mappings),
            'primary_fields': len(self.primary_fields),
            'secondary_fields': len(self.secondary_fields),
            'threshold_config': self.threshold_config,
            'primary_field_names': [f.source_field for f in self.primary_fields],
            'secondary_field_names': [f.source_field for f in self.secondary_fields]
        }
