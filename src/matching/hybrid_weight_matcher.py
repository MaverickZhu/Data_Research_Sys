#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
混合权重匹配器
基于算法分析结果，设计分层匹配和动态权重分配机制
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
from src.matching.optimized_fuzzy_matcher import OptimizedFuzzyMatcher
from src.matching.intelligent_unit_name_matcher import IntelligentUnitNameMatcher
from src.utils.config import ConfigManager

class MatchingMode(Enum):
    """匹配模式枚举"""
    PERFORMANCE = "performance"  # 性能优先
    ACCURACY = "accuracy"       # 准确性优先
    BALANCED = "balanced"       # 平衡模式
    INTERACTIVE = "interactive" # 交互式
    AUDIT = "audit"            # 审核模式

class DataQuality(Enum):
    """数据质量枚举"""
    HIGH = "high"      # 高质量
    MEDIUM = "medium"  # 中等质量
    LOW = "low"        # 低质量
    UNKNOWN = "unknown" # 未知质量

@dataclass
class MatchingContext:
    """匹配上下文"""
    mode: MatchingMode = MatchingMode.BALANCED
    data_quality: DataQuality = DataQuality.UNKNOWN
    batch_size: int = 1
    time_limit: Optional[float] = None
    accuracy_requirement: float = 0.8
    performance_requirement: float = 1.0  # 秒

@dataclass
class HybridMatchResult:
    """混合匹配结果"""
    matched: bool
    best_match: Optional[Dict[str, Any]]
    similarity_score: float
    confidence_score: float
    algorithm_used: str
    field_scores: Dict[str, float]
    processing_time: float
    explanation: str

class HybridWeightMatcher:
    """混合权重匹配器"""
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化混合权重匹配器"""
        self.config = config or self._get_default_config()
        
        # 初始化各种匹配器
        self.fuzzy_matcher = FuzzyMatcher(self.config)
        self.enhanced_fuzzy_matcher = EnhancedFuzzyMatcher(self.config)
        self.optimized_fuzzy_matcher = OptimizedFuzzyMatcher(self.config)
        self.intelligent_matcher = IntelligentUnitNameMatcher()
        
        # 算法权重配置
        self.algorithm_weights = self._initialize_algorithm_weights()
        
        logger.info("混合权重匹配器初始化完成")
    
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        try:
            config_manager = ConfigManager()
            return config_manager.get_matching_config()
        except Exception as e:
            logger.warning(f"配置加载失败，使用默认配置: {e}")
            return {
                'fuzzy_match': {
                    'similarity_threshold': 0.75,
                    'fields': {
                        'unit_name': {'weight': 0.4, 'source_field': 'UNIT_NAME', 'target_field': 'dwmc'},
                        'address': {'weight': 0.3, 'source_field': 'ADDRESS', 'target_field': 'dwdz'}
                    }
                }
            }
    
    def _initialize_algorithm_weights(self) -> Dict[MatchingMode, Dict[str, float]]:
        """初始化不同模式下的算法权重"""
        return {
            MatchingMode.PERFORMANCE: {
                'optimized_fuzzy': 0.7,
                'fuzzy': 0.2,
                'intelligent': 0.1,
                'enhanced': 0.0
            },
            MatchingMode.ACCURACY: {
                'intelligent': 0.5,
                'enhanced': 0.3,
                'fuzzy': 0.2,
                'optimized_fuzzy': 0.0
            },
            MatchingMode.BALANCED: {
                'intelligent': 0.4,
                'optimized_fuzzy': 0.3,
                'enhanced': 0.2,
                'fuzzy': 0.1
            },
            MatchingMode.INTERACTIVE: {
                'intelligent': 0.4,
                'enhanced': 0.3,
                'optimized_fuzzy': 0.2,
                'fuzzy': 0.1
            },
            MatchingMode.AUDIT: {
                'intelligent': 0.4,
                'enhanced': 0.4,
                'fuzzy': 0.2,
                'optimized_fuzzy': 0.0
            }
        }
    
    def _assess_data_quality(self, source_record: Dict, target_records: List[Dict]) -> DataQuality:
        """评估数据质量"""
        try:
            # 检查字段完整性
            source_completeness = sum(1 for v in source_record.values() if v and str(v).strip())
            source_total = len(source_record)
            
            target_completeness = 0
            target_total = 0
            for record in target_records[:5]:  # 只检查前5条
                target_completeness += sum(1 for v in record.values() if v and str(v).strip())
                target_total += len(record)
            
            if target_total > 0:
                avg_completeness = (source_completeness / source_total + 
                                  target_completeness / target_total) / 2
            else:
                avg_completeness = source_completeness / source_total
            
            # 检查关键字段质量
            unit_name = source_record.get('UNIT_NAME', '')
            if not unit_name or len(unit_name.strip()) < 3:
                return DataQuality.LOW
            
            # 根据完整性评估质量
            if avg_completeness >= 0.8:
                return DataQuality.HIGH
            elif avg_completeness >= 0.5:
                return DataQuality.MEDIUM
            else:
                return DataQuality.LOW
                
        except Exception as e:
            logger.warning(f"数据质量评估失败: {e}")
            return DataQuality.UNKNOWN
    
    def _adjust_weights_by_quality(self, base_weights: Dict[str, float], 
                                  data_quality: DataQuality) -> Dict[str, float]:
        """根据数据质量调整权重"""
        adjusted_weights = base_weights.copy()
        
        if data_quality == DataQuality.HIGH:
            # 高质量数据，提高智能匹配权重
            adjusted_weights['intelligent'] = min(1.0, adjusted_weights['intelligent'] * 1.2)
            adjusted_weights['optimized_fuzzy'] = max(0.0, adjusted_weights['optimized_fuzzy'] * 0.8)
            
        elif data_quality == DataQuality.LOW:
            # 低质量数据，提高模糊匹配权重
            adjusted_weights['fuzzy'] = min(1.0, adjusted_weights['fuzzy'] * 1.3)
            adjusted_weights['enhanced'] = min(1.0, adjusted_weights['enhanced'] * 1.2)
            adjusted_weights['intelligent'] = max(0.0, adjusted_weights['intelligent'] * 0.7)
        
        # 归一化权重
        total_weight = sum(adjusted_weights.values())
        if total_weight > 0:
            adjusted_weights = {k: v / total_weight for k, v in adjusted_weights.items()}
        
        return adjusted_weights
    
    def _run_algorithm(self, algorithm_name: str, source_record: Dict, 
                      target_records: List[Dict]) -> Tuple[float, Dict, str]:
        """运行指定算法"""
        try:
            start_time = time.time()
            
            if algorithm_name == 'fuzzy':
                result = self.fuzzy_matcher.match_single_record(source_record, target_records)
                score = result.similarity_score if result.matched else 0.0
                best_match = result.target_record if result.matched else None
                explanation = f"基础模糊匹配 (阈值: {self.config.get('fuzzy_match', {}).get('similarity_threshold', 0.75)})"
                
            elif algorithm_name == 'enhanced':
                result = self.enhanced_fuzzy_matcher.match_single_record(source_record, target_records)
                score = result.similarity_score if result.matched else 0.0
                best_match = result.target_record if result.matched else None
                explanation = f"增强模糊匹配 (结构化验证)"
                
            elif algorithm_name == 'optimized_fuzzy':
                result = self.optimized_fuzzy_matcher.match_single_record_optimized(source_record, target_records)
                score = result['similarity_score'] if result['matched'] else 0.0
                best_match = result['matched_record'] if result['matched'] else None
                explanation = f"优化模糊匹配 (快速预筛选)"
                
            elif algorithm_name == 'intelligent':
                # 智能匹配只处理单位名称
                source_name = source_record.get('UNIT_NAME', '')
                best_score = 0.0
                best_target = None
                
                for target in target_records:
                    target_name = target.get('dwmc', '')
                    similarity = self.intelligent_matcher.calculate_similarity(source_name, target_name)
                    if similarity > best_score:
                        best_score = similarity
                        best_target = target
                
                score = best_score
                best_match = best_target if best_score > 0.5 else None
                explanation = f"智能单位名称匹配 (语义理解)"
                
            else:
                raise ValueError(f"未知算法: {algorithm_name}")
            
            processing_time = time.time() - start_time
            return score, best_match, explanation
            
        except Exception as e:
            logger.error(f"算法 {algorithm_name} 执行失败: {e}")
            return 0.0, None, f"算法执行失败: {str(e)}"
    
    def match_single_record(self, source_record: Dict, target_records: List[Dict], 
                           context: Optional[MatchingContext] = None) -> HybridMatchResult:
        """单条记录混合匹配"""
        start_time = time.time()
        
        if context is None:
            context = MatchingContext()
        
        try:
            # 评估数据质量
            data_quality = self._assess_data_quality(source_record, target_records)
            logger.debug(f"数据质量评估: {data_quality.value}")
            
            # 获取基础权重
            base_weights = self.algorithm_weights[context.mode]
            
            # 根据数据质量调整权重
            adjusted_weights = self._adjust_weights_by_quality(base_weights, data_quality)
            logger.debug(f"调整后权重: {adjusted_weights}")
            
            # 运行各算法并收集结果
            algorithm_results = {}
            field_scores = {}
            
            for algorithm_name, weight in adjusted_weights.items():
                if weight > 0:
                    score, best_match, explanation = self._run_algorithm(
                        algorithm_name, source_record, target_records
                    )
                    algorithm_results[algorithm_name] = {
                        'score': score,
                        'best_match': best_match,
                        'explanation': explanation,
                        'weight': weight
                    }
            
            # 计算加权综合得分
            weighted_score = 0.0
            total_weight = 0.0
            best_algorithm = None
            best_match = None
            best_explanation = ""
            
            for algorithm_name, result in algorithm_results.items():
                weighted_contribution = result['score'] * result['weight']
                weighted_score += weighted_contribution
                total_weight += result['weight']
                
                # 记录最佳单算法结果
                if best_algorithm is None or result['score'] > algorithm_results[best_algorithm]['score']:
                    best_algorithm = algorithm_name
                    best_match = result['best_match']
                    best_explanation = result['explanation']
            
            # 归一化得分
            if total_weight > 0:
                final_score = weighted_score / total_weight
            else:
                final_score = 0.0
            
            # 计算置信度
            confidence_score = self._calculate_confidence(algorithm_results, data_quality)
            
            # 判断是否匹配成功
            threshold = self._get_dynamic_threshold(context, data_quality)
            matched = final_score >= threshold and best_match is not None
            
            processing_time = time.time() - start_time
            
            # 生成详细解释
            explanation = self._generate_explanation(
                algorithm_results, final_score, confidence_score, 
                best_algorithm, data_quality, context
            )
            
            result = HybridMatchResult(
                matched=matched,
                best_match=best_match,
                similarity_score=final_score,
                confidence_score=confidence_score,
                algorithm_used=f"混合匹配 (主导: {best_algorithm})",
                field_scores=field_scores,
                processing_time=processing_time,
                explanation=explanation
            )
            
            logger.debug(f"混合匹配完成: 得分={final_score:.3f}, 置信度={confidence_score:.3f}, 用时={processing_time:.3f}s")
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"混合匹配失败: {e}")
            
            return HybridMatchResult(
                matched=False,
                best_match=None,
                similarity_score=0.0,
                confidence_score=0.0,
                algorithm_used="混合匹配",
                field_scores={},
                processing_time=processing_time,
                explanation=f"匹配过程出错: {str(e)}"
            )
    
    def _calculate_confidence(self, algorithm_results: Dict, data_quality: DataQuality) -> float:
        """计算置信度"""
        if not algorithm_results:
            return 0.0
        
        scores = [result['score'] for result in algorithm_results.values()]
        
        # 计算得分一致性
        if len(scores) > 1:
            score_variance = sum((s - sum(scores)/len(scores))**2 for s in scores) / len(scores)
            consistency = max(0.0, 1.0 - score_variance)
        else:
            consistency = 1.0
        
        # 计算平均得分
        avg_score = sum(scores) / len(scores)
        
        # 数据质量加成
        quality_bonus = {
            DataQuality.HIGH: 0.1,
            DataQuality.MEDIUM: 0.05,
            DataQuality.LOW: 0.0,
            DataQuality.UNKNOWN: 0.0
        }[data_quality]
        
        confidence = min(1.0, avg_score * consistency + quality_bonus)
        return confidence
    
    def _get_dynamic_threshold(self, context: MatchingContext, data_quality: DataQuality) -> float:
        """获取动态阈值"""
        base_threshold = self.config.get('fuzzy_match', {}).get('similarity_threshold', 0.75)
        
        # 根据模式调整
        mode_adjustments = {
            MatchingMode.PERFORMANCE: -0.05,
            MatchingMode.ACCURACY: +0.1,
            MatchingMode.BALANCED: 0.0,
            MatchingMode.INTERACTIVE: +0.05,
            MatchingMode.AUDIT: +0.15
        }
        
        # 根据数据质量调整
        quality_adjustments = {
            DataQuality.HIGH: +0.05,
            DataQuality.MEDIUM: 0.0,
            DataQuality.LOW: -0.1,
            DataQuality.UNKNOWN: 0.0
        }
        
        adjusted_threshold = (base_threshold + 
                            mode_adjustments[context.mode] + 
                            quality_adjustments[data_quality])
        
        return max(0.1, min(0.95, adjusted_threshold))
    
    def _generate_explanation(self, algorithm_results: Dict, final_score: float, 
                            confidence_score: float, best_algorithm: str, 
                            data_quality: DataQuality, context: MatchingContext) -> str:
        """生成详细解释"""
        explanation_parts = []
        
        explanation_parts.append(f"混合匹配结果 (模式: {context.mode.value})")
        explanation_parts.append(f"数据质量: {data_quality.value}")
        explanation_parts.append(f"最终得分: {final_score:.3f}")
        explanation_parts.append(f"置信度: {confidence_score:.3f}")
        explanation_parts.append(f"主导算法: {best_algorithm}")
        
        explanation_parts.append("\n算法贡献:")
        for algorithm_name, result in algorithm_results.items():
            contribution = result['score'] * result['weight']
            explanation_parts.append(
                f"  • {algorithm_name}: 得分={result['score']:.3f}, "
                f"权重={result['weight']:.3f}, 贡献={contribution:.3f}"
            )
        
        return "\n".join(explanation_parts)

    def batch_match(self, source_records: List[Dict], target_records: List[Dict],
                   context: Optional[MatchingContext] = None) -> List[HybridMatchResult]:
        """批量匹配"""
        if context is None:
            context = MatchingContext()
        
        results = []
        start_time = time.time()
        
        logger.info(f"开始批量混合匹配: {len(source_records)} 条源记录")
        
        for i, source_record in enumerate(source_records):
            if context.time_limit and (time.time() - start_time) > context.time_limit:
                logger.warning(f"批量匹配超时，已处理 {i} 条记录")
                break
            
            result = self.match_single_record(source_record, target_records, context)
            results.append(result)
            
            if (i + 1) % 100 == 0:
                logger.info(f"已处理 {i + 1} 条记录")
        
        total_time = time.time() - start_time
        logger.info(f"批量匹配完成: 处理 {len(results)} 条记录，用时 {total_time:.2f} 秒")
        
        return results
