#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强的模糊匹配器
集成结构化名称匹配器，解决匹配幻觉问题
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from .fuzzy_matcher import FuzzyMatcher, FuzzyMatchResult, SimilarityCalculator
from .structured_name_matcher import StructuredNameMatcher, StructuredMatchResult
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


@dataclass
class EnhancedFuzzyMatchResult(FuzzyMatchResult):
    """增强的模糊匹配结果"""
    structured_match_used: bool = False
    structured_match_details: Optional[Dict] = None
    explanation: Optional[Dict] = None  # 使用explanation替代warnings


class EnhancedFuzzyMatcher(FuzzyMatcher):
    """增强的模糊匹配器"""
    
    def __init__(self, config: Dict):
        """
        初始化增强模糊匹配器
        
        Args:
            config: 配置参数
        """
        super().__init__(config)
        
        # 初始化结构化名称匹配器
        self.structured_matcher = StructuredNameMatcher(config)
        
        # 增强配置
        self.enhanced_config = {
            'use_structured_matching': True,  # 是否使用结构化匹配
            'core_name_weight_boost': 1.5,    # 核心名称权重提升系数
            'address_penalty_threshold': 0.4, # 地址差异惩罚阈值（提高到0.4）
            'business_conflict_penalty': 0.3, # 业务类型冲突惩罚
            'structured_match_threshold': 0.85, # 结构化匹配阈值
            'address_mismatch_max_score': 0.60,  # 地址不匹配时的最大分数（降低到0.60）
            'core_name_mismatch_max_score': 0.50  # 核心名称不匹配时的最大分数（新增）
        }
        
        # 更新字段权重配置，强调核心名称的重要性
        self.fields_config['unit_name']['weight'] = 0.5  # 提高单位名称权重
        self.fields_config['address']['weight'] = 0.35   # 提高地址权重
        self.fields_config['legal_person']['weight'] = 0.10
        self.fields_config['security_person']['weight'] = 0.05
    
    def match_single_record(self, source_record: Dict, target_records: List[Dict]) -> EnhancedFuzzyMatchResult:
        """
        对单条记录进行增强模糊匹配
        
        Args:
            source_record: 源记录
            target_records: 目标记录列表
            
        Returns:
            EnhancedFuzzyMatchResult: 增强的模糊匹配结果
        """
        # 收集所有候选匹配
        candidates = []
        
        for target_record in target_records:
            # 1. 首先进行结构化名称匹配
            structured_result = None
            if self.enhanced_config['use_structured_matching']:
                structured_result = self._perform_structured_matching(
                    source_record, target_record
                )
                
                # 如果结构化匹配明确拒绝（如业务类型冲突），跳过此记录
                if structured_result and not structured_result.matched and \
                   structured_result.match_details.get('business_conflict', False):
                    logger.debug(f"结构化匹配拒绝（业务冲突）: {source_record.get('UNIT_NAME')} vs {target_record.get('dwmc')}")
                    continue
            
            # 2. 计算增强的相似度分数
            score, field_similarities, explanation = self._calculate_enhanced_similarity(
                source_record, target_record, structured_result
            )
            
            # 2.5 核心名称相似度检查（新增）
            if structured_result and structured_result.core_name_similarity < 0.7:
                # 核心名称相似度太低，直接跳过
                logger.debug(f"核心名称相似度太低: {source_record.get('UNIT_NAME')} vs {target_record.get('dwmc')} (相似度: {structured_result.core_name_similarity:.3f})")
                continue
            
            # 3. 应用阈值检查
            if score >= self.threshold:
                candidates.append({
                    'record': target_record,
                    'score': score,
                    'field_similarities': field_similarities,
                    'structured_result': structured_result,
                    'explanation': explanation
                })
        
        if not candidates:
            # 没有匹配的记录
            return EnhancedFuzzyMatchResult(
                matched=False,
                similarity_score=0.0,
                source_record=source_record,
                match_details={
                    'threshold': self.threshold,
                    'candidates_count': len(target_records),
                    'qualified_candidates': 0,
                    'reason': 'no_qualified_candidates'
                }
            )
        
        # 选择最佳候选
        best_candidate = self._select_best_enhanced_match(source_record, candidates)
        
        # 构建增强的匹配结果
        return EnhancedFuzzyMatchResult(
            matched=True,
            similarity_score=best_candidate['score'],
            source_record=source_record,
            target_record=best_candidate['record'],
            field_similarities=best_candidate['field_similarities'],
            structured_match_used=best_candidate.get('structured_result') is not None,
            structured_match_details=self._extract_structured_details(best_candidate.get('structured_result')),
            explanation=best_candidate.get('explanation', {}),
            match_details={
                'threshold': self.threshold,
                'candidates_count': len(target_records),
                'qualified_candidates': len(candidates),
                'selection_method': 'enhanced_selection',
                'structured_matching_enabled': self.enhanced_config['use_structured_matching']
            }
        )
    
    def _perform_structured_matching(self, source_record: Dict, target_record: Dict) -> Optional[StructuredMatchResult]:
        """
        执行结构化名称匹配
        
        Args:
            source_record: 源记录
            target_record: 目标记录
            
        Returns:
            StructuredMatchResult: 结构化匹配结果
        """
        try:
            source_name = self._safe_str(source_record.get('UNIT_NAME'))
            target_name = self._safe_str(target_record.get('dwmc'))
            source_address = self._safe_str(source_record.get('ADDRESS'))
            target_address = self._safe_str(target_record.get('dwdz'))
            
            if not source_name or not target_name:
                return None
            
            return self.structured_matcher.match_structured_names(
                source_name, target_name, source_address, target_address
            )
            
        except Exception as e:
            logger.error(f"结构化匹配失败: {str(e)}")
            return None
    
    def _calculate_enhanced_similarity(self, source_record: Dict, target_record: Dict, 
                                     structured_result: Optional[StructuredMatchResult]) -> Tuple[float, Dict, Dict]:
        """
        计算增强的相似度分数
        
        Args:
            source_record: 源记录
            target_record: 目标记录
            structured_result: 结构化匹配结果
            
        Returns:
            Tuple[float, Dict, Dict]: (总分数, 字段相似度, 解释)
        """
        explanation = {"positive": [], "negative": []}
        
        # 1. 使用基础模糊匹配计算初始分数
        base_score, field_similarities = self._calculate_record_similarity(source_record, target_record)
        explanation['positive'].append(f"基础模糊匹配得分: {base_score:.2f}")

        # 2. 如果有结构化匹配结果，进行增强调整
        if structured_result:
            # 核心名称权重提升
            if structured_result.core_name_similarity >= 0.9:
                core_boost = self.enhanced_config['core_name_weight_boost']
                boosted_score = min(1.0, base_score * core_boost)
                if boosted_score > base_score:
                    explanation['positive'].append(f"核心名称高度相似({structured_result.core_name_similarity:.2f})，分数提升至 {boosted_score:.2f}")
                    base_score = boosted_score
                
                field_similarities['core_name_boost'] = {
                    'similarity': structured_result.core_name_similarity,
                    'boost_factor': core_boost
                }
            
            # 业务类型冲突惩罚
            if structured_result.match_details.get('business_conflict', False):
                penalty = self.enhanced_config['business_conflict_penalty']
                base_score = max(0.0, base_score - penalty)
                explanation['negative'].append(f"业务类型冲突: '{structured_result.source_structure.business_type}' vs '{structured_result.target_structure.business_type}' (扣分: {penalty})")
                field_similarities['business_conflict_penalty'] = penalty
            
            # 添加结构化匹配信息到字段相似度
            field_similarities['structured_match'] = {
                'overall_similarity': structured_result.overall_similarity,
                'core_name_similarity': structured_result.core_name_similarity,
                'business_type_similarity': structured_result.business_type_similarity
            }
        
        # 3. 地址差异检查和惩罚
        address_sim = field_similarities.get('address', {}).get('similarity', 0.0)
        unit_name_sim = field_similarities.get('unit_name', {}).get('similarity', 0.0)
        
        # 如果单位名称高度相似但地址差异很大，进行惩罚
        if unit_name_sim >= 0.8 and address_sim < self.enhanced_config['address_penalty_threshold']:
            # 限制最高分数
            max_allowed_score = self.enhanced_config['address_mismatch_max_score']
            if base_score > max_allowed_score:
                explanation['negative'].append(f"地址相似度低({address_sim:.2f})，分数限制为 {max_allowed_score}")
                base_score = max_allowed_score
                field_similarities['address_penalty_applied'] = True
        elif address_sim > 0.8:
             explanation['positive'].append(f"地址相似度高: {address_sim:.2f}")
        
        # 4. 核心名称差异检查
        if structured_result and structured_result.core_name_similarity < 0.7:
            # 核心名称差异过大，即使其他字段相似也要降低分数
            core_penalty = 0.2
            base_score = max(0.0, base_score - core_penalty)
            explanation['negative'].append(f"核心名称差异大: '{structured_result.source_structure.core_name}' vs '{structured_result.target_structure.core_name}' (扣分: {core_penalty})")
            field_similarities['core_name_penalty'] = core_penalty
        
        # 5. 增强的核心名称严格检查（新增）
        if structured_result:
            source_core = structured_result.source_structure.core_name
            target_core = structured_result.target_structure.core_name
            
            # 对于短核心名称（2-4个字），要求更严格的匹配
            if source_core and target_core:
                core_len = min(len(source_core), len(target_core))
                
                if core_len <= 4:
                    # 短名称要求精确匹配或极高相似度
                    if structured_result.core_name_similarity < 0.95:
                        # 检查是否只有一个字不同
                        diff_count = sum(1 for a, b in zip(source_core, target_core) if a != b)
                        if diff_count >= 1 and core_len <= 3:
                            # 短名称中有字不同，严重惩罚
                            severe_penalty = 0.5
                            base_score = max(0.0, base_score - severe_penalty)
                            explanation['negative'].append(f"短核心名称存在差异: '{source_core}' vs '{target_core}' (扣分: {severe_penalty})")
                            field_similarities['core_name_severe_penalty'] = severe_penalty
                
                # 拼音检查（防止音近字不同的误匹配）
                try:
                    import pypinyin
                    source_pinyin = ''.join([p[0] for p in pypinyin.pinyin(source_core, style=pypinyin.NORMAL)])
                    target_pinyin = ''.join([p[0] for p in pypinyin.pinyin(target_core, style=pypinyin.NORMAL)])
                    
                    # 拼音完全不同时进行惩罚
                    pinyin_sim = fuzz.ratio(source_pinyin, target_pinyin) / 100.0
                    if pinyin_sim < 0.6 and structured_result.core_name_similarity > 0.7:
                        # 字形相似但读音不同，可能是误匹配
                        pinyin_penalty = 0.2
                        base_score = max(0.0, base_score - pinyin_penalty)
                        explanation['negative'].append(f"核心名称拼音差异大: '{source_pinyin}' vs '{target_pinyin}' (扣分: {pinyin_penalty})")
                        field_similarities['pinyin_penalty'] = pinyin_penalty
                except:
                    pass
        
        # 6. 最终分数上限检查（新增）
        if structured_result:
            # 如果核心名称相似度低于阈值，限制最高分数
            if structured_result.core_name_similarity < 0.8:
                max_allowed = self.enhanced_config.get('core_name_mismatch_max_score', 0.50)
                if base_score > max_allowed:
                    explanation['negative'].append(f"核心名称相似度低({structured_result.core_name_similarity:.2f})，分数限制为 {max_allowed}")
                    base_score = max_allowed
                    field_similarities['core_name_max_score_limited'] = True
        
        return base_score, field_similarities, explanation
    
    def _select_best_enhanced_match(self, source_record: Dict, candidates: List[Dict]) -> Dict:
        """
        选择最佳的增强匹配候选
        
        Args:
            source_record: 源记录
            candidates: 候选列表
            
        Returns:
            Dict: 最佳候选
        """
        if len(candidates) == 1:
            return candidates[0]
        
        # 按分数排序
        candidates.sort(key=lambda x: x['score'], reverse=True)
        
        # 检查最高分和次高分的差距
        top_score = candidates[0]['score']
        score_threshold = 0.05
        
        # 找出分数接近的候选
        close_candidates = [c for c in candidates if top_score - c['score'] <= score_threshold]
        
        if len(close_candidates) == 1:
            return close_candidates[0]
        
        # 多个分数接近的候选，进行增强的二次筛选
        logger.info(f"发现 {len(close_candidates)} 个分数接近的候选，进行增强二次筛选")
        
        scored_candidates = []
        
        for candidate in close_candidates:
            secondary_score = 0
            reasons = []
            
            # 1. 结构化匹配质量 (40分)
            if candidate.get('structured_result'):
                struct_result = candidate['structured_result']
                if struct_result.matched:
                    struct_score = 40
                    secondary_score += struct_score
                    reasons.append("结构化匹配通过")
                
                # 核心名称匹配质量加分
                core_name_sim = struct_result.core_name_similarity
                if core_name_sim >= 0.95:
                    secondary_score += 20
                    reasons.append(f"核心名称完美匹配({core_name_sim:.2f})")
            
            # 2. 无警告加分 (20分)
            if not candidate.get('explanation', {}).get('negative'):
                secondary_score += 20
                reasons.append("无匹配警告")
            
            # 3. 地址匹配质量 (30分)
            address_sim = candidate['field_similarities'].get('address', {}).get('similarity', 0)
            if address_sim >= 0.8:
                address_score = address_sim * 30
                secondary_score += address_score
                reasons.append(f"地址高度匹配({address_sim:.2f})")
            
            # 4. 数据完整度 (10分)
            completeness = self._calculate_target_completeness(candidate['record'])
            if completeness >= 0.8:
                secondary_score += 10
                reasons.append(f"数据完整({completeness:.2f})")
            
            scored_candidates.append({
                'candidate': candidate,
                'secondary_score': secondary_score,
                'reasons': reasons
            })
        
        # 按二次分数排序
        scored_candidates.sort(key=lambda x: x['secondary_score'], reverse=True)
        best = scored_candidates[0]
        
        logger.info(f"增强二次筛选完成，选择原因: {', '.join(best['reasons'])}")
        
        return best['candidate']
    
    def _extract_structured_details(self, structured_result: Optional[StructuredMatchResult]) -> Optional[Dict]:
        """提取结构化匹配详情"""
        if not structured_result:
            return None
        
        return {
            'matched': structured_result.matched,
            'overall_similarity': structured_result.overall_similarity,
            'core_name_similarity': structured_result.core_name_similarity,
            'business_type_similarity': structured_result.business_type_similarity,
            'address_similarity': structured_result.address_similarity,
            'source_structure': {
                'region': structured_result.source_structure.region,
                'core_name': structured_result.source_structure.core_name,
                'business_type': structured_result.source_structure.business_type,
                'company_type': structured_result.source_structure.company_type
            } if structured_result.source_structure else None,
            'target_structure': {
                'region': structured_result.target_structure.region,
                'core_name': structured_result.target_structure.core_name,
                'business_type': structured_result.target_structure.business_type,
                'company_type': structured_result.target_structure.company_type
            } if structured_result.target_structure else None,
            'match_details': structured_result.match_details
        } 