"""
优化的模糊匹配器
性能优化版本，专注于速度提升
"""

import logging
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from rapidfuzz import fuzz, process
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import threading
from .match_result import MatchResult

logger = logging.getLogger(__name__)


@dataclass
class FuzzyMatchResult:
    """模糊匹配结果数据类"""
    matched: bool
    similarity_score: float
    source_record: Dict
    target_record: Optional[Dict] = None
    field_similarities: Optional[Dict] = None
    match_details: Optional[Dict] = None


class OptimizedFuzzyMatcher:
    """优化的模糊匹配器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.threshold = config.get('fuzzy_match', {}).get('similarity_threshold', 0.75)
        self.fields_config = config.get('fuzzy_match', {}).get('fields', {})
        
        # 缓存
        self._similarity_cache = {}
        self._cache_lock = threading.Lock()
        
        # 预筛选配置 - 大幅优化性能
        self.prefilter_config = {
            'unit_name_threshold': 0.8,  # 提高预筛选阈值，减少候选记录
            'max_candidates': 20,        # 大幅减少候选记录数量
            'enable_parallel': True,     # 启用并行处理
            'thread_count': 4,          # 线程数
            'early_exit_threshold': 0.95  # 早期退出阈值
        }
        
    def match_single_record_optimized(self, source_record: Dict, target_records: List[Dict]) -> Dict:
        """
        优化的单记录匹配
        
        Args:
            source_record: 源记录
            target_records: 目标记录列表
            
        Returns:
            Dict: 匹配结果
        """
        # 1. 快速预筛选候选记录
        candidates = self._prefilter_candidates_ultra_fast(source_record, target_records)
        
        if not candidates:
            return {
                'matched': False,
                'similarity_score': 0.0,
                'source_record': source_record,
                'target_record': None,
                'match_details': {'reason': 'no_candidates_after_prefilter'}
            }
        
        # 2. 对候选记录进行详细匹配
        best_match = None
        best_score = 0.0
        
        if self.prefilter_config['enable_parallel'] and len(candidates) > 10:
            # 并行处理大量候选记录
            best_match, best_score = self._parallel_match_candidates_fast(source_record, candidates)
        else:
            # 串行处理少量候选记录
            best_match, best_score = self._serial_match_candidates_fast(source_record, candidates)
        
        if best_score >= self.threshold:
            return {
                'matched': True,
                'similarity_score': best_score,
                'source_record': source_record,
                'target_record': best_match,
                'match_details': {
                    'candidates_count': len(candidates),
                    'match_method': 'optimized_fuzzy_ultra_fast'
                }
            }
        else:
            return {
                'matched': False,
                'similarity_score': best_score,
                'source_record': source_record,
                'target_record': None,
                'match_details': {
                    'candidates_count': len(candidates),
                    'best_score': best_score,
                    'threshold': self.threshold
                }
            }
    
    def _prefilter_candidates_ultra_fast(self, source_record: Dict, target_records: List[Dict]) -> List[Dict]:
        """
        超快速预筛选候选记录
        进一步减少候选记录数量
        """
        source_name = self._normalize_text_fast(source_record.get('UNIT_NAME', ''))
        if not source_name:
            return []
        
        candidates = []
        
        # 使用rapidfuzz的process.extract进行快速筛选
        target_names = []
        name_to_record = {}
        
        for record in target_records:
            target_name = self._normalize_text_fast(record.get('dwmc', ''))
            if target_name:
                target_names.append(target_name)
                name_to_record[target_name] = record
        
        # 快速提取相似的单位名称，使用更高的阈值和更少的候选数量
        if target_names:
            matches = process.extract(
                source_name, 
                target_names, 
                scorer=fuzz.token_set_ratio,
                limit=self.prefilter_config['max_candidates']
            )
            
            threshold = self.prefilter_config['unit_name_threshold'] * 100
            for match_name, score, _ in matches:
                if score >= threshold:
                    candidates.append(name_to_record[match_name])
                    # 如果找到非常高分的匹配，可以减少候选数量
                    if score >= 95 and len(candidates) >= 5:
                        break
        
        logger.debug(f"超快预筛选: {len(target_records)} -> {len(candidates)} 候选记录")
        return candidates
    
    def _parallel_match_candidates_fast(self, source_record: Dict, candidates: List[Dict]) -> Tuple[Optional[Dict], float]:
        """并行匹配候选记录 - 快速版本"""
        
        best_match = None
        best_score = 0.0
        
        def match_chunk_fast(chunk):
            chunk_best_match = None
            chunk_best_score = 0.0
            
            for candidate in chunk:
                score = self._calculate_ultra_fast_similarity(source_record, candidate)
                if score > chunk_best_score:
                    chunk_best_score = score
                    chunk_best_match = candidate
                
                # 早期退出机制
                if score >= self.prefilter_config['early_exit_threshold']:
                    break
            
            return chunk_best_match, chunk_best_score
        
        # 分块处理
        chunk_size = max(1, len(candidates) // self.prefilter_config['thread_count'])
        chunks = [candidates[i:i + chunk_size] for i in range(0, len(candidates), chunk_size)]
        
        with ThreadPoolExecutor(max_workers=self.prefilter_config['thread_count']) as executor:
            futures = [executor.submit(match_chunk_fast, chunk) for chunk in chunks]
            
            for future in futures:
                chunk_match, chunk_score = future.result()
                if chunk_score > best_score:
                    best_score = chunk_score
                    best_match = chunk_match
                
                # 如果找到非常高分的匹配，可以提前退出
                if chunk_score >= self.prefilter_config['early_exit_threshold']:
                    break
        
        return best_match, best_score
    
    def _serial_match_candidates_fast(self, source_record: Dict, candidates: List[Dict]) -> Tuple[Optional[Dict], float]:
        """串行匹配候选记录 - 快速版本"""
        
        best_match = None
        best_score = 0.0
        
        for candidate in candidates:
            score = self._calculate_ultra_fast_similarity(source_record, candidate)
            if score > best_score:
                best_score = score
                best_match = candidate
            
            # 早期退出机制
            if score >= self.prefilter_config['early_exit_threshold']:
                break
        
        return best_match, best_score
    
    def _calculate_ultra_fast_similarity(self, source_record: Dict, target_record: Dict) -> float:
        """
        超快速相似度计算
        优先使用单位名称，减少复杂计算
        """
        # 生成缓存键
        cache_key = self._generate_cache_key(source_record, target_record)
        
        with self._cache_lock:
            if cache_key in self._similarity_cache:
                return self._similarity_cache[cache_key]
        
        # 优先计算单位名称相似度
        source_name = self._normalize_text_fast(source_record.get('UNIT_NAME', ''))
        target_name = self._normalize_text_fast(target_record.get('dwmc', ''))
        
        if not source_name or not target_name:
            return 0.0
        
        # 使用快速的单位名称相似度作为主要指标
        name_sim = fuzz.token_set_ratio(source_name, target_name) / 100.0
        
        # 如果单位名称相似度很高，直接返回
        if name_sim >= 0.9:
            final_score = name_sim
        elif name_sim >= 0.7:
            # 只在中等相似度时才计算其他字段
            similarities = {'unit_name': name_sim}
            weights = {'unit_name': 0.7}  # 增加单位名称权重
            total_weight = 0.7
            
            # 简化的地址相似度
            source_addr = self._normalize_text_fast(source_record.get('ADDRESS', ''))
            target_addr = self._normalize_text_fast(target_record.get('dwdz', ''))
            
            if source_addr and target_addr and len(source_addr) > 3 and len(target_addr) > 3:
                addr_sim = fuzz.partial_ratio(source_addr, target_addr) / 100.0
                similarities['address'] = addr_sim
                weights['address'] = 0.3
                total_weight += 0.3
            
            # 计算加权平均分数
            final_score = sum(similarities[field] * weights[field] for field in similarities) / total_weight
        else:
            # 低相似度直接返回单位名称相似度
            final_score = name_sim
        
        # 缓存结果
        with self._cache_lock:
            if len(self._similarity_cache) < 5000:  # 减少缓存大小以节省内存
                self._similarity_cache[cache_key] = final_score
        
        return final_score
    
    def _normalize_text_fast(self, text: str) -> str:
        """超快速文本标准化"""
        if not text:
            return ''
        
        # 更简化的标准化处理
        text = str(text).strip()
        # 只移除空格，保留其他字符以提高速度
        text = re.sub(r'\s+', '', text)
        
        return text.lower()
    
    def _normalize_text(self, text: str) -> str:
        """快速文本标准化"""
        if not text:
            return ''
        
        # 简化的标准化处理
        text = str(text).strip()
        text = re.sub(r'[\s\u3000]+', '', text)  # 移除空格
        text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', text)  # 只保留中文、英文、数字
        
        return text.lower()
    
    def _generate_cache_key(self, source_record: Dict, target_record: Dict) -> str:
        """生成缓存键"""
        source_id = str(source_record.get('_id', ''))
        target_id = str(target_record.get('_id', ''))
        return f"{source_id}_{target_id}"
    
    def clear_cache(self):
        """清空缓存"""
        with self._cache_lock:
            self._similarity_cache.clear()
