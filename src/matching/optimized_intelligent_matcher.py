#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
优化的智能匹配器
集成性能优化和精度优化
"""

import logging
import time
import yaml
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import os
import sys

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.matching.intelligent_unit_name_matcher import IntelligentUnitNameMatcher
from src.matching.hybrid_weight_matcher import HybridWeightMatcher
from optimize_matching_system import EnhancedMatchingOptimizer, MatchingOptimization

logger = logging.getLogger(__name__)

@dataclass
class OptimizedMatchResult:
    """优化匹配结果"""
    similarity_score: float
    optimized_score: float
    match_decision: str  # 'match', 'suspicious', 'reject'
    matched_record: Optional[Dict] = None
    optimization_details: Optional[Dict] = None
    processing_time: float = 0.0
    confidence_level: str = "unknown"

class OptimizedIntelligentMatcher:
    """优化的智能匹配器"""
    
    def __init__(self, config_path: str = None):
        """
        初始化优化匹配器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path or "config/mongodb_optimization.yaml"
        self._load_config()
        
        # 初始化核心组件
        self.intelligent_matcher = IntelligentUnitNameMatcher()
        self.hybrid_matcher = HybridWeightMatcher()
        self.optimizer = EnhancedMatchingOptimizer(self.optimization_config)
        
        # 性能统计
        self.stats = {
            'total_matches': 0,
            'successful_matches': 0,
            'suspicious_matches': 0,
            'rejected_matches': 0,
            'total_processing_time': 0.0,
            'average_processing_time': 0.0
        }
        
        logger.info("优化智能匹配器初始化完成")
    
    def _load_config(self):
        """加载配置文件"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                
                # 提取优化配置
                batch_config = config.get('batch_processing', {})
                self.optimization_config = MatchingOptimization(
                    batch_size=batch_config.get('batch_size', 50),
                    max_workers=batch_config.get('max_workers', 4),
                    connection_pool_size=config.get('connection_pool', {}).get('max_pool_size', 50),
                    similarity_threshold=0.6,
                    suspicious_threshold=0.4,
                    reject_threshold=0.25
                )
                
                self.mongodb_config = config
                logger.info(f"配置文件加载成功: {self.config_path}")
            else:
                logger.warning(f"配置文件不存在: {self.config_path}，使用默认配置")
                self.optimization_config = MatchingOptimization()
                self.mongodb_config = {}
                
        except Exception as e:
            logger.error(f"配置文件加载失败: {e}")
            self.optimization_config = MatchingOptimization()
            self.mongodb_config = {}
    
    def match_single_record(self, source_record: Dict, target_records: List[Dict], 
                           use_hybrid: bool = True) -> OptimizedMatchResult:
        """
        匹配单条记录（优化版本）
        
        Args:
            source_record: 源记录
            target_records: 目标记录列表
            use_hybrid: 是否使用混合匹配器
            
        Returns:
            OptimizedMatchResult: 优化匹配结果
        """
        start_time = time.time()
        
        try:
            if use_hybrid:
                # 使用混合匹配器
                result = self.hybrid_matcher.match_single_record(source_record, target_records)
                original_score = result.get('similarity_score', 0.0)
                best_match = result.get('best_match')
            else:
                # 使用基础智能匹配器
                best_match = None
                original_score = 0.0
                
                source_name = source_record.get('UNIT_NAME', '')
                if not source_name:
                    return OptimizedMatchResult(
                        similarity_score=0.0,
                        optimized_score=0.0,
                        match_decision='reject',
                        processing_time=time.time() - start_time
                    )
                
                for target_record in target_records:
                    target_name = target_record.get('dwmc', '')
                    if not target_name:
                        continue
                    
                    score = self.intelligent_matcher.calculate_similarity(source_name, target_name)
                    if score > original_score:
                        original_score = score
                        best_match = target_record
            
            # 应用优化策略
            if best_match:
                source_name = source_record.get('UNIT_NAME', '')
                target_name = best_match.get('dwmc', '')
                source_address = source_record.get('ADDRESS', '')
                target_address = best_match.get('dwdz', '')
                
                optimized_score, optimization_details = self.optimizer.optimize_similarity_score(
                    original_score, source_name, target_name, source_address, target_address
                )
                
                match_decision = self.optimizer.get_match_decision(optimized_score, optimization_details)
                confidence_level = self._calculate_confidence_level(optimized_score, optimization_details)
            else:
                optimized_score = 0.0
                optimization_details = {}
                match_decision = 'reject'
                confidence_level = 'low'
            
            processing_time = time.time() - start_time
            
            # 更新统计信息
            self._update_stats(match_decision, processing_time)
            
            return OptimizedMatchResult(
                similarity_score=original_score,
                optimized_score=optimized_score,
                match_decision=match_decision,
                matched_record=best_match,
                optimization_details=optimization_details,
                processing_time=processing_time,
                confidence_level=confidence_level
            )
            
        except Exception as e:
            logger.error(f"匹配过程出错: {e}")
            processing_time = time.time() - start_time
            return OptimizedMatchResult(
                similarity_score=0.0,
                optimized_score=0.0,
                match_decision='reject',
                processing_time=processing_time,
                confidence_level='error'
            )
    
    def batch_match(self, source_records: List[Dict], target_records: List[Dict],
                   use_hybrid: bool = True, progress_callback=None) -> List[OptimizedMatchResult]:
        """
        批量匹配（优化版本）
        
        Args:
            source_records: 源记录列表
            target_records: 目标记录列表
            use_hybrid: 是否使用混合匹配器
            progress_callback: 进度回调函数
            
        Returns:
            List[OptimizedMatchResult]: 匹配结果列表
        """
        results = []
        batch_size = self.optimization_config.batch_size
        total_records = len(source_records)
        
        logger.info(f"开始批量匹配: {total_records} 条记录，批次大小: {batch_size}")
        
        for i in range(0, total_records, batch_size):
            batch_start = time.time()
            batch_records = source_records[i:i + batch_size]
            batch_results = []
            
            for j, source_record in enumerate(batch_records):
                result = self.match_single_record(source_record, target_records, use_hybrid)
                batch_results.append(result)
                
                # 调用进度回调
                if progress_callback:
                    progress = (i + j + 1) / total_records * 100
                    progress_callback(progress, i + j + 1, total_records)
            
            results.extend(batch_results)
            batch_time = time.time() - batch_start
            
            logger.info(f"批次 {i//batch_size + 1} 完成: {len(batch_records)} 条记录，"
                       f"用时 {batch_time:.2f}秒，速度 {len(batch_records)/batch_time:.1f} 条/秒")
            
            # 短暂休息，避免MongoDB过载
            if i + batch_size < total_records:
                time.sleep(0.1)
        
        logger.info(f"批量匹配完成: 总计 {total_records} 条记录")
        self._log_batch_stats(results)
        
        return results
    
    def _calculate_confidence_level(self, score: float, optimization_details: Dict) -> str:
        """计算置信度等级"""
        if score >= 0.8:
            return 'high'
        elif score >= 0.6:
            return 'medium'
        elif score >= 0.4:
            return 'low'
        else:
            return 'very_low'
    
    def _update_stats(self, decision: str, processing_time: float):
        """更新统计信息"""
        self.stats['total_matches'] += 1
        self.stats['total_processing_time'] += processing_time
        
        if decision == 'match':
            self.stats['successful_matches'] += 1
        elif decision == 'suspicious':
            self.stats['suspicious_matches'] += 1
        else:
            self.stats['rejected_matches'] += 1
        
        self.stats['average_processing_time'] = (
            self.stats['total_processing_time'] / self.stats['total_matches']
        )
    
    def _log_batch_stats(self, results: List[OptimizedMatchResult]):
        """记录批次统计信息"""
        total = len(results)
        matches = sum(1 for r in results if r.match_decision == 'match')
        suspicious = sum(1 for r in results if r.match_decision == 'suspicious')
        rejected = sum(1 for r in results if r.match_decision == 'reject')
        
        avg_time = sum(r.processing_time for r in results) / total if total > 0 else 0
        avg_original_score = sum(r.similarity_score for r in results) / total if total > 0 else 0
        avg_optimized_score = sum(r.optimized_score for r in results) / total if total > 0 else 0
        
        logger.info("=" * 60)
        logger.info("批量匹配统计:")
        logger.info(f"总记录数: {total}")
        logger.info(f"匹配成功: {matches} ({matches/total*100:.1f}%)")
        logger.info(f"疑似匹配: {suspicious} ({suspicious/total*100:.1f}%)")
        logger.info(f"匹配拒绝: {rejected} ({rejected/total*100:.1f}%)")
        logger.info(f"平均处理时间: {avg_time:.3f}秒")
        logger.info(f"平均原始分数: {avg_original_score:.3f}")
        logger.info(f"平均优化分数: {avg_optimized_score:.3f}")
        logger.info("=" * 60)
    
    def get_optimization_summary(self) -> Dict:
        """获取优化效果摘要"""
        return {
            'config': {
                'batch_size': self.optimization_config.batch_size,
                'max_workers': self.optimization_config.max_workers,
                'similarity_threshold': self.optimization_config.similarity_threshold,
                'suspicious_threshold': self.optimization_config.suspicious_threshold
            },
            'stats': self.stats.copy(),
            'mongodb_config': {
                'connection_pool_size': self.optimization_config.connection_pool_size,
                'timeouts': self.mongodb_config.get('timeouts', {}),
                'batch_processing': self.mongodb_config.get('batch_processing', {})
            }
        }

def test_optimized_matcher():
    """测试优化匹配器"""
    matcher = OptimizedIntelligentMatcher()
    
    # 测试数据
    source_records = [
        {
            'UNIT_NAME': '中国联合网络通信有限公司上海市分公司浦东新区营业厅',
            'ADDRESS': '上海市浦东新区陆家嘴环路1000号'
        },
        {
            'UNIT_NAME': '上海为民食品厂',
            'ADDRESS': '上海市黄浦区南京路100号'
        },
        {
            'UNIT_NAME': '北京华为科技有限公司',
            'ADDRESS': '北京市海淀区中关村'
        }
    ]
    
    target_records = [
        {
            'dwmc': '上海市松江区邮行餐饮店',
            'dwdz': '上海市松江区某某路123号'
        },
        {
            'dwmc': '上海惠民食品厂',
            'dwdz': '上海市黄浦区南京路100号'
        },
        {
            'dwmc': '北京华美科技有限公司',
            'dwdz': '北京市朝阳区建国路'
        }
    ]
    
    print("优化智能匹配器测试:")
    print("=" * 80)
    
    # 测试单条匹配
    for i, source_record in enumerate(source_records):
        print(f"\n测试记录 {i+1}:")
        print(f"源记录: {source_record['UNIT_NAME']}")
        
        result = matcher.match_single_record(source_record, target_records, use_hybrid=False)
        
        print(f"原始相似度: {result.similarity_score:.3f}")
        print(f"优化相似度: {result.optimized_score:.3f}")
        print(f"匹配决策: {result.match_decision}")
        print(f"置信度: {result.confidence_level}")
        print(f"处理时间: {result.processing_time:.3f}秒")
        
        if result.matched_record:
            print(f"匹配目标: {result.matched_record['dwmc']}")
        
        if result.optimization_details:
            details = result.optimization_details
            if details.get('industry_conflict', {}).get('penalty', 0) > 0:
                print(f"行业冲突: {details['industry_conflict']['reason']}")
            if details.get('entity_type_conflict', {}).get('penalty', 0) > 0:
                print(f"企业性质冲突: {details['entity_type_conflict']['reason']}")
        
        print("-" * 60)
    
    # 获取优化摘要
    summary = matcher.get_optimization_summary()
    print("\n优化配置摘要:")
    print(f"批次大小: {summary['config']['batch_size']}")
    print(f"相似度阈值: {summary['config']['similarity_threshold']}")
    print(f"疑似阈值: {summary['config']['suspicious_threshold']}")

if __name__ == "__main__":
    test_optimized_matcher()
