#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单快速匹配器 - 专为速度优化
基于188万数据成功经验，使用最简单但最快的算法
"""

import time
import logging
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class SimpleFastMatcher:
    """简单快速匹配器 - 优先速度"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.stats = {
            'total_queries': 0,
            'total_matches': 0,
            'avg_query_time': 0.0
        }
    
    def batch_match(self, source_records: List[Dict], mappings: List[Dict], 
                   source_table: str, task_id: str) -> List[Dict]:
        """批量匹配 - 超高速版本"""
        start_time = time.time()
        batch_size = len(source_records)
        
        logger.info(f"🚀 简单快速匹配开始: {batch_size} 条记录")
        
        # 获取目标表和字段映射
        target_tables = {}
        for mapping in mappings:
            target_table = mapping['target_table']
            if target_table not in target_tables:
                target_tables[target_table] = []
            target_tables[target_table].append(mapping)
        
        all_results = []
        
        # 对每个目标表进行匹配
        for target_table, table_mappings in target_tables.items():
            logger.info(f"📊 匹配目标表: {target_table}")
            
            # 获取目标数据（一次性加载，避免重复查询）
            target_collection = self.db_manager.get_collection(target_table)
            target_records = list(target_collection.find({}).limit(100000))  # 限制数量避免内存问题
            
            logger.info(f"📊 目标记录数: {len(target_records)}")
            
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=16) as executor:  # 高并发
                future_to_source = {}
                
                for source_record in source_records:
                    future = executor.submit(
                        self._match_single_record_fast,
                        source_record, target_records, table_mappings,
                        source_table, target_table, task_id
                    )
                    future_to_source[future] = source_record
                
                # 收集结果
                for future in as_completed(future_to_source):
                    try:
                        result = future.result(timeout=5)  # 快速超时
                        if result:
                            all_results.extend(result)
                    except Exception as e:
                        logger.warning(f"单记录匹配失败: {e}")
        
        duration = time.time() - start_time
        speed = batch_size / duration if duration > 0 else 0
        
        logger.info(f"✅ 简单快速匹配完成: {batch_size} 条记录, "
                   f"耗时: {duration:.2f}秒, 速度: {speed:.1f} 条/秒, "
                   f"匹配结果: {len(all_results)}")
        
        return all_results
    
    def _match_single_record_fast(self, source_record: Dict, target_records: List[Dict],
                                 mappings: List[Dict], source_table: str, 
                                 target_table: str, task_id: str) -> List[Dict]:
        """单记录快速匹配"""
        results = []
        
        try:
            # 获取主要匹配字段
            primary_mappings = [m for m in mappings if m.get('field_priority') == 'primary']
            if not primary_mappings:
                primary_mappings = mappings[:2]  # 取前两个字段
            
            # 快速精确匹配
            for target_record in target_records:
                match_score = 0
                matched_fields = []
                
                # 只检查主要字段，快速匹配
                for mapping in primary_mappings:
                    source_field = mapping['source_field']
                    target_field = mapping['target_field']
                    
                    source_value = str(source_record.get(source_field, '')).strip()
                    target_value = str(target_record.get(target_field, '')).strip()
                    
                    if source_value and target_value:
                        # 简单字符串匹配（最快）
                        if source_value == target_value:
                            match_score += 1.0
                            matched_fields.append(f"{source_field}->{target_field}")
                        elif source_value in target_value or target_value in source_value:
                            match_score += 0.7  # 调整包含匹配得分从0.8到0.7
                            matched_fields.append(f"{source_field}->{target_field}")
                
                # 计算最终相似度
                similarity = match_score / len(primary_mappings) if primary_mappings else 0
                
                # 快速阈值过滤
                if similarity >= 0.5:  # 调整阈值适配0.7包含匹配得分
                    result = {
                        'source_id': str(source_record.get('_id', '')),
                        'target_id': str(target_record.get('_id', '')),
                        'source_table': source_table,
                        'target_table': target_table,
                        'similarity_score': similarity,
                        'matched_fields': matched_fields,
                        'match_algorithm': 'simple_fast',
                        'algorithm_used': 'simple_fast',
                        'source_key_fields': {m['source_field']: source_record.get(m['source_field'], '') for m in primary_mappings},
                        'target_key_fields': {m['target_field']: target_record.get(m['target_field'], '') for m in primary_mappings},
                        'match_details': {
                            'field_scores': {f"{m['source_field']}->{m['target_field']}": 1.0 if f"{m['source_field']}->{m['target_field']}" in matched_fields else 0.0 for m in primary_mappings},
                            'total_fields': len(primary_mappings),
                            'matched_field_count': len(matched_fields),
                            'confidence_level': 'high' if similarity >= 0.9 else 'medium',
                            'match_type': 'exact' if similarity >= 0.9 else 'fuzzy'
                        },
                        'created_at': time.time(),
                        'task_id': task_id,
                        'match_version': '2.0_fast'
                    }
                    results.append(result)
                    
                    # 限制结果数量，提升速度
                    if len(results) >= 5:
                        break
        
        except Exception as e:
            logger.error(f"快速匹配单记录失败: {e}")
        
        return results
