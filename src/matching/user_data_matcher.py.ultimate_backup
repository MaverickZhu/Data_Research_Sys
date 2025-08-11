"""
用户数据智能匹配器
专门处理用户上传数据的智能匹配，复用原项目成熟算法
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from datetime import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入原项目成熟匹配算法
from .exact_matcher import ExactMatcher
from .fuzzy_matcher import FuzzyMatcher
from .optimized_match_processor import OptimizedMatchProcessor
from .enhanced_fuzzy_matcher import EnhancedFuzzyMatcher
from .similarity_scorer import SimilarityCalculator
from .match_result import MatchResult
from .smart_index_manager import SmartIndexManager
from .optimized_prefilter import OptimizedPrefilter, CandidateRanker
from .graph_matcher import GraphMatcher
from .slice_enhanced_matcher import SliceEnhancedMatcher

logger = logging.getLogger(__name__)


class UserDataMatcher:
    """用户数据智能匹配器"""
    
    def __init__(self, db_manager=None, config: Dict[str, Any] = None):
        """
        初始化用户数据匹配器
        
        Args:
            db_manager: 数据库管理器
            config: 配置参数
        """
        self.db_manager = db_manager
        self.config = config or self._get_default_config()
        
        # 初始化各种匹配算法
        self._init_matchers()
        
        # 初始化性能优化组件
        self.index_manager = SmartIndexManager(db_manager) if db_manager else None
        self.prefilter = None  # 将在启动匹配任务时初始化
        self.candidate_ranker = None  # 将在启动匹配任务时初始化
        
        # 初始化高性能匹配组件（原项目算法）
        self.graph_matcher = None  # 图匹配器
        self.slice_matcher = None  # 切片增强匹配器
        self.use_high_performance = True  # 启用高性能模式
        
        # 匹配任务缓存
        self.running_tasks = {}
        self.stop_flags = {}  # 任务停止标志
        
        # 性能统计
        self.performance_stats = {
            'total_processed': 0,
            'total_matched': 0,
            'avg_processing_time': 0,
            'index_creation_time': 0,
            'prefilter_performance': {}
        }
        
        logger.info("用户数据智能匹配器初始化完成")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'exact_match': {
                'fields': {
                    'unit_name': {
                        'source_field': 'UNIT_NAME',
                        'target_field': 'dwmc',
                        'match_type': 'string',
                        'weight': 1.0,
                        'required': True
                    },
                    'credit_code': {
                        'source_field': 'TYSHXYDM',
                        'target_field': 'tyshxydm',
                        'match_type': 'string',
                        'weight': 1.0,
                        'required': False
                    }
                },
                'enable_preprocessing': True,
                'case_sensitive': False
            },
            'fuzzy_match': {
                'fields': {
                    'unit_name': {
                        'source_field': 'UNIT_NAME',
                        'target_field': 'dwmc',
                        'match_type': 'string',
                        'weight': 1.0,
                        'required': True
                    },
                    'address': {
                        'source_field': 'ADDRESS',
                        'target_field': 'dz',
                        'match_type': 'address',
                        'weight': 0.8,
                        'required': False
                    }
                },
                'similarity_threshold': 0.7,
                'enable_chinese_processing': True,
                'enable_vector_similarity': True
            },
            'similarity': {
                'string_similarity': {
                    'chinese_processing': {
                        'enable': True,
                        'use_pinyin': True,
                        'use_jieba': True
                    }
                },
                'numeric_similarity': {
                    'tolerance': 0.1
                },
                'address_similarity': {
                    'enable_fuzzy_match': True,
                    'similarity_threshold': 0.6
                }
            },
            'performance': {
                'batch_size': 1000,
                'max_workers': 4,
                'enable_cache': True
            }
        }
    
    def _init_matchers(self):
        """初始化各种匹配算法"""
        try:
            # 精确匹配器
            self.exact_matcher = ExactMatcher(self.config)
            
            # 模糊匹配器
            self.fuzzy_matcher = FuzzyMatcher(self.config)
            
            # 增强模糊匹配器
            self.enhanced_fuzzy_matcher = EnhancedFuzzyMatcher(self.config)
            
            # 相似度评分器
            self.similarity_calculator = SimilarityCalculator(self.config.get('similarity', {}))
            
            # 优化匹配处理器（如果需要复杂匹配）
            # 注意：OptimizedMatchProcessor需要ConfigManager，这里先不初始化
            # 如果需要使用，可以在具体匹配方法中临时创建
            self.optimized_processor = None
            
            logger.info("匹配算法初始化完成")
            
        except Exception as e:
            logger.error(f"匹配算法初始化失败: {str(e)}")
            raise
    
    def start_matching_task(self, task_config: Dict[str, Any]) -> str:
        """
        启动匹配任务（包含性能优化）
        
        Args:
            task_config: 任务配置
            
        Returns:
            str: 任务ID
        """
        task_id = task_config['task_id']
        
        try:
            # 第一步：创建优化索引
            if self.index_manager and task_config.get('mappings'):
                logger.info("开始创建优化索引...")
                index_start_time = time.time()
                
                index_result = self.index_manager.create_mapping_optimized_indexes(
                    mappings=task_config['mappings'],
                    source_table=task_config.get('source_table', ''),
                    target_tables=task_config.get('target_tables', [])
                )
                
                index_creation_time = time.time() - index_start_time
                self.performance_stats['index_creation_time'] = index_creation_time
                
                logger.info(f"索引创建完成，耗时: {index_creation_time:.2f}秒, "
                           f"创建: {index_result.get('created_count', 0)}, "
                           f"跳过: {index_result.get('skipped_count', 0)}")
            
            # 第二步：初始化高性能匹配组件
            if task_config.get('mappings') and self.use_high_performance:
                logger.info("初始化高性能匹配组件...")
                
                # 初始化图匹配器
                if self.db_manager and hasattr(self.db_manager, 'mongo_client') and self.db_manager.mongo_client is not None:
                    try:
                        # 获取数据库实例
                        db = self.db_manager.get_db()
                        logger.debug(f"成功获取数据库实例: {db}")
                            
                        if db is not None:
                            self.graph_matcher = GraphMatcher(db, self.config)
                            logger.info("图匹配器初始化完成")
                            
                            # 预先构建图结构（关键优化）
                            logger.info("开始预建图结构...")
                            graph_build_start = time.time()
                            self.graph_matcher.build_graph(limit=50000)  # 预建更大的图
                            logger.info(f"图结构预建完成，耗时: {time.time() - graph_build_start:.2f}秒")
                            
                            # 初始化切片增强匹配器
                            self.slice_matcher = SliceEnhancedMatcher(self.db_manager)
                            logger.info("切片增强匹配器初始化完成")
                        else:
                            logger.warning("数据库连接无效，跳过图匹配器初始化")
                    except Exception as e:
                        logger.error(f"图匹配器初始化失败: {str(e)}")
                        self.graph_matcher = None
                        self.slice_matcher = None
                
                # 初始化预过滤系统（作为补充）
                self.prefilter = OptimizedPrefilter(self.db_manager, task_config['mappings'])
                self.candidate_ranker = CandidateRanker(task_config['mappings'])
                logger.info("高性能匹配组件初始化完成")
            
            # 第三步：创建后台线程执行匹配
            thread = threading.Thread(
                target=self._execute_optimized_matching_task,
                args=(task_id, task_config)
            )
            thread.daemon = True
            thread.start()
            
            self.running_tasks[task_id] = {
                'thread': thread,
                'config': task_config,
                'start_time': datetime.now(),
                'has_optimization': True
            }
            
            logger.info(f"启动用户数据匹配任务（优化版）: {task_id}")
            return task_id
            
        except Exception as e:
            logger.error(f"启动优化匹配任务失败: {str(e)}")
            # 降级到普通匹配
            return self._start_fallback_matching_task(task_config)
    
    def _start_fallback_matching_task(self, task_config: Dict[str, Any]) -> str:
        """启动降级匹配任务（不使用优化）"""
        task_id = task_config['task_id']
        
        logger.info(f"启动降级匹配任务: {task_id}")
        
        thread = threading.Thread(
            target=self._execute_matching_task,
            args=(task_id, task_config)
        )
        thread.daemon = True
        thread.start()
        
        self.running_tasks[task_id] = {
            'thread': thread,
            'config': task_config,
            'start_time': datetime.now(),
            'has_optimization': False
        }
        
        return task_id
    
    def _execute_optimized_matching_task(self, task_id: str, config: Dict[str, Any]):
        """
        执行优化匹配任务（后台线程）
        """
        try:
            logger.info(f"开始执行优化匹配任务: {task_id}")
            
            # 更新任务状态
            self._update_task_status(task_id, 'running', 0, '初始化匹配环境')
            
            # 获取源表和映射配置
            source_table = config.get('source_table', '')
            mappings = config.get('mappings', [])
            
            if not source_table or not mappings:
                raise ValueError("缺少源表或字段映射配置")
            
            # 获取源数据
            db = self.db_manager.mongo_client.get_database()
            source_collection = db[source_table]
            
            # 统计总记录数
            total_records = source_collection.count_documents({})
            logger.info(f"源表 {source_table} 总记录数: {total_records}")
            
            if total_records == 0:
                self._update_task_status(task_id, 'completed', 100, '源表无数据', 0, 0, 0)
                return
            
            # 初始化进度
            processed_count = 0
            matched_count = 0
            batch_size = config.get('batch_size', 1000)
            
            # 创建结果集合
            result_collection_name = f'user_match_results_{task_id}'
            result_collection = db[result_collection_name]
            
            # 分批处理记录
            batch_start_time = time.time()
            
            for batch_records in self._get_source_records_batch(source_collection, batch_size):
                if self.stop_flags.get(task_id, False):
                    logger.info(f"任务 {task_id} 被停止")
                    self._update_task_status(task_id, 'stopped', 100, '任务已停止', 
                                           processed_count, total_records, matched_count)
                    return
                
                # 处理当前批次
                batch_results = self._process_optimized_batch(
                    batch_records, mappings, source_table, task_id
                )
                
                # 保存匹配结果
                if batch_results:
                    try:
                        result_collection.insert_many(batch_results)
                        matched_count += len(batch_results)
                    except Exception as e:
                        logger.error(f"保存匹配结果失败: {str(e)}")
                
                processed_count += len(batch_records)
                
                # 更新进度
                progress = (processed_count / total_records) * 100
                elapsed_time = time.time() - batch_start_time
                
                self._update_task_status(task_id, 'running', round(progress, 2), 
                                       f'处理中 {processed_count}/{total_records}',
                                       processed_count, total_records, matched_count)
                
                logger.info(f"任务 {task_id} 进度: {progress:.1f}% "
                           f"({processed_count}/{total_records}), 匹配: {matched_count}")
            
            # 任务完成
            total_time = time.time() - batch_start_time
            
            # 更新性能统计
            self.performance_stats['total_processed'] += processed_count
            self.performance_stats['total_matched'] += matched_count
            if self.prefilter:
                self.performance_stats['prefilter_performance'] = self.prefilter.get_performance_stats()
            
            self._update_task_status(task_id, 'completed', 100.0, 
                                   f'匹配完成，共找到 {matched_count} 个匹配结果',
                                   processed_count, total_records, matched_count)
            
            logger.info(f"优化匹配任务完成: {task_id}, "
                       f"处理: {processed_count}, 匹配: {matched_count}, "
                       f"耗时: {total_time:.2f}秒")
            
        except Exception as e:
            logger.error(f"执行优化匹配任务失败: {task_id} - {str(e)}")
            self._update_task_status(task_id, 'failed', 0, f'匹配任务失败: {str(e)}',
                                   0, 0, 0)
        finally:
            # 清理任务
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
    
    def _process_optimized_batch(self, batch_records: List[Dict], mappings: List[Dict], 
                               source_table: str, task_id: str) -> List[Dict]:
        """处理优化批次（使用高性能算法）"""
        batch_results = []
        batch_start_time = time.time()
        
        # 检查图匹配器状态（图应该已经预建完成）
        if self.graph_matcher:
            if not self.graph_matcher._is_built:
                logger.warning("图结构未预建，这可能影响性能")
            else:
                logger.debug("使用预建的图结构进行匹配")
        
        processed_count = 0
        for source_record in batch_records:
            try:
                processed_count += 1
                
                # 第一优先级：使用切片增强匹配器（最快）
                candidates = []
                if self.slice_matcher and self.use_high_performance:
                    candidates = self._get_slice_enhanced_candidates(source_record, mappings)
                    logger.debug(f"切片匹配获得 {len(candidates)} 个候选")
                
                # 第二优先级：使用图匹配器（高质量）
                if not candidates and self.graph_matcher:
                    candidates = self._get_graph_matching_candidates(source_record, mappings)
                    logger.debug(f"图匹配获得 {len(candidates)} 个候选")
                
                # 第三优先级：使用预过滤系统
                if not candidates and self.prefilter:
                    candidates = self.prefilter.get_optimized_candidates(source_record, source_table)
                    logger.debug(f"预过滤获得 {len(candidates)} 个候选")
                
                # 最后降级：简单查询
                if not candidates:
                    candidates = self._get_simple_candidates(source_record, mappings)
                    logger.debug(f"简单查询获得 {len(candidates)} 个候选")
                
                if not candidates:
                    continue
                
                # 使用候选排序器优化顺序
                if self.candidate_ranker and candidates:
                    candidates = self.candidate_ranker.rank_candidates(candidates, source_record)
                
                # 执行高性能匹配算法
                matches = self._execute_high_performance_matching(
                    source_record, candidates, mappings
                )
                
                # 添加到结果中
                for match in matches:
                    result = self._format_optimized_match_result(
                        source_record, match, mappings, task_id
                    )
                    batch_results.append(result)
                
                # 每100条记录输出一次进度
                if processed_count % 100 == 0:
                    elapsed = time.time() - batch_start_time
                    speed = processed_count / elapsed
                    logger.info(f"批次进度: {processed_count}/{len(batch_records)}, "
                               f"速度: {speed:.1f} 条/秒, 匹配: {len(batch_results)}")
                    
            except Exception as e:
                logger.warning(f"处理记录失败: {source_record.get('_id', 'unknown')} - {str(e)}")
                continue
        
        total_time = time.time() - batch_start_time
        speed = len(batch_records) / total_time if total_time > 0 else 0
        logger.info(f"批次处理完成: {len(batch_records)} 条记录, "
                   f"匹配结果: {len(batch_results)}, "
                   f"总耗时: {total_time:.2f}秒, 速度: {speed:.1f} 条/秒")
        
        return batch_results
    
    def _format_optimized_match_result(self, source_record: Dict, match: Dict, 
                                     mappings: List[Dict], task_id: str) -> Dict:
        """格式化优化匹配结果"""
        return {
            # 基础标识信息
            'source_id': str(source_record.get('_id', '')),
            'target_id': str(match['target_record'].get('_id', '')),
            'source_table': match.get('source_table', ''),
            'target_table': match.get('target_table', ''),
            
            # 匹配核心信息
            'similarity_score': match['similarity'],
            'matched_fields': match['matched_fields'],
            'match_algorithm': 'enhanced_optimized',
            
            # 源记录关键字段
            'source_key_fields': self._extract_key_fields(source_record, mappings, 'source'),
            'target_key_fields': self._extract_key_fields(match['target_record'], mappings, 'target'),
            
            # 匹配详细信息
            'match_details': {
                'field_scores': match.get('details', {}).get('field_scores', {}),
                'total_fields': len(mappings),
                'matched_field_count': len(match['matched_fields']),
                'confidence_level': self._calculate_confidence_level(match['similarity']),
                'match_type': self._determine_match_type(match['similarity'], match['matched_fields']),
                'prefilter_candidates': match.get('prefilter_candidates', 0)
            },
            
            # 元数据信息
            'created_at': datetime.now().isoformat(),
            'task_id': task_id,
            'match_version': '2.1',  # 标识优化版本
            'optimization_enabled': True
        }
    
    def _get_simple_candidates(self, source_record: Dict, mappings: List[Dict]) -> List[Dict]:
        """获取简单候选记录（降级方法）"""
        if not self.db_manager or not self.db_manager.mongo_client:
            return []
        
        db = self.db_manager.mongo_client.get_database()
        all_candidates = []
        
        # 为每个映射字段查询候选
        for mapping in mappings[:2]:  # 限制字段数量
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            target_table = mapping['target_table']
            
            source_value = source_record.get(source_field)
            if not source_value:
                continue
            
            try:
                collection = db[target_table]
                
                # 精确匹配
                exact_candidates = list(collection.find({target_field: source_value}).limit(20))
                all_candidates.extend(exact_candidates)
                
                # 如果候选不足，进行文本搜索
                if len(all_candidates) < 50:
                    try:
                        text_candidates = list(collection.find(
                            {'$text': {'$search': str(source_value)}}
                        ).limit(30))
                        all_candidates.extend(text_candidates)
                    except:
                        pass
                        
            except Exception as e:
                logger.debug(f"简单候选查询失败: {str(e)}")
        
        # 去重
        seen_ids = set()
        unique_candidates = []
        for candidate in all_candidates:
            candidate_id = candidate.get('_id')
            if candidate_id and candidate_id not in seen_ids:
                seen_ids.add(candidate_id)
                unique_candidates.append(candidate)
        
        return unique_candidates[:100]  # 限制候选数量
    
    def _get_slice_enhanced_candidates(self, source_record: Dict, mappings: List[Dict]) -> List[Dict]:
        """使用切片增强匹配器获取候选记录"""
        if not self.slice_matcher:
            return []
        
        try:
            # 提取主要匹配字段
            primary_field = None
            for mapping in mappings:
                if 'name' in mapping['source_field'].lower() or 'company' in mapping['source_field'].lower():
                    primary_field = mapping['source_field']
                    break
            
            if not primary_field or not source_record.get(primary_field):
                return []
            
            source_name = str(source_record[primary_field]).strip()
            if len(source_name) < 3:
                return []
            
            # 使用切片匹配器进行快速匹配
            matches = self.slice_matcher.fast_fuzzy_match(
                source_name, 
                threshold=0.7,
                max_results=50
            )
            
            # 转换为候选记录格式
            candidates = []
            for match in matches:
                if match.get('target_record'):
                    candidates.append(match['target_record'])
            
            return candidates[:50]  # 限制数量
            
        except Exception as e:
            logger.debug(f"切片匹配失败: {str(e)}")
            return []
    
    def _get_graph_matching_candidates(self, source_record: Dict, mappings: List[Dict]) -> List[Dict]:
        """使用图匹配器获取候选记录"""
        if not self.graph_matcher or not self.graph_matcher._is_built:
            return []
        
        try:
            source_id = str(source_record.get('_id', ''))
            if not source_id:
                return []
            
            # 使用图匹配查找相关候选
            similar_entities = self.graph_matcher.find_similar_entities(
                source_id, 
                similarity_threshold=0.6,
                max_results=30
            )
            
            if not similar_entities:
                return []
            
            # 获取完整的候选记录
            db = self.db_manager.mongo_client.get_database()
            candidates = []
            
            for entity_info in similar_entities:
                entity_id = entity_info.get('entity_id')
                if entity_id:
                    # 从目标表查询完整记录
                    for mapping in mappings:
                        target_table = mapping['target_table']
                        collection = db[target_table]
                        
                        record = collection.find_one({'_id': entity_id})
                        if record:
                            candidates.append(record)
                            break
            
            return candidates[:30]
            
        except Exception as e:
            logger.debug(f"图匹配失败: {str(e)}")
            return []
    
    def _execute_high_performance_matching(self, source_record: Dict, candidates: List[Dict], 
                                         mappings: List[Dict]) -> List[Dict]:
        """执行高性能匹配算法"""
        matches = []
        
        try:
            # 优先使用切片增强匹配器进行精确评分
            if self.slice_matcher:
                for candidate in candidates[:20]:  # 限制候选数量以提升速度
                    similarity_score = self._calculate_slice_similarity(
                        source_record, candidate, mappings
                    )
                    
                    if similarity_score >= 0.6:  # 阈值
                        matches.append({
                            'target_record': candidate,
                            'similarity': similarity_score,
                            'matched_fields': self._get_matched_fields(source_record, candidate, mappings),
                            'details': {
                                'algorithm': 'slice_enhanced',
                                'field_scores': self._calculate_field_scores(source_record, candidate, mappings)
                            }
                        })
            
            # 如果切片匹配结果不足，使用增强模糊匹配器
            if len(matches) < 3 and self.enhanced_fuzzy_matcher:
                fuzzy_matches = self._match_record_to_targets(
                    source_record, candidates, mappings,
                    self.enhanced_fuzzy_matcher,
                    0.6, 5
                )
                
                # 合并结果，去重
                existing_ids = {match['target_record'].get('_id') for match in matches}
                for fuzzy_match in fuzzy_matches:
                    target_id = fuzzy_match['target_record'].get('_id')
                    if target_id not in existing_ids:
                        matches.append(fuzzy_match)
            
            # 按相似度排序
            matches.sort(key=lambda x: x['similarity'], reverse=True)
            
            return matches[:5]  # 返回前5个最佳匹配
            
        except Exception as e:
            logger.warning(f"高性能匹配执行失败: {str(e)}")
            return []
    
    def _calculate_slice_similarity(self, source_record: Dict, target_record: Dict, 
                                  mappings: List[Dict]) -> float:
        """计算切片相似度"""
        if not self.slice_matcher:
            return 0.0
        
        total_score = 0.0
        total_weight = 0.0
        
        for mapping in mappings:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            weight = mapping.get('similarity_score', 1.0)
            
            source_value = source_record.get(source_field, '')
            target_value = target_record.get(target_field, '')
            
            if source_value and target_value:
                # 使用切片匹配器计算相似度
                field_similarity = self.slice_matcher.calculate_similarity(
                    str(source_value), str(target_value)
                )
                
                total_score += field_similarity * weight
                total_weight += weight
        
        return total_score / max(total_weight, 0.1)
    
    def _calculate_field_scores(self, source_record: Dict, target_record: Dict, 
                              mappings: List[Dict]) -> Dict[str, float]:
        """计算各字段得分"""
        field_scores = {}
        
        for mapping in mappings:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            
            source_value = source_record.get(source_field, '')
            target_value = target_record.get(target_field, '')
            
            if source_value and target_value:
                if self.slice_matcher:
                    score = self.slice_matcher.calculate_similarity(
                        str(source_value), str(target_value)
                    )
                else:
                    # 简单字符串相似度
                    score = self._simple_string_similarity(str(source_value), str(target_value))
                
                field_scores[f"{source_field}->{target_field}"] = score
        
        return field_scores
    
    def _simple_string_similarity(self, str1: str, str2: str) -> float:
        """简单字符串相似度计算"""
        if str1 == str2:
            return 1.0
        
        if not str1 or not str2:
            return 0.0
        
        # 简单的字符重叠率计算
        set1 = set(str1)
        set2 = set(str2)
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / max(union, 1)
    
    def _get_matched_fields(self, source_record: Dict, target_record: Dict, 
                          mappings: List[Dict]) -> List[str]:
        """获取匹配的字段列表"""
        matched_fields = []
        
        for mapping in mappings:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            
            source_value = source_record.get(source_field, '')
            target_value = target_record.get(target_field, '')
            
            if source_value and target_value:
                similarity = self._simple_string_similarity(str(source_value), str(target_value))
                if similarity >= 0.5:  # 字段匹配阈值
                    matched_fields.append(f"{source_field}->{target_field}")
        
        return matched_fields
    
    def _get_source_records_batch(self, collection, batch_size: int = 1000):
        """
        批量获取源记录
        
        Args:
            collection: MongoDB集合
            batch_size: 批次大小
            
        Yields:
            List[Dict]: 批次记录
        """
        skip = 0
        while True:
            batch = list(collection.find({}).skip(skip).limit(batch_size))
            if not batch:
                break
            yield batch
            skip += batch_size

    def _update_task_status(self, task_id: str, status: str, progress: float = 0, 
                           message: str = "", processed: int = 0, total: int = 0, matches: int = 0):
        """更新任务状态"""
        # 更新内存中的任务状态
        if task_id in self.running_tasks:
            task = self.running_tasks[task_id]
            task.update({
                'status': status,
                'progress': progress,
                'message': message,
                'processed': processed,
                'total': total,
                'matches': matches,
                'updated_time': datetime.now()
            })
            
            # 同时更新数据库中的任务状态
            try:
                if self.db_manager and hasattr(self.db_manager, 'get_db'):
                    db = self.db_manager.get_db()
                    task_collection = db['user_matching_tasks']
                    
                    task_collection.update_one(
                        {'task_id': task_id},
                        {
                            '$set': {
                                'status': status,
                                'progress': {
                                    'progress': progress,
                                    'processed': processed,
                                    'total': total,
                                    'matches': matches,
                                    'message': message
                                },
                                'updated_at': datetime.now().isoformat()
                            }
                        }
                    )
                    logger.debug(f"任务状态更新（内存+数据库）: {task_id} - {status} ({progress:.1f}%)")
            except Exception as e:
                logger.warning(f"更新数据库任务状态失败: {e}")
                logger.debug(f"任务状态更新（仅内存）: {task_id} - {status} ({progress:.1f}%)")
    
    def _execute_matching_task(self, task_id: str, config: Dict[str, Any]):
        """
        执行匹配任务（后台线程）
        
        Args:
            task_id: 任务ID
            config: 任务配置
        """
        try:
            logger.info(f"开始执行用户数据匹配任务: {task_id}")
            
            # 获取数据库连接
            if not self.db_manager or not self.db_manager.mongo_client:
                raise Exception("数据库连接未初始化")
            
            db = self.db_manager.mongo_client.get_database()
            task_collection = db['user_matching_tasks']
            
            # 更新任务状态的函数
            def update_progress(percentage, processed, total, matches, status='running', current_operation=''):
                task_collection.update_one(
                    {'task_id': task_id},
                    {
                        '$set': {
                            'progress': {
                                'percentage': percentage,
                                'processed': processed,
                                'total': total,
                                'matches': matches,
                                'current_operation': current_operation
                            },
                            'status': status,
                            'updated_at': datetime.now().isoformat()
                        }
                    }
                )
            
            # 获取源表和目标表数据
            source_table = config['source_table']
            target_tables = config['target_tables']
            mappings = config['mappings']
            algorithm_type = config.get('algorithm_type', 'optimized')
            similarity_threshold = config.get('similarity_threshold', 0.7)
            batch_size = config.get('batch_size', 100)
            max_results = config.get('max_results', 10)
            
            update_progress(0, 0, 0, 0, 'running', '正在准备数据...')
            
            # 获取源表数据
            source_collection = db[source_table]
            total_records = source_collection.count_documents({})
            
            if total_records == 0:
                raise Exception(f"源表 {source_table} 没有数据")
            
            update_progress(5, 0, total_records, 0, 'running', '正在加载源数据...')
            
            # 获取目标表数据
            target_data = {}
            for target_table in target_tables:
                target_collection = db[target_table]
                target_data[target_table] = list(target_collection.find({}))
                logger.info(f"加载目标表 {target_table}: {len(target_data[target_table])} 条记录")
            
            update_progress(10, 0, total_records, 0, 'running', '数据加载完成，开始匹配...')
            
            # 选择匹配算法
            matcher = self._select_matcher(algorithm_type)
            
            # 批量处理源数据
            processed = 0
            total_matches = 0
            results = []
            
            # 使用游标分批处理，避免内存溢出
            cursor = source_collection.find({}).batch_size(batch_size)
            
            for batch_start in range(0, total_records, batch_size):
                # 检查任务是否被停止
                current_task = task_collection.find_one({'task_id': task_id})
                if current_task and current_task.get('status') == 'stopped':
                    logger.info(f"用户数据匹配任务被停止: {task_id}")
                    return
                
                # 获取当前批次数据
                batch_records = []
                for _ in range(batch_size):
                    try:
                        record = next(cursor)
                        batch_records.append(record)
                    except StopIteration:
                        break
                
                if not batch_records:
                    break
                
                # 执行批次匹配
                batch_matches = self._execute_batch_matching(
                    batch_records, target_data, mappings, matcher,
                    similarity_threshold, max_results
                )
                
                results.extend(batch_matches)
                processed += len(batch_records)
                total_matches += len(batch_matches)
                
                # 更新进度
                percentage = min(int((processed / total_records) * 100), 100)
                update_progress(
                    percentage, processed, total_records, total_matches,
                    'running', f'已处理 {processed}/{total_records} 条记录'
                )
                
                # 模拟处理时间（实际匹配会有计算时间）
                time.sleep(0.1)
            
            # 保存匹配结果
            update_progress(95, processed, total_records, total_matches, 'running', '正在保存结果...')
            
            result_collection = db[f'user_match_results_{task_id}']
            if results:
                result_collection.insert_many(results)
                result_collection.create_index([('source_id', 1)])
                result_collection.create_index([('similarity_score', -1)])
            
            # 任务完成
            update_progress(100, total_records, total_records, total_matches, 'completed', '匹配完成')
            
            logger.info(f"用户数据匹配任务完成: {task_id}, 匹配数: {total_matches}")
            
        except Exception as e:
            logger.error(f"执行用户数据匹配任务失败: {str(e)}")
            # 更新任务状态为失败
            try:
                db = self.db_manager.mongo_client.get_database()
                task_collection = db['user_matching_tasks']
                task_collection.update_one(
                    {'task_id': task_id},
                    {
                        '$set': {
                            'status': 'failed',
                            'error': str(e),
                            'updated_at': datetime.now().isoformat()
                        }
                    }
                )
            except:
                pass
        finally:
            # 清理任务缓存
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
    
    def _select_matcher(self, algorithm_type: str):
        """
        选择匹配算法
        
        Args:
            algorithm_type: 算法类型
            
        Returns:
            匹配器实例
        """
        matchers = {
            'exact': self.exact_matcher,
            'fuzzy': self.fuzzy_matcher,
            'enhanced': self.enhanced_fuzzy_matcher,
            'optimized': self.optimized_processor if hasattr(self, 'optimized_processor') else self.enhanced_fuzzy_matcher,
            'hybrid': self.enhanced_fuzzy_matcher  # 混合算法使用增强模糊匹配
        }
        
        return matchers.get(algorithm_type, self.enhanced_fuzzy_matcher)
    
    def _execute_batch_matching(self, source_records: List[Dict], target_data: Dict[str, List[Dict]],
                               mappings: List[Dict], matcher, similarity_threshold: float,
                               max_results: int) -> List[Dict]:
        """
        执行批次匹配
        
        Args:
            source_records: 源记录列表
            target_data: 目标数据字典
            mappings: 字段映射配置
            matcher: 匹配器
            similarity_threshold: 相似度阈值
            max_results: 最大结果数
            
        Returns:
            List[Dict]: 匹配结果列表
        """
        batch_results = []
        
        for source_record in source_records:
            # 对每个目标表进行匹配
            for target_table, target_records in target_data.items():
                # 获取该目标表的映射配置
                table_mappings = [m for m in mappings if m['target_table'] == target_table]
                
                if not table_mappings:
                    continue
                
                # 执行匹配
                matches = self._match_record_to_targets(
                    source_record, target_records, table_mappings,
                    matcher, similarity_threshold, max_results
                )
                
                # 添加到结果中，参照原项目设计，保留可追溯的源表ID字段
                for match in matches:
                    # 构建匹配结果，参照原项目的数据结构
                    result = {
                        # 基础标识信息
                        'source_id': str(source_record.get('_id', '')),
                        'target_id': str(match['target_record'].get('_id', '')),
                        'source_table': source_table,
                        'target_table': target_table,
                        
                        # 匹配核心信息
                        'similarity_score': match['similarity'],
                        'matched_fields': match['matched_fields'],
                        'match_algorithm': config.get('algorithm_type', 'enhanced'),
                        
                        # 源记录关键字段（用于快速查看和追溯）
                        'source_key_fields': self._extract_key_fields(source_record, mappings, 'source'),
                        'target_key_fields': self._extract_key_fields(match['target_record'], mappings, 'target'),
                        
                        # 完整记录数据（可选，用于详细分析）
                        'source_record': source_record if config.get('save_full_records', False) else None,
                        'target_record': match['target_record'] if config.get('save_full_records', False) else None,
                        
                        # 匹配详细信息
                        'match_details': {
                            'field_scores': match.get('details', {}).get('field_scores', {}),
                            'total_fields': match.get('details', {}).get('total_fields', 0),
                            'matched_field_count': match.get('details', {}).get('matched_field_count', 0),
                            'confidence_level': self._calculate_confidence_level(match['similarity']),
                            'match_type': self._determine_match_type(match['similarity'], match['matched_fields'])
                        },
                        
                        # 元数据信息
                        'created_at': datetime.now().isoformat(),
                        'task_id': task_id,
                        'config_id': config.get('config_id', ''),
                        'match_version': '2.0'  # 标识新版本匹配
                    }
                    batch_results.append(result)
        
        return batch_results
    
    def _match_record_to_targets(self, source_record: Dict, target_records: List[Dict],
                                mappings: List[Dict], matcher, similarity_threshold: float,
                                max_results: int) -> List[Dict]:
        """
        将源记录与目标记录进行匹配
        
        Args:
            source_record: 源记录
            target_records: 目标记录列表
            mappings: 字段映射
            matcher: 匹配器
            similarity_threshold: 相似度阈值
            max_results: 最大结果数
            
        Returns:
            List[Dict]: 匹配结果
        """
        matches = []
        
        for target_record in target_records:
            # 计算匹配相似度
            similarity, matched_fields, details = self._calculate_similarity(
                source_record, target_record, mappings, matcher
            )
            
            # 如果相似度达到阈值，添加到结果中
            if similarity >= similarity_threshold:
                matches.append({
                    'target_record': target_record,
                    'similarity': similarity,
                    'matched_fields': matched_fields,
                    'details': details
                })
        
        # 按相似度排序，返回前N个结果
        matches.sort(key=lambda x: x['similarity'], reverse=True)
        return matches[:max_results]
    
    def _calculate_similarity(self, source_record: Dict, target_record: Dict,
                            mappings: List[Dict], matcher) -> Tuple[float, List[str], Dict]:
        """
        计算两条记录的相似度
        
        Args:
            source_record: 源记录
            target_record: 目标记录
            mappings: 字段映射
            matcher: 匹配器
            
        Returns:
            Tuple[float, List[str], Dict]: (相似度, 匹配字段列表, 详细信息)
        """
        total_score = 0.0
        field_scores = {}
        matched_fields = []
        
        for mapping in mappings:
            source_field = mapping['source_field']
            target_field = mapping['target_field']
            
            source_value = source_record.get(source_field, '')
            target_value = target_record.get(target_field, '')
            
            # 跳过空值
            if not source_value or not target_value:
                continue
            
            # 计算字段相似度
            field_similarity = self._calculate_field_similarity(
                str(source_value), str(target_value), matcher
            )
            
            field_scores[f"{source_field}->{target_field}"] = field_similarity
            total_score += field_similarity
            
            if field_similarity > 0.5:  # 字段匹配阈值
                matched_fields.append(f"{source_field}->{target_field}")
        
        # 计算平均相似度
        avg_similarity = total_score / len(mappings) if mappings else 0.0
        
        details = {
            'field_scores': field_scores,
            'total_fields': len(mappings),
            'matched_field_count': len(matched_fields)
        }
        
        return avg_similarity, matched_fields, details
    
    def _calculate_field_similarity(self, value1: str, value2: str, matcher) -> float:
        """
        计算两个字段值的相似度
        
        Args:
            value1: 值1
            value2: 值2
            matcher: 匹配器
            
        Returns:
            float: 相似度 (0-1)
        """
        try:
            # 使用不同的匹配器计算相似度
            if hasattr(matcher, 'calculate_similarity'):
                return matcher.calculate_similarity(value1, value2)
            elif hasattr(matcher, 'fuzzy_match_score'):
                return matcher.fuzzy_match_score(value1, value2)
            else:
                # 默认使用相似度评分器
                return self.similarity_calculator.calculate_string_similarity(value1, value2)
        except Exception as e:
            logger.warning(f"计算字段相似度失败: {str(e)}")
            return 0.0
    
    def preview_matching(self, config_id: str, preview_count: int = 5,
                        algorithm_type: str = 'enhanced',
                        similarity_threshold: float = 0.7) -> List[Dict]:
        """
        预览匹配结果
        
        Args:
            config_id: 配置ID
            preview_count: 预览数量
            algorithm_type: 算法类型
            similarity_threshold: 相似度阈值
            
        Returns:
            List[Dict]: 预览结果
        """
        try:
            if not self.db_manager or not self.db_manager.mongo_client:
                raise Exception("数据库连接未初始化")
            
            db = self.db_manager.mongo_client.get_database()
            
            # 获取字段映射配置
            config_collection = db['field_mapping_configs']
            config = config_collection.find_one({'config_id': config_id})
            
            if not config:
                raise Exception("映射配置不存在")
            
            # 获取少量数据进行预览
            source_collection = db[config['source_table']]
            source_records = list(source_collection.find({}).limit(preview_count))
            
            target_data = {}
            for target_table in config['target_tables']:
                target_collection = db[target_table]
                target_data[target_table] = list(target_collection.find({}).limit(100))  # 限制目标数据量
            
            # 选择匹配器
            matcher = self._select_matcher(algorithm_type)
            
            # 执行预览匹配
            preview_results = []
            for source_record in source_records:
                for target_table, target_records in target_data.items():
                    table_mappings = [m for m in config['mappings'] if m['target_table'] == target_table]
                    
                    if not table_mappings:
                        continue
                    
                    matches = self._match_record_to_targets(
                        source_record, target_records, table_mappings,
                        matcher, similarity_threshold, 1  # 每个源记录只返回最佳匹配
                    )
                    
                    if matches:
                        preview_results.append({
                            'source_record': {k: v for k, v in source_record.items() if k != '_id'},
                            'matched_record': {k: v for k, v in matches[0]['target_record'].items() if k != '_id'},
                            'similarity': matches[0]['similarity'],
                            'matched_fields': matches[0]['matched_fields'],
                            'target_table': target_table
                        })
            
            return preview_results[:preview_count]
            
        except Exception as e:
            logger.error(f"预览匹配失败: {str(e)}")
            raise
    
    def stop_task(self, task_id: str) -> bool:
        """
        停止匹配任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否成功
        """
        try:
            if not self.db_manager or not self.db_manager.mongo_client:
                return False
            
            db = self.db_manager.mongo_client.get_database()
            task_collection = db['user_matching_tasks']
            
            # 更新任务状态为停止
            result = task_collection.update_one(
                {'task_id': task_id},
                {
                    '$set': {
                        'status': 'stopped',
                        'updated_at': datetime.now().isoformat()
                    }
                }
            )
            
            # 清理任务缓存
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
            
            return result.modified_count > 0
            
        except Exception as e:
            logger.error(f"停止任务失败: {str(e)}")
            return False
    
    def get_task_progress(self, task_id: str) -> Optional[Dict]:
        """
        获取任务进度
        
        Args:
            task_id: 任务ID
            
        Returns:
            Optional[Dict]: 任务进度信息
        """
        try:
            if not self.db_manager or not self.db_manager.mongo_client:
                return None
            
            db = self.db_manager.mongo_client.get_database()
            task_collection = db['user_matching_tasks']
            
            task = task_collection.find_one({'task_id': task_id})
            
            if not task:
                return None
            
            return {
                'progress': task.get('progress', {}),
                'status': task.get('status', 'unknown'),
                'error': task.get('error'),
                'created_at': task.get('created_at'),
                'updated_at': task.get('updated_at')
            }
            
        except Exception as e:
            logger.error(f"获取任务进度失败: {str(e)}")
            return None
    
    def _extract_key_fields(self, record: Dict, mappings: List[Dict], field_type: str) -> Dict:
        """
        提取记录的关键字段（用于快速查看和追溯）
        
        Args:
            record: 记录数据
            mappings: 字段映射配置
            field_type: 字段类型 ('source' 或 'target')
            
        Returns:
            Dict: 关键字段字典
        """
        key_fields = {}
        
        for mapping in mappings:
            if field_type == 'source':
                field_name = mapping['source_field']
            else:
                field_name = mapping['target_field']
            
            value = record.get(field_name)
            if value is not None:
                key_fields[field_name] = str(value)[:100]  # 限制长度
        
        return key_fields
    
    def _calculate_confidence_level(self, similarity_score: float) -> str:
        """
        计算置信度等级
        
        Args:
            similarity_score: 相似度分数
            
        Returns:
            str: 置信度等级
        """
        if similarity_score >= 0.9:
            return 'very_high'
        elif similarity_score >= 0.8:
            return 'high'
        elif similarity_score >= 0.7:
            return 'medium'
        elif similarity_score >= 0.6:
            return 'low'
        else:
            return 'very_low'
    
    def _determine_match_type(self, similarity_score: float, matched_fields: List[str]) -> str:
        """
        确定匹配类型
        
        Args:
            similarity_score: 相似度分数
            matched_fields: 匹配的字段列表
            
        Returns:
            str: 匹配类型
        """
        field_count = len(matched_fields)
        
        if similarity_score >= 0.95 and field_count >= 3:
            return 'exact_match'
        elif similarity_score >= 0.8 and field_count >= 2:
            return 'strong_match'
        elif similarity_score >= 0.7:
            return 'good_match'
        elif similarity_score >= 0.6:
            return 'weak_match'
        else:
            return 'poor_match'
