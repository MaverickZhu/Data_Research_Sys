"""
知识图谱质量评估器
提供全面的知识图谱质量监控和评估功能
"""

import logging
import math
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import statistics

from .kg_models import Entity, Relation, KnowledgeTriple, EntityType, RelationType
from .kg_store import KnowledgeGraphStore

logger = logging.getLogger(__name__)

class KnowledgeGraphQualityAssessor:
    """知识图谱质量评估器"""
    
    def __init__(self, kg_store: KnowledgeGraphStore, config: Dict[str, Any] = None):
        """
        初始化质量评估器
        
        Args:
            kg_store: 知识图谱存储引擎
            config: 配置参数
        """
        self.kg_store = kg_store
        self.config = config or {}
        
        # 质量评估阈值
        self.quality_thresholds = {
            'entity_confidence_min': self.config.get('entity_confidence_min', 0.7),
            'relation_confidence_min': self.config.get('relation_confidence_min', 0.6),
            'triple_confidence_min': self.config.get('triple_confidence_min', 0.6),
            'completeness_min': self.config.get('completeness_min', 0.8),
            'consistency_min': self.config.get('consistency_min', 0.9),
            'accuracy_min': self.config.get('accuracy_min', 0.85)
        }
        
        # 质量权重
        self.quality_weights = {
            'accuracy': self.config.get('accuracy_weight', 0.3),
            'completeness': self.config.get('completeness_weight', 0.25),
            'consistency': self.config.get('consistency_weight', 0.25),
            'timeliness': self.config.get('timeliness_weight', 0.1),
            'uniqueness': self.config.get('uniqueness_weight', 0.1)
        }
        
        logger.info("知识图谱质量评估器初始化完成")
    
    def assess_overall_quality(self) -> Dict[str, Any]:
        """
        评估知识图谱整体质量
        
        Returns:
            Dict: 质量评估报告
        """
        logger.info("开始进行知识图谱整体质量评估")
        start_time = datetime.now()
        
        try:
            # 1. 实体质量评估
            entity_quality = self._assess_entity_quality()
            
            # 2. 关系质量评估
            relation_quality = self._assess_relation_quality()
            
            # 3. 三元组质量评估
            triple_quality = self._assess_triple_quality()
            
            # 4. 图结构质量评估
            structure_quality = self._assess_graph_structure()
            
            # 5. 数据一致性评估
            consistency_quality = self._assess_data_consistency()
            
            # 6. 计算综合质量分数
            overall_score = self._calculate_overall_quality_score({
                'entity_quality': entity_quality,
                'relation_quality': relation_quality,
                'triple_quality': triple_quality,
                'structure_quality': structure_quality,
                'consistency_quality': consistency_quality
            })
            
            # 7. 生成质量建议
            recommendations = self._generate_quality_recommendations({
                'entity_quality': entity_quality,
                'relation_quality': relation_quality,
                'triple_quality': triple_quality,
                'structure_quality': structure_quality,
                'consistency_quality': consistency_quality
            })
            
            assessment_time = (datetime.now() - start_time).total_seconds()
            
            quality_report = {
                'overall_score': overall_score,
                'assessment_time': assessment_time,
                'timestamp': datetime.now().isoformat(),
                'detailed_scores': {
                    'entity_quality': entity_quality,
                    'relation_quality': relation_quality,
                    'triple_quality': triple_quality,
                    'structure_quality': structure_quality,
                    'consistency_quality': consistency_quality
                },
                'recommendations': recommendations,
                'thresholds': self.quality_thresholds,
                'weights': self.quality_weights
            }
            
            logger.info(f"知识图谱质量评估完成，总分: {overall_score:.2f}, 耗时: {assessment_time:.2f}秒")
            return quality_report
            
        except Exception as e:
            logger.error(f"知识图谱质量评估失败: {str(e)}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def _assess_entity_quality(self) -> Dict[str, Any]:
        """评估实体质量"""
        logger.debug("开始评估实体质量")
        
        try:
            # 获取所有实体
            entities = self.kg_store.find_entities()
            
            if not entities:
                return {'score': 0.0, 'details': {'error': '没有找到实体'}}
            
            # 置信度分析
            confidences = [e.confidence for e in entities if e.confidence is not None]
            confidence_stats = {
                'average': statistics.mean(confidences) if confidences else 0.0,
                'median': statistics.median(confidences) if confidences else 0.0,
                'min': min(confidences) if confidences else 0.0,
                'max': max(confidences) if confidences else 0.0,
                'std_dev': statistics.stdev(confidences) if len(confidences) > 1 else 0.0
            }
            
            # 高质量实体比例
            high_quality_count = sum(1 for c in confidences if c >= self.quality_thresholds['entity_confidence_min'])
            high_quality_ratio = high_quality_count / len(confidences) if confidences else 0.0
            
            # 实体完整性检查
            completeness_score = self._check_entity_completeness(entities)
            
            # 实体唯一性检查
            uniqueness_score = self._check_entity_uniqueness(entities)
            
            # 实体类型分布
            type_distribution = Counter([e.type.value for e in entities])
            
            # 计算实体质量分数
            entity_score = (
                confidence_stats['average'] * 0.4 +
                high_quality_ratio * 0.3 +
                completeness_score * 0.2 +
                uniqueness_score * 0.1
            )
            
            return {
                'score': entity_score,
                'details': {
                    'total_entities': len(entities),
                    'confidence_stats': confidence_stats,
                    'high_quality_ratio': high_quality_ratio,
                    'completeness_score': completeness_score,
                    'uniqueness_score': uniqueness_score,
                    'type_distribution': dict(type_distribution)
                }
            }
            
        except Exception as e:
            logger.error(f"实体质量评估失败: {str(e)}")
            return {'score': 0.0, 'details': {'error': str(e)}}
    
    def _assess_relation_quality(self) -> Dict[str, Any]:
        """评估关系质量"""
        logger.debug("开始评估关系质量")
        
        try:
            # 获取所有三元组（包含关系信息）
            triples = self.kg_store.find_triples()
            
            if not triples:
                return {'score': 0.0, 'details': {'error': '没有找到关系'}}
            
            # 关系置信度分析
            relation_confidences = [t.predicate.confidence for t in triples 
                                  if t.predicate and t.predicate.confidence is not None]
            
            confidence_stats = {
                'average': statistics.mean(relation_confidences) if relation_confidences else 0.0,
                'median': statistics.median(relation_confidences) if relation_confidences else 0.0,
                'min': min(relation_confidences) if relation_confidences else 0.0,
                'max': max(relation_confidences) if relation_confidences else 0.0
            }
            
            # 关系类型分布
            relation_types = [t.predicate.type.value for t in triples if t.predicate]
            type_distribution = Counter(relation_types)
            
            # 关系一致性检查
            consistency_score = self._check_relation_consistency(triples)
            
            # 关系完整性检查
            completeness_score = self._check_relation_completeness(triples)
            
            # 高质量关系比例
            high_quality_count = sum(1 for c in relation_confidences 
                                   if c >= self.quality_thresholds['relation_confidence_min'])
            high_quality_ratio = high_quality_count / len(relation_confidences) if relation_confidences else 0.0
            
            # 计算关系质量分数
            relation_score = (
                confidence_stats['average'] * 0.4 +
                consistency_score * 0.3 +
                completeness_score * 0.2 +
                high_quality_ratio * 0.1
            )
            
            return {
                'score': relation_score,
                'details': {
                    'total_relations': len(triples),
                    'confidence_stats': confidence_stats,
                    'consistency_score': consistency_score,
                    'completeness_score': completeness_score,
                    'high_quality_ratio': high_quality_ratio,
                    'type_distribution': dict(type_distribution)
                }
            }
            
        except Exception as e:
            logger.error(f"关系质量评估失败: {str(e)}")
            return {'score': 0.0, 'details': {'error': str(e)}}
    
    def _assess_triple_quality(self) -> Dict[str, Any]:
        """评估三元组质量"""
        logger.debug("开始评估三元组质量")
        
        try:
            triples = self.kg_store.find_triples()
            
            if not triples:
                return {'score': 0.0, 'details': {'error': '没有找到三元组'}}
            
            # 三元组置信度分析
            triple_confidences = [t.confidence for t in triples if t.confidence is not None]
            
            confidence_stats = {
                'average': statistics.mean(triple_confidences) if triple_confidences else 0.0,
                'median': statistics.median(triple_confidences) if triple_confidences else 0.0,
                'min': min(triple_confidences) if triple_confidences else 0.0,
                'max': max(triple_confidences) if triple_confidences else 0.0
            }
            
            # 三元组完整性检查
            complete_triples = sum(1 for t in triples 
                                 if t.subject and t.predicate and t.object)
            completeness_ratio = complete_triples / len(triples) if triples else 0.0
            
            # 证据质量评估
            evidence_quality = self._assess_triple_evidence_quality(triples)
            
            # 高质量三元组比例
            high_quality_count = sum(1 for c in triple_confidences 
                                   if c >= self.quality_thresholds['triple_confidence_min'])
            high_quality_ratio = high_quality_count / len(triple_confidences) if triple_confidences else 0.0
            
            # 计算三元组质量分数
            triple_score = (
                confidence_stats['average'] * 0.4 +
                completeness_ratio * 0.3 +
                evidence_quality * 0.2 +
                high_quality_ratio * 0.1
            )
            
            return {
                'score': triple_score,
                'details': {
                    'total_triples': len(triples),
                    'confidence_stats': confidence_stats,
                    'completeness_ratio': completeness_ratio,
                    'evidence_quality': evidence_quality,
                    'high_quality_ratio': high_quality_ratio
                }
            }
            
        except Exception as e:
            logger.error(f"三元组质量评估失败: {str(e)}")
            return {'score': 0.0, 'details': {'error': str(e)}}
    
    def _assess_graph_structure(self) -> Dict[str, Any]:
        """评估图结构质量"""
        logger.debug("开始评估图结构质量")
        
        try:
            entities = self.kg_store.find_entities()
            triples = self.kg_store.find_triples()
            
            if not entities or not triples:
                return {'score': 0.0, 'details': {'error': '图数据不足'}}
            
            # 图连通性分析
            connectivity_score = self._analyze_graph_connectivity(entities, triples)
            
            # 度分布分析
            degree_distribution = self._analyze_degree_distribution(entities, triples)
            
            # 图密度计算
            graph_density = self._calculate_graph_density(entities, triples)
            
            # 聚类系数计算
            clustering_coefficient = self._calculate_clustering_coefficient(entities, triples)
            
            # 路径长度分析
            avg_path_length = self._calculate_average_path_length(entities, triples)
            
            # 计算结构质量分数
            structure_score = (
                connectivity_score * 0.3 +
                min(graph_density * 10, 1.0) * 0.25 +  # 密度标准化
                clustering_coefficient * 0.25 +
                (1.0 / max(avg_path_length, 1.0)) * 0.2  # 路径长度越短越好
            )
            
            return {
                'score': structure_score,
                'details': {
                    'connectivity_score': connectivity_score,
                    'degree_distribution': degree_distribution,
                    'graph_density': graph_density,
                    'clustering_coefficient': clustering_coefficient,
                    'average_path_length': avg_path_length
                }
            }
            
        except Exception as e:
            logger.error(f"图结构质量评估失败: {str(e)}")
            return {'score': 0.0, 'details': {'error': str(e)}}
    
    def _assess_data_consistency(self) -> Dict[str, Any]:
        """评估数据一致性"""
        logger.debug("开始评估数据一致性")
        
        try:
            entities = self.kg_store.find_entities()
            triples = self.kg_store.find_triples()
            
            # 实体引用一致性
            entity_ref_consistency = self._check_entity_reference_consistency(entities, triples)
            
            # 关系类型一致性
            relation_type_consistency = self._check_relation_type_consistency(triples)
            
            # 数据类型一致性
            data_type_consistency = self._check_data_type_consistency(entities)
            
            # 时间戳一致性
            timestamp_consistency = self._check_timestamp_consistency(entities, triples)
            
            # 计算一致性分数
            consistency_score = (
                entity_ref_consistency * 0.3 +
                relation_type_consistency * 0.3 +
                data_type_consistency * 0.2 +
                timestamp_consistency * 0.2
            )
            
            return {
                'score': consistency_score,
                'details': {
                    'entity_reference_consistency': entity_ref_consistency,
                    'relation_type_consistency': relation_type_consistency,
                    'data_type_consistency': data_type_consistency,
                    'timestamp_consistency': timestamp_consistency
                }
            }
            
        except Exception as e:
            logger.error(f"数据一致性评估失败: {str(e)}")
            return {'score': 0.0, 'details': {'error': str(e)}}
    
    # ====== 辅助评估方法 ======
    
    def _check_entity_completeness(self, entities: List[Entity]) -> float:
        """检查实体完整性"""
        if not entities:
            return 0.0
        
        complete_count = 0
        for entity in entities:
            # 检查必需字段
            has_label = entity.label and len(entity.label.strip()) > 0
            has_type = entity.type is not None
            has_properties = entity.properties and len(entity.properties) > 0
            
            if has_label and has_type and has_properties:
                complete_count += 1
        
        return complete_count / len(entities)
    
    def _check_entity_uniqueness(self, entities: List[Entity]) -> float:
        """检查实体唯一性"""
        if not entities:
            return 0.0
        
        # 基于标签和类型的唯一性检查
        entity_keys = set()
        duplicate_count = 0
        
        for entity in entities:
            key = f"{entity.type.value}:{entity.label.lower()}"
            if key in entity_keys:
                duplicate_count += 1
            else:
                entity_keys.add(key)
        
        uniqueness_ratio = 1.0 - (duplicate_count / len(entities))
        return max(uniqueness_ratio, 0.0)
    
    def _check_relation_consistency(self, triples: List[KnowledgeTriple]) -> float:
        """检查关系一致性"""
        if not triples:
            return 0.0
        
        consistent_count = 0
        for triple in triples:
            # 检查关系是否符合实体类型约束
            if self._is_relation_type_consistent(triple):
                consistent_count += 1
        
        return consistent_count / len(triples)
    
    def _check_relation_completeness(self, triples: List[KnowledgeTriple]) -> float:
        """检查关系完整性"""
        if not triples:
            return 0.0
        
        complete_count = 0
        for triple in triples:
            # 检查三元组是否完整
            has_subject = triple.subject is not None
            has_predicate = triple.predicate is not None
            has_object = triple.object is not None
            has_confidence = triple.confidence is not None
            
            if has_subject and has_predicate and has_object and has_confidence:
                complete_count += 1
        
        return complete_count / len(triples)
    
    def _is_relation_type_consistent(self, triple: KnowledgeTriple) -> bool:
        """检查关系类型是否一致"""
        if not triple.subject or not triple.predicate or not triple.object:
            return False
        
        # 基于关系类型检查实体类型约束
        relation_type = triple.predicate.type
        subject_type = triple.subject.type
        object_type = triple.object.type
        
        # 定义类型约束规则
        type_constraints = {
            RelationType.LOCATED_IN: {
                'subject_types': [EntityType.ORGANIZATION, EntityType.PERSON],
                'object_types': [EntityType.LOCATION]
            },
            RelationType.MANAGED_BY: {
                'subject_types': [EntityType.ORGANIZATION],
                'object_types': [EntityType.PERSON]
            },
            RelationType.REPRESENTS: {
                'subject_types': [EntityType.PERSON],
                'object_types': [EntityType.ORGANIZATION]
            },
            RelationType.CONTACT_OF: {
                'subject_types': [EntityType.ORGANIZATION, EntityType.PERSON],
                'object_types': [EntityType.IDENTIFIER]
            }
        }
        
        if relation_type in type_constraints:
            constraint = type_constraints[relation_type]
            subject_valid = subject_type in constraint['subject_types']
            object_valid = object_type in constraint['object_types']
            return subject_valid and object_valid
        
        return True  # 未定义约束的关系认为是一致的
    
    def _assess_triple_evidence_quality(self, triples: List[KnowledgeTriple]) -> float:
        """评估三元组证据质量"""
        if not triples:
            return 0.0
        
        evidence_scores = []
        for triple in triples:
            if triple.evidence:
                # 证据数量分数
                evidence_count_score = min(len(triple.evidence) / 3.0, 1.0)
                
                # 证据多样性分数
                evidence_sources = set()
                for evidence in triple.evidence:
                    if ':' in evidence:
                        source = evidence.split(':', 1)[0]
                        evidence_sources.add(source)
                
                diversity_score = min(len(evidence_sources) / 2.0, 1.0)
                
                # 综合证据分数
                evidence_score = (evidence_count_score + diversity_score) / 2.0
                evidence_scores.append(evidence_score)
            else:
                evidence_scores.append(0.0)
        
        return statistics.mean(evidence_scores) if evidence_scores else 0.0
    
    def _analyze_graph_connectivity(self, entities: List[Entity], 
                                  triples: List[KnowledgeTriple]) -> float:
        """分析图连通性"""
        if not entities or not triples:
            return 0.0
        
        # 构建邻接表
        adjacency = defaultdict(set)
        for triple in triples:
            if triple.subject and triple.object:
                adjacency[triple.subject.id].add(triple.object.id)
                adjacency[triple.object.id].add(triple.subject.id)
        
        # 使用DFS查找连通组件
        visited = set()
        connected_components = []
        
        for entity in entities:
            if entity.id not in visited:
                component = set()
                self._dfs(entity.id, adjacency, visited, component)
                if component:
                    connected_components.append(component)
        
        # 计算连通性分数
        if not connected_components:
            return 0.0
        
        largest_component_size = max(len(comp) for comp in connected_components)
        connectivity_score = largest_component_size / len(entities)
        
        return connectivity_score
    
    def _dfs(self, node_id: str, adjacency: Dict, visited: Set, component: Set) -> None:
        """深度优先搜索"""
        visited.add(node_id)
        component.add(node_id)
        
        for neighbor in adjacency.get(node_id, []):
            if neighbor not in visited:
                self._dfs(neighbor, adjacency, visited, component)
    
    def _analyze_degree_distribution(self, entities: List[Entity], 
                                   triples: List[KnowledgeTriple]) -> Dict[str, Any]:
        """分析度分布"""
        if not entities or not triples:
            return {'average_degree': 0.0, 'degree_distribution': {}}
        
        degree_count = defaultdict(int)
        
        for triple in triples:
            if triple.subject:
                degree_count[triple.subject.id] += 1
            if triple.object:
                degree_count[triple.object.id] += 1
        
        degrees = list(degree_count.values())
        average_degree = statistics.mean(degrees) if degrees else 0.0
        
        # 度分布统计
        degree_distribution = Counter(degrees)
        
        return {
            'average_degree': average_degree,
            'max_degree': max(degrees) if degrees else 0,
            'min_degree': min(degrees) if degrees else 0,
            'degree_distribution': dict(degree_distribution)
        }
    
    def _calculate_graph_density(self, entities: List[Entity], 
                                triples: List[KnowledgeTriple]) -> float:
        """计算图密度"""
        if len(entities) < 2:
            return 0.0
        
        max_possible_edges = len(entities) * (len(entities) - 1)  # 有向图
        actual_edges = len(triples)
        
        return actual_edges / max_possible_edges if max_possible_edges > 0 else 0.0
    
    def _calculate_clustering_coefficient(self, entities: List[Entity], 
                                        triples: List[KnowledgeTriple]) -> float:
        """计算聚类系数（简化版）"""
        # 简化实现：返回基于三角形数量的近似聚类系数
        if len(entities) < 3:
            return 0.0
        
        # 构建邻接表
        adjacency = defaultdict(set)
        for triple in triples:
            if triple.subject and triple.object:
                adjacency[triple.subject.id].add(triple.object.id)
        
        # 计算三角形数量
        triangles = 0
        for entity_id in adjacency:
            neighbors = list(adjacency[entity_id])
            for i in range(len(neighbors)):
                for j in range(i + 1, len(neighbors)):
                    if neighbors[j] in adjacency[neighbors[i]]:
                        triangles += 1
        
        # 简化的聚类系数计算
        max_triangles = len(entities) * (len(entities) - 1) * (len(entities) - 2) / 6
        return triangles / max_triangles if max_triangles > 0 else 0.0
    
    def _calculate_average_path_length(self, entities: List[Entity], 
                                     triples: List[KnowledgeTriple]) -> float:
        """计算平均路径长度（简化版）"""
        if len(entities) < 2:
            return 0.0
        
        # 简化实现：基于图的直径估算
        # 实际应用中可以使用BFS计算所有节点对的最短路径
        return math.log(len(entities)) if len(entities) > 1 else 1.0
    
    def _check_entity_reference_consistency(self, entities: List[Entity], 
                                          triples: List[KnowledgeTriple]) -> float:
        """检查实体引用一致性"""
        if not entities or not triples:
            return 0.0
        
        entity_ids = set(e.id for e in entities)
        referenced_entity_ids = set()
        
        for triple in triples:
            if triple.subject:
                referenced_entity_ids.add(triple.subject.id)
            if triple.object:
                referenced_entity_ids.add(triple.object.id)
        
        # 检查引用的实体是否都存在
        valid_references = len(referenced_entity_ids.intersection(entity_ids))
        total_references = len(referenced_entity_ids)
        
        return valid_references / total_references if total_references > 0 else 1.0
    
    def _check_relation_type_consistency(self, triples: List[KnowledgeTriple]) -> float:
        """检查关系类型一致性"""
        if not triples:
            return 0.0
        
        consistent_count = sum(1 for triple in triples 
                             if self._is_relation_type_consistent(triple))
        
        return consistent_count / len(triples)
    
    def _check_data_type_consistency(self, entities: List[Entity]) -> float:
        """检查数据类型一致性"""
        if not entities:
            return 0.0
        
        consistent_count = 0
        for entity in entities:
            # 检查实体属性的数据类型一致性
            if self._is_entity_data_type_consistent(entity):
                consistent_count += 1
        
        return consistent_count / len(entities)
    
    def _is_entity_data_type_consistent(self, entity: Entity) -> bool:
        """检查单个实体的数据类型一致性"""
        if not entity.properties:
            return True
        
        # 检查属性值的类型一致性
        for key, value in entity.properties.items():
            if key.lower() in ['confidence', 'score', 'weight']:
                # 数值类型属性
                try:
                    float(value)
                except (ValueError, TypeError):
                    return False
            elif key.lower() in ['created_time', 'updated_time', 'timestamp']:
                # 时间类型属性
                if not self._is_valid_timestamp(value):
                    return False
        
        return True
    
    def _is_valid_timestamp(self, timestamp_str: str) -> bool:
        """验证时间戳格式"""
        try:
            datetime.fromisoformat(str(timestamp_str).replace('Z', '+00:00'))
            return True
        except (ValueError, TypeError):
            return False
    
    def _check_timestamp_consistency(self, entities: List[Entity], 
                                   triples: List[KnowledgeTriple]) -> float:
        """检查时间戳一致性"""
        consistent_items = 0
        total_items = 0
        
        # 检查实体时间戳
        for entity in entities:
            total_items += 1
            if (hasattr(entity, 'created_time') and hasattr(entity, 'updated_time') and
                entity.created_time and entity.updated_time):
                if entity.updated_time >= entity.created_time:
                    consistent_items += 1
            else:
                consistent_items += 1  # 没有时间戳认为是一致的
        
        # 检查三元组时间戳
        for triple in triples:
            total_items += 1
            if hasattr(triple, 'created_time') and triple.created_time:
                if self._is_valid_timestamp(triple.created_time):
                    consistent_items += 1
            else:
                consistent_items += 1  # 没有时间戳认为是一致的
        
        return consistent_items / total_items if total_items > 0 else 1.0
    
    def _calculate_overall_quality_score(self, quality_scores: Dict[str, Dict]) -> float:
        """计算综合质量分数"""
        weighted_score = 0.0
        
        # 提取各维度分数
        accuracy = quality_scores.get('entity_quality', {}).get('score', 0.0)
        completeness = quality_scores.get('triple_quality', {}).get('score', 0.0)
        consistency = quality_scores.get('consistency_quality', {}).get('score', 0.0)
        structure = quality_scores.get('structure_quality', {}).get('score', 0.0)
        relation_quality = quality_scores.get('relation_quality', {}).get('score', 0.0)
        
        # 加权计算
        weighted_score = (
            accuracy * self.quality_weights['accuracy'] +
            completeness * self.quality_weights['completeness'] +
            consistency * self.quality_weights['consistency'] +
            structure * 0.1 +  # 结构权重
            relation_quality * 0.1  # 关系权重
        )
        
        return min(weighted_score, 1.0)
    
    def _generate_quality_recommendations(self, quality_scores: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """生成质量改进建议"""
        recommendations = []
        
        # 实体质量建议
        entity_score = quality_scores.get('entity_quality', {}).get('score', 0.0)
        if entity_score < self.quality_thresholds['entity_confidence_min']:
            recommendations.append({
                'type': 'entity_quality',
                'priority': 'high',
                'issue': '实体质量偏低',
                'recommendation': '建议提高实体识别算法的准确性，增加实体验证规则',
                'expected_improvement': '预期可提升10-20%的整体质量'
            })
        
        # 关系质量建议
        relation_score = quality_scores.get('relation_quality', {}).get('score', 0.0)
        if relation_score < self.quality_thresholds['relation_confidence_min']:
            recommendations.append({
                'type': 'relation_quality',
                'priority': 'high',
                'issue': '关系质量偏低',
                'recommendation': '建议优化关系抽取算法，增加关系验证机制',
                'expected_improvement': '预期可提升15-25%的关系准确性'
            })
        
        # 一致性建议
        consistency_score = quality_scores.get('consistency_quality', {}).get('score', 0.0)
        if consistency_score < self.quality_thresholds['consistency_min']:
            recommendations.append({
                'type': 'consistency',
                'priority': 'medium',
                'issue': '数据一致性问题',
                'recommendation': '建议添加数据一致性检查和清理规则',
                'expected_improvement': '预期可提升5-15%的数据质量'
            })
        
        # 结构质量建议
        structure_score = quality_scores.get('structure_quality', {}).get('score', 0.0)
        if structure_score < 0.6:
            recommendations.append({
                'type': 'graph_structure',
                'priority': 'low',
                'issue': '图结构稀疏',
                'recommendation': '建议增加更多的实体关系，提高图的连通性',
                'expected_improvement': '预期可提升图查询和推理效果'
            })
        
        return recommendations
    
    def get_quality_trends(self, days: int = 7) -> Dict[str, Any]:
        """获取质量趋势分析"""
        logger.info(f"开始分析过去{days}天的质量趋势")
        
        try:
            # 简化实现：基于实体和三元组的时间戳分析质量趋势
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # 获取时间范围内的数据
            entities = self.kg_store.find_entities()
            triples = self.kg_store.find_triples()
            
            # 按日期分组分析
            daily_stats = defaultdict(lambda: {'entities': 0, 'triples': 0, 'avg_confidence': 0.0})
            
            for entity in entities:
                if hasattr(entity, 'created_time') and entity.created_time:
                    try:
                        created_date = datetime.fromisoformat(entity.created_time.replace('Z', '+00:00'))
                        if start_date <= created_date <= end_date:
                            date_key = created_date.date().isoformat()
                            daily_stats[date_key]['entities'] += 1
                    except (ValueError, TypeError):
                        continue
            
            for triple in triples:
                if hasattr(triple, 'created_time') and triple.created_time:
                    try:
                        created_date = datetime.fromisoformat(triple.created_time.replace('Z', '+00:00'))
                        if start_date <= created_date <= end_date:
                            date_key = created_date.date().isoformat()
                            daily_stats[date_key]['triples'] += 1
                            if triple.confidence:
                                daily_stats[date_key]['avg_confidence'] += triple.confidence
                    except (ValueError, TypeError):
                        continue
            
            # 计算平均置信度
            for date_key in daily_stats:
                if daily_stats[date_key]['triples'] > 0:
                    daily_stats[date_key]['avg_confidence'] /= daily_stats[date_key]['triples']
            
            return {
                'period': f'{start_date.date()} to {end_date.date()}',
                'daily_statistics': dict(daily_stats),
                'summary': {
                    'total_days': days,
                    'data_points': len(daily_stats),
                    'avg_daily_entities': statistics.mean([stats['entities'] for stats in daily_stats.values()]) if daily_stats else 0.0,
                    'avg_daily_triples': statistics.mean([stats['triples'] for stats in daily_stats.values()]) if daily_stats else 0.0
                }
            }
            
        except Exception as e:
            logger.error(f"质量趋势分析失败: {str(e)}")
            return {'error': str(e)}
    
    def get_quality_statistics(self) -> Dict[str, Any]:
        """获取质量统计信息"""
        return {
            'assessor_config': {
                'quality_thresholds': self.quality_thresholds,
                'quality_weights': self.quality_weights
            },
            'supported_assessments': [
                'overall_quality',
                'entity_quality',
                'relation_quality',
                'triple_quality',
                'graph_structure',
                'data_consistency',
                'quality_trends'
            ],
            'quality_dimensions': [
                'accuracy',
                'completeness',
                'consistency',
                'timeliness',
                'uniqueness'
            ]
        }
