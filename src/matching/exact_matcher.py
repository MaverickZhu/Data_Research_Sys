"""
精确匹配算法模块
用于处理单位名称和统一社会信用代码的精确匹配
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from .match_result import MatchResult, MultiMatchResult

logger = logging.getLogger(__name__)


class ExactMatcher:
    """精确匹配器"""
    
    def __init__(self, config: Dict):
        """
        初始化精确匹配器
        
        Args:
            config: 匹配配置参数
        """
        self.config = config
        self.exact_match_config = config.get('exact_match', {})
        self.fields_config = self.exact_match_config.get('fields', {})
    
    def _safe_str(self, value, default: str = '') -> str:
        """安全地将任何类型转换为字符串并去除空白"""
        if value is None:
            return default
        try:
            return str(value).strip()
        except Exception:
            return default
        
    def match_single_record(self, source_record: Dict, target_records: List[Dict]) -> MatchResult:
        """
        对单条记录进行精确匹配
        
        Args:
            source_record: 源记录（消防监督系统）
            target_records: 目标记录列表（安全排查系统）
            
        Returns:
            MatchResult: 匹配结果
        """
        try:
            # 仅使用统一社会信用代码进行精确匹配
            credit_code_result = self._match_by_credit_code(source_record, target_records)
            if credit_code_result.matched:
                logger.info(f"信用代码精确匹配成功: {source_record.get('UNIT_NAME', 'Unknown')}")
                return credit_code_result
            
            # 无精确匹配结果（只有信用代码匹配才算精确匹配）
            logger.debug(f"精确匹配失败: {source_record.get('UNIT_NAME', 'Unknown')}")
            return MatchResult(
                matched=False,
                match_type='none',
                confidence=0.0,
                source_record=source_record
            )
            
        except Exception as e:
            logger.error(f"精确匹配过程出错: {str(e)}")
            return MatchResult(
                matched=False,
                match_type='error',
                confidence=0.0,
                source_record=source_record,
                match_details={'error': str(e)}
            )
    
    def _match_by_credit_code(self, source_record: Dict, target_records: List[Dict]) -> MatchResult:
        """
        基于统一社会信用代码进行匹配
        
        Args:
            source_record: 源记录（安全排查系统）
            target_records: 目标记录列表（消防监督系统）
            
        Returns:
            MatchResult: 匹配结果
        """
        source_credit_code = self._normalize_credit_code(
            source_record.get('CREDIT_CODE', '')
        )
        
        # 如果源记录没有信用代码，直接返回未匹配
        if not source_credit_code:
            return MatchResult(
                matched=False,
                match_type='none',
                confidence=0.0,
                source_record=source_record
            )
        
        # 收集所有匹配的目标记录
        matched_targets = []
        for target_record in target_records:
            target_credit_code = self._normalize_credit_code(
                target_record.get('tyshxydm', '')
            )
            
            if target_credit_code and source_credit_code == target_credit_code:
                matched_targets.append(target_record)
        
        # 如果没有匹配记录
        if not matched_targets:
            return MatchResult(
                matched=False,
                match_type='none',
                confidence=0.0,
                source_record=source_record
            )
        
        # 如果只有一个匹配记录，直接返回
        if len(matched_targets) == 1:
            return MatchResult(
                matched=True,
                match_type='credit_code',
                confidence=1.0,
                source_record=source_record,
                target_record=matched_targets[0],
                match_details={
                    'source_credit_code': source_credit_code,
                    'target_credit_code': source_credit_code,
                    'match_basis': 'unified_social_credit_code',
                    'duplicate_count': 1
                }
            )
        
        # 如果有多个匹配记录，选择最佳匹配
        best_target = self._select_best_target_record(source_record, matched_targets)
        
        return MatchResult(
            matched=True,
            match_type='credit_code',
            confidence=1.0,
            source_record=source_record,
            target_record=best_target,
            match_details={
                'source_credit_code': source_credit_code,
                'target_credit_code': source_credit_code,
                'match_basis': 'unified_social_credit_code',
                'duplicate_count': len(matched_targets),
                'selection_reason': 'best_match_selected'
            }
        )
    
    def _select_best_target_record(self, source_record: Dict, target_candidates: List[Dict]) -> Dict:
        """
        从多个候选目标记录中选择最佳匹配
        
        优先级策略：
        1. 法定代表人匹配
        2. 地址相似度最高
        3. 联系电话匹配
        4. 数据完整度最高
        5. 记录ID最新（假设ID越大越新）
        
        Args:
            source_record: 源记录
            target_candidates: 候选目标记录列表
            
        Returns:
            Dict: 最佳匹配的目标记录
        """
        if len(target_candidates) == 1:
            return target_candidates[0]
        
        logger.info(f"发现 {len(target_candidates)} 个重复记录，开始智能选择最佳匹配")
        
        # 为每个候选记录计算匹配分数
        scored_candidates = []
        
        for target in target_candidates:
            score = 0
            reasons = []
            
            # 1. 法定代表人匹配 (权重: 40分)
            source_legal_rep = self._normalize_text(source_record.get('LEGAL_REPRESENTATIVE', ''))
            target_legal_rep = self._normalize_text(target.get('fddbr', ''))
            
            if source_legal_rep and target_legal_rep:
                if source_legal_rep == target_legal_rep:
                    score += 40
                    reasons.append("法定代表人完全匹配")
                elif self._text_similarity(source_legal_rep, target_legal_rep) > 0.8:
                    score += 20
                    reasons.append("法定代表人高度相似")
            
            # 2. 地址相似度 (权重: 30分)
            source_address = self._normalize_text(source_record.get('ADDRESS', ''))
            target_address = self._normalize_text(target.get('dwdz', ''))
            
            if source_address and target_address:
                address_similarity = self._address_similarity(source_address, target_address)
                address_score = int(address_similarity * 30)
                score += address_score
                if address_score > 20:
                    reasons.append(f"地址高度相似({address_similarity:.2f})")
                elif address_score > 10:
                    reasons.append(f"地址部分相似({address_similarity:.2f})")
            
            # 3. 联系电话匹配 (权重: 15分)
            source_phone = self._normalize_phone(source_record.get('CONTACT_PHONE', ''))
            target_phone = self._normalize_phone(target.get('lxdh', ''))
            
            if source_phone and target_phone:
                if source_phone == target_phone:
                    score += 15
                    reasons.append("联系电话完全匹配")
                elif source_phone in target_phone or target_phone in source_phone:
                    score += 8
                    reasons.append("联系电话部分匹配")
            
            # 4. 数据完整度 (权重: 10分)
            completeness_score = self._calculate_completeness_score(target)
            score += completeness_score
            if completeness_score > 5:
                reasons.append("数据完整度高")
            
            # 5. 记录新旧程度 (权重: 5分) - 假设ObjectId越大越新
            try:
                record_id = str(target.get('_id', ''))
                if record_id:
                    # 简单的新旧判断：ID字符串越大认为越新
                    id_score = min(5, len(record_id) / 5)
                    score += id_score
            except:
                pass
            
            scored_candidates.append({
                'record': target,
                'score': score,
                'reasons': reasons
            })
        
        # 按分数排序，选择最高分的记录
        scored_candidates.sort(key=lambda x: x['score'], reverse=True)
        best_candidate = scored_candidates[0]
        
        logger.info(f"最佳匹配选择完成，得分: {best_candidate['score']:.1f}, "
                   f"原因: {', '.join(best_candidate['reasons'])}")
        
        return best_candidate['record']
    
    def _normalize_text(self, text) -> str:
        """标准化文本"""
        if not text:
            return ''
        text_str = self._safe_str(text).lower()
        # 移除常见的无意义词
        text_str = text_str.replace('无', '').replace('null', '').replace('none', '')
        return text_str
    
    def _normalize_phone(self, phone) -> str:
        """标准化电话号码"""
        if not phone:
            return ''
        phone_str = self._safe_str(phone)
        # 移除非数字字符
        normalized = ''.join(c for c in phone_str if c.isdigit())
        # 移除常见前缀
        if normalized.startswith('86'):
            normalized = normalized[2:]
        return normalized
    
    def _text_similarity(self, text1: str, text2: str) -> float:
        """计算文本相似度"""
        if not text1 or not text2:
            return 0.0
        
        # 简单的字符相似度计算
        set1 = set(text1)
        set2 = set(text2)
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def _address_similarity(self, addr1: str, addr2: str) -> float:
        """计算地址相似度"""
        if not addr1 or not addr2:
            return 0.0
        
        # 提取关键地址信息
        addr1_keys = self._extract_address_keywords(addr1)
        addr2_keys = self._extract_address_keywords(addr2)
        
        if not addr1_keys or not addr2_keys:
            return 0.0
        
        # 计算关键词重叠度
        common_keys = addr1_keys.intersection(addr2_keys)
        total_keys = addr1_keys.union(addr2_keys)
        
        similarity = len(common_keys) / len(total_keys) if total_keys else 0.0
        
        # 如果有街道或路名匹配，增加权重
        street_keywords = {'路', '街', '巷', '弄', '号', '室', '层', '楼'}
        if any(key in street_keywords for key in common_keys):
            similarity += 0.2
        
        return min(1.0, similarity)
    
    def _extract_address_keywords(self, address: str) -> set:
        """提取地址关键词"""
        if not address:
            return set()
        
        # 移除常见的地址前缀
        address = address.replace('上海市', '').replace('市', '')
        address = address.replace('区', '').replace('县', '')
        
        # 分割地址，提取有意义的词汇
        keywords = set()
        
        # 按常见分隔符分割
        parts = address.replace('号', ' ').replace('室', ' ').replace('层', ' ').replace('楼', ' ')
        for part in parts.split():
            if len(part) >= 2:  # 只保留长度>=2的词
                keywords.add(part)
        
        return keywords
    
    def _calculate_completeness_score(self, record: Dict) -> float:
        """计算记录完整度分数"""
        important_fields = ['dwmc', 'fddbr', 'dwdz', 'lxdh', 'tyshxydm']
        filled_fields = 0
        
        for field in important_fields:
            value = record.get(field, '')
            safe_value = self._safe_str(value)
            if safe_value and safe_value.lower() not in ['无', 'null', 'none', '']:
                filled_fields += 1
        
        return (filled_fields / len(important_fields)) * 10
    
    def _match_by_unit_name(self, source_record: Dict, target_records: List[Dict]) -> MatchResult:
        """
        基于单位名称进行精确匹配
        
        Args:
            source_record: 源记录（安全排查系统）
            target_records: 目标记录列表（消防监督系统）
            
        Returns:
            MatchResult: 匹配结果
        """
        source_unit_name = self._normalize_unit_name(
            source_record.get('UNIT_NAME', '')
        )
        
        # 如果源记录没有单位名称，直接返回未匹配
        if not source_unit_name:
            return MatchResult(
                matched=False,
                match_type='none',
                confidence=0.0,
                source_record=source_record
            )
        
        for target_record in target_records:
            target_unit_name = self._normalize_unit_name(
                target_record.get('dwmc', '')
            )
            
            if target_unit_name and source_unit_name == target_unit_name:
                # 单位名称精确匹配（注意：现在只有信用代码才算精确匹配）
                return MatchResult(
                    matched=True,
                    match_type='unit_name',
                    confidence=1.0,
                    source_record=source_record,
                    target_record=target_record,
                    match_details={
                        'source_unit_name': source_unit_name,
                        'target_unit_name': target_unit_name
                    }
                )
        
        return MatchResult(
            matched=False,
            match_type='none',
            confidence=0.0,
            source_record=source_record
        )
    
    def _normalize_credit_code(self, credit_code) -> str:
        """
        标准化统一社会信用代码
        
        Args:
            credit_code: 原始信用代码（可能是字符串或数字）
            
        Returns:
            str: 标准化后的信用代码
        """
        if not credit_code:
            return ''
        
        # 确保是字符串类型
        credit_code_str = str(credit_code)
        
        # 移除空格和特殊字符
        normalized = ''.join(credit_code_str.split())
        normalized = normalized.replace('-', '').replace('_', '')
        
        # 转换为大写
        normalized = normalized.upper()
        
        # 验证长度（统一社会信用代码应为18位）
        if len(normalized) == 18:
            return normalized
        
        return ''
    
    def _normalize_unit_name(self, unit_name) -> str:
        """
        标准化单位名称（精确匹配要求完全一致）
        
        Args:
            unit_name: 原始单位名称（可能是字符串或其他类型）
            
        Returns:
            str: 标准化后的单位名称
        """
        if not unit_name:
            return ''
        
        # 确保是字符串类型
        unit_name_str = str(unit_name)
        
        # 移除首尾空格
        normalized = self._safe_str(unit_name_str)
        
        # 统一括号格式
        normalized = normalized.replace('（', '(').replace('）', ')')
        normalized = normalized.replace('[', '(').replace(']', ')')
        normalized = normalized.replace('【', '(').replace('】', ')')
        
        # 移除多余空格
        normalized = ' '.join(normalized.split())
        
        # 转换为小写进行比较（避免大小写差异）
        normalized = normalized.lower()
        
        return normalized
    
    def match_single_record_multi(self, source_record: Dict, target_records: List[Dict]) -> MultiMatchResult:
        """
        对单条记录进行一对多精确匹配
        
        Args:
            source_record: 源记录（安全排查系统）
            target_records: 目标记录列表（消防监督系统）
            
        Returns:
            MultiMatchResult: 一对多匹配结果
        """
        try:
            # 使用统一社会信用代码进行匹配
            credit_code_matches = self._match_by_credit_code_multi(source_record, target_records)
            
            if credit_code_matches:
                # 按检查时间排序（最新的在前）
                sorted_matches = self._sort_matches_by_date(credit_code_matches)
                
                # 选择主要匹配记录（数据质量最好的）
                primary_match = self._select_best_target_record(source_record, sorted_matches)
                
                # 构建匹配统计信息
                match_summary = {
                    'exact_matches': len(sorted_matches),
                    'fuzzy_matches': 0,
                    'total_matches': len(sorted_matches),
                    'match_method': 'credit_code',
                    'latest_inspection_date': self._get_latest_inspection_date(sorted_matches),
                    'earliest_inspection_date': self._get_earliest_inspection_date(sorted_matches),
                    'duplicate_handling': 'all_records_preserved'
                }
                
                # 为每个匹配记录添加匹配信息
                enriched_matches = []
                for i, match in enumerate(sorted_matches):
                    enriched_match = match.copy()
                    enriched_match['match_info'] = {
                        'match_type': 'exact_credit_code',
                        'similarity_score': 1.0,
                        'match_rank': i + 1,
                        'is_primary': match == primary_match,
                        'inspection_date': self._extract_inspection_date(match),
                        'data_completeness': self._calculate_completeness_score(match)
                    }
                    enriched_matches.append(enriched_match)
                
                logger.info(f"一对多精确匹配成功: {source_record.get('UNIT_NAME', 'Unknown')} -> {len(sorted_matches)} 条记录")
                
                return MultiMatchResult(
                    matched=True,
                    source_record=source_record,
                    matched_records=enriched_matches,
                    match_summary=match_summary,
                    primary_match=primary_match
                )
            
            # 无匹配结果
            logger.debug(f"一对多精确匹配失败: {source_record.get('UNIT_NAME', 'Unknown')}")
            return MultiMatchResult(
                matched=False,
                source_record=source_record,
                matched_records=[],
                match_summary={
                    'exact_matches': 0,
                    'fuzzy_matches': 0,
                    'total_matches': 0,
                    'match_method': 'none',
                    'reason': 'no_credit_code_match_found'
                }
            )
            
        except Exception as e:
            logger.error(f"一对多精确匹配过程出错: {str(e)}")
            return MultiMatchResult(
                matched=False,
                source_record=source_record,
                matched_records=[],
                match_summary={
                    'exact_matches': 0,
                    'fuzzy_matches': 0,
                    'total_matches': 0,
                    'match_method': 'error',
                    'error': str(e)
                }
            )
    
    def _match_by_credit_code_multi(self, source_record: Dict, target_records: List[Dict]) -> List[Dict]:
        """
        基于统一社会信用代码进行一对多匹配
        
        Args:
            source_record: 源记录（安全排查系统）
            target_records: 目标记录列表（消防监督系统）
            
        Returns:
            List[Dict]: 所有匹配的目标记录
        """
        source_credit_code = self._normalize_credit_code(
            source_record.get('CREDIT_CODE', '')
        )
        
        # 如果源记录没有信用代码，返回空列表
        if not source_credit_code:
            return []
        
        # 收集所有匹配的目标记录
        matched_targets = []
        for target_record in target_records:
            target_credit_code = self._normalize_credit_code(
                target_record.get('tyshxydm', '')
            )
            
            if target_credit_code and source_credit_code == target_credit_code:
                matched_targets.append(target_record)
        
        return matched_targets
    
    def _sort_matches_by_date(self, matches: List[Dict]) -> List[Dict]:
        """
        按检查日期对匹配记录排序（最新的在前）
        
        Args:
            matches: 匹配记录列表
            
        Returns:
            List[Dict]: 排序后的匹配记录
        """
        def get_sort_key(record):
            # 尝试多个可能的日期字段
            date_fields = ['jcsj', 'create_time', 'update_time', 'inspection_date', '_id']
            
            for field in date_fields:
                if field in record and record[field]:
                    if field == '_id':
                        # ObjectId包含创建时间信息，可以用于排序
                        return str(record[field])
                    return str(record[field])
            
            # 如果没有找到日期字段，使用记录ID作为排序依据
            return str(record.get('_id', ''))
        
        try:
            return sorted(matches, key=get_sort_key, reverse=True)
        except Exception as e:
            logger.warning(f"排序匹配记录时出错: {str(e)}, 返回原始顺序")
            return matches
    
    def _get_latest_inspection_date(self, matches: List[Dict]) -> Optional[str]:
        """获取最新的检查日期"""
        if not matches:
            return None
        
        # 假设matches已经按日期排序，第一个就是最新的
        latest_record = matches[0]
        return self._extract_inspection_date(latest_record)
    
    def _get_earliest_inspection_date(self, matches: List[Dict]) -> Optional[str]:
        """获取最早的检查日期"""
        if not matches:
            return None
        
        # 假设matches已经按日期排序，最后一个就是最早的
        earliest_record = matches[-1]
        return self._extract_inspection_date(earliest_record)
    
    def _extract_inspection_date(self, record: Dict) -> Optional[str]:
        """从记录中提取检查日期"""
        date_fields = ['jcsj', 'inspection_date', 'create_time', 'update_time']
        
        for field in date_fields:
            if field in record and record[field]:
                date_value = record[field]
                if isinstance(date_value, str):
                    return date_value
                elif hasattr(date_value, 'isoformat'):
                    return date_value.isoformat()
                else:
                    return str(date_value)
        
        return None
    
    def batch_match(self, source_records: List[Dict], target_records: List[Dict]) -> List[MatchResult]:
        """
        批量精确匹配
        
        Args:
            source_records: 源记录列表
            target_records: 目标记录列表
            
        Returns:
            List[MatchResult]: 匹配结果列表
        """
        results = []
        total_count = len(source_records)
        
        logger.info(f"开始批量精确匹配，共 {total_count} 条记录")
        
        for i, source_record in enumerate(source_records):
            try:
                result = self.match_single_record(source_record, target_records)
                results.append(result)
                
                # 每处理100条记录输出进度
                if (i + 1) % 100 == 0:
                    logger.info(f"精确匹配进度: {i + 1}/{total_count}")
                    
            except Exception as e:
                logger.error(f"处理第 {i + 1} 条记录时出错: {str(e)}")
                results.append(MatchResult(
                    matched=False,
                    match_type='error',
                    confidence=0.0,
                    source_record=source_record,
                    match_details={'error': str(e)}
                ))
        
        # 统计匹配结果
        matched_count = sum(1 for r in results if r.matched)
        logger.info(f"精确匹配完成，匹配成功: {matched_count}/{total_count}")
        
        return results