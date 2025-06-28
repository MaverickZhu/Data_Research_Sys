"""
模糊匹配算法模块
基于多维度相似度计算的智能匹配算法
"""

import logging
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import jieba
import pypinyin
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from .similarity_scorer import SimilarityCalculator
from .match_result import MatchResult, MultiMatchResult

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


class SimilarityCalculator:
    """相似度计算器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.chinese_config = config.get('string_similarity', {}).get('chinese_processing', {})
        
    def calculate_string_similarity(self, str1: str, str2: str) -> float:
        """
        计算字符串相似度
        
        Args:
            str1: 字符串1
            str2: 字符串2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not str1 or not str2:
            return 0.0
        
        # 预处理字符串
        processed_str1 = self._preprocess_string(str1)
        processed_str2 = self._preprocess_string(str2)
        
        if not processed_str1 or not processed_str2:
            return 0.0
        
        # 多种相似度算法加权计算
        similarities = {}
        
        # 1. Levenshtein距离相似度 (使用更适合乱序的token_set_ratio)
        similarities['levenshtein'] = fuzz.token_set_ratio(processed_str1, processed_str2) / 100.0
        
        # 2. Jaro-Winkler相似度 (WRatio能处理部分乱序和不同长度)
        similarities['jaro_winkler'] = fuzz.WRatio(processed_str1, processed_str2) / 100.0
        
        # 3. 移除余弦相似度，因为它在这种场景下性能极差
        # similarities['cosine'] = self._calculate_cosine_similarity(processed_str1, processed_str2)
        
        # 4. 中文处理相似度
        if self.chinese_config.get('enable_pinyin', False):
            similarities['pinyin'] = self._calculate_pinyin_similarity(str1, str2)
        
        # 加权平均计算最终相似度 - 权重已调整
        algorithms_config = [
            {'name': 'levenshtein', 'weight': 0.5},
            {'name': 'jaro_winkler', 'weight': 0.5}
        ]
        final_similarity = 0.0
        total_weight = 0.0
        
        for algo_config in algorithms_config:
            algo_name = algo_config['name']
            weight = algo_config['weight']
            
            if algo_name in similarities:
                final_similarity += similarities[algo_name] * weight
                total_weight += weight
        
        if total_weight > 0:
            final_similarity /= total_weight
        
        return min(1.0, max(0.0, final_similarity))
    
    def calculate_numeric_similarity(self, num1: Optional[float], num2: Optional[float], 
                                   field_config: Dict) -> float:
        """
        计算数值相似度
        
        Args:
            num1: 数值1
            num2: 数值2
            field_config: 字段配置
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if num1 is None or num2 is None:
            return 0.0
        
        try:
            num1 = float(num1)
            num2 = float(num2)
        except (ValueError, TypeError):
            return 0.0
        
        if num1 == 0 and num2 == 0:
            return 1.0
        
        if num1 == 0 or num2 == 0:
            return 0.0
        
        # 计算百分比差异
        max_val = max(abs(num1), abs(num2))
        diff = abs(num1 - num2)
        percentage_diff = diff / max_val
        
        # 获取容差配置
        tolerance = field_config.get('tolerance', 0.1)
        
        if percentage_diff <= tolerance:
            return 1.0 - (percentage_diff / tolerance) * 0.2  # 最高0.8分
        else:
            return max(0.0, 1.0 - percentage_diff)
    
    def calculate_phone_similarity(self, phone1: str, phone2: str) -> float:
        """
        计算电话号码相似度
        
        Args:
            phone1: 电话号码1
            phone2: 电话号码2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not phone1 or not phone2:
            return 0.0
        
        # 标准化电话号码
        normalized_phone1 = self._normalize_phone(phone1)
        normalized_phone2 = self._normalize_phone(phone2)
        
        if not normalized_phone1 or not normalized_phone2:
            return 0.0
        
        if normalized_phone1 == normalized_phone2:
            return 1.0
        
        # 检查是否为同一号码的不同格式
        digits1 = re.sub(r'\D', '', normalized_phone1)
        digits2 = re.sub(r'\D', '', normalized_phone2)
        
        if digits1 == digits2:
            return 1.0
        
        # 检查后缀匹配（手机号后8位或座机号后7位）
        if len(digits1) >= 8 and len(digits2) >= 8:
            if digits1[-8:] == digits2[-8:]:
                return 0.8
        
        if len(digits1) >= 7 and len(digits2) >= 7:
            if digits1[-7:] == digits2[-7:]:
                return 0.7
        
        return 0.0
    
    def _preprocess_string(self, text: str) -> str:
        """
        增强的文本预处理函数
        - 行政区划同义词归一化
        - 全角转半角
        - 转小写
        - 移除公司常见后缀和噪音词
        - 移除标点符号
        - 标准化空格
        """
        if not text:
            return ''

        # 0. 行政区划同义词归一化
        ADMIN_EQUIV = {
            "崇明县": "崇明",
            "崇明区": "崇明",
            "浦东新区": "浦东",
            "浦东县": "浦东",
            "浦东区": "浦东",
            "嘉定县": "嘉定",
            "嘉定区": "嘉定",
            # 可扩展更多
        }
        for k, v in ADMIN_EQUIV.items():
            text = text.replace(k, v)

        # 1. 全角转半角
        full_width_chars = "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ（）－ "
        half_width_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz()- "
        translator = str.maketrans(full_width_chars, half_width_chars)
        text = text.translate(translator)

        # 2. 转为小写
        text = text.lower()

        # 3. 移除常见公司后缀和噪音词
        # 优化：使用更精确的替换逻辑，避免过度删除
        
        # 首先移除固定的、通常出现在末尾的后缀
        suffixes = ['有限公司', '有限责任公司', '股份有限公司', '股份合作公司', '分公司', '总公司']
        for suffix in suffixes:
            if text.endswith(suffix):
                text = text[:-len(suffix)]

        # 然后使用正则表达式移除作为独立单词出现的噪音词
        # 使用\b单词边界，避免从"城市超市"中错误地移除"市"
        noise_pattern_words = [
            '办事处', '管理处', '管理部', '项目部', '集团', '中心', 
            '商行', '商社', '商场', '酒店', '宾馆', '股份',
            '上海市', '上海', '中国'
        ]
        # 对单字符噪音词进行特殊处理，确保它们不会破坏专有名词
        single_char_noise = ['市', '区', '县', '镇', '乡', '村']

        for word in noise_pattern_words:
            text = text.replace(word, '') # 这些词通常是连贯的，直接替换影响不大

        # 对单字符词，使用更严格的边界匹配
        for char in single_char_noise:
            # 仅当它被非字母数字包围时才移除，或者在开头/结尾
             text = re.sub(rf'(?<![\u4e00-\u9fa5a-zA-Z0-9]){re.escape(char)}(?![\u4e00-\u9fa5a-zA-Z0-9])', '', text)

        # 移除所有括号和其中的内容
        text = re.sub(r'[\(（].*?[\)）]', '', text)
        
        # 4. 移除剩余的特殊字符和空格
        text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', text)
        
        return text
    
    def calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """
        计算地址相似度
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not addr1 or not addr2:
            return 0.0
        
        # 标准化地址
        norm_addr1 = self._preprocess_string(addr1)
        norm_addr2 = self._preprocess_string(addr2)
        
        if not norm_addr1 or not norm_addr2:
            return 0.0
            
        if norm_addr1 == norm_addr2:
            return 1.0
        
        # 使用token_set_ratio，它对乱序和部分匹配有很好的效果
        return fuzz.token_set_ratio(norm_addr1, norm_addr2) / 100.0
    
    def _calculate_pinyin_similarity(self, str1: str, str2: str) -> float:
        """计算拼音相似度"""
        try:
            pinyin1 = ''.join([item[0] for item in pypinyin.pinyin(str1, style=pypinyin.NORMAL)])
            pinyin2 = ''.join([item[0] for item in pypinyin.pinyin(str2, style=pypinyin.NORMAL)])
            
            return fuzz.token_set_ratio(pinyin1, pinyin2) / 100.0
            
        except Exception as e:
            logger.warning(f"计算拼音相似度时出错: {str(e)}")
            return 0.0
    
    def _normalize_phone(self, phone: str) -> str:
        """标准化电话号码"""
        if not phone:
            return ''
        
        # 移除所有非数字字符，保留+号
        normalized = re.sub(r'[^\d+]', '', phone)
        
        # 处理国际格式
        if normalized.startswith('+86'):
            normalized = normalized[3:]
        elif normalized.startswith('86') and len(normalized) > 11:
            normalized = normalized[2:]
        
        return normalized


class FuzzyMatcher:
    """模糊匹配器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.fuzzy_config = config.get('fuzzy_match', {})
        self.fields_config = {
            'unit_name': {
                'source_field': 'UNIT_NAME',  # 安全排查系统字段
                'target_field': 'dwmc',       # 消防监督系统字段
                'match_type': 'string',
                'weight': 0.4
            },
            'address': {
                'source_field': 'ADDRESS',    # 安全排查系统字段
                'target_field': 'dwdz',       # 消防监督系统字段
                'match_type': 'address',
                'weight': 0.3
            },
            'legal_person': {
                'source_field': 'LEGAL_REPRESENTATIVE',  # 安全排查系统字段
                'target_field': 'fddbr',                 # 消防监督系统字段
                'match_type': 'string',
                'weight': 0.15
            },
            'security_person': {
                'source_field': 'CONTACT_PERSON',  # 安全排查系统字段
                'target_field': 'xfaqglr',         # 消防监督系统字段
                'match_type': 'string',
                'weight': 0.15
            }
        }
        self.threshold = self.fuzzy_config.get('similarity_threshold', 0.75)
        self.similarity_calculator = SimilarityCalculator(config)
    
    def _safe_str(self, value, default: str = '') -> str:
        """安全地将任何类型转换为字符串并去除空白"""
        if value is None:
            return default
        try:
            return str(value).strip()
        except Exception:
            return default
        
    def match_single_record(self, source_record: Dict, target_records: List[Dict]) -> FuzzyMatchResult:
        """
        对单条记录进行模糊匹配
        
        Args:
            source_record: 源记录
            target_records: 目标记录列表
            
        Returns:
            FuzzyMatchResult: 模糊匹配结果
        """
        # 收集所有候选匹配（分数超过阈值的记录）
        candidates = []
        
        for target_record in target_records:
            score, field_similarities = self._calculate_record_similarity(
                source_record, target_record
            )
            
            if score >= self.threshold:
                candidates.append({
                    'record': target_record,
                    'score': score,
                    'field_similarities': field_similarities
                })
        
        if not candidates:
            # 没有匹配的记录
            return FuzzyMatchResult(
                matched=False,
                similarity_score=0.0,
                source_record=source_record,
                match_details={
                    'threshold': self.threshold,
                    'candidates_count': len(target_records),
                    'qualified_candidates': 0
                }
            )
        
        # 如果只有一个候选，直接返回
        if len(candidates) == 1:
            best_candidate = candidates[0]
            logger.debug(f"模糊匹配成功: {source_record.get('UNIT_NAME', 'Unknown')} "
                        f"-> {best_candidate['record'].get('dwmc', 'Unknown')} "
                        f"(score: {best_candidate['score']:.3f})")
            
            return FuzzyMatchResult(
                matched=True,
                similarity_score=best_candidate['score'],
                source_record=source_record,
                target_record=best_candidate['record'],
                field_similarities=best_candidate['field_similarities'],
                match_details={
                    'threshold': self.threshold,
                    'candidates_count': len(target_records),
                    'qualified_candidates': 1
                }
            )
        
        # 如果有多个候选，进行智能选择
        best_candidate = self._select_best_fuzzy_match(source_record, candidates)
        
        logger.info(f"模糊匹配多候选选择: {source_record.get('UNIT_NAME', 'Unknown')} "
                   f"-> {best_candidate['record'].get('dwmc', 'Unknown')} "
                   f"(score: {best_candidate['score']:.3f}, 候选数: {len(candidates)})")
        
        return FuzzyMatchResult(
            matched=True,
            similarity_score=best_candidate['score'],
            source_record=source_record,
            target_record=best_candidate['record'],
            field_similarities=best_candidate['field_similarities'],
            match_details={
                'threshold': self.threshold,
                'candidates_count': len(target_records),
                'qualified_candidates': len(candidates),
                'selection_method': 'intelligent_selection'
            }
        )
    
    def _select_best_fuzzy_match(self, source_record: Dict, candidates: List[Dict]) -> Dict:
        """
        从多个模糊匹配候选中选择最佳匹配
        
        策略：
        1. 优先选择相似度分数最高的
        2. 如果分数接近，考虑数据完整度
        3. 如果仍然接近，考虑特定字段匹配质量
        
        Args:
            source_record: 源记录
            candidates: 候选记录列表，每个包含record、score、field_similarities
            
        Returns:
            Dict: 最佳候选记录
        """
        if len(candidates) == 1:
            return candidates[0]
        
        # 按分数排序
        candidates.sort(key=lambda x: x['score'], reverse=True)
        
        # 检查最高分和次高分的差距
        top_score = candidates[0]['score']
        score_threshold = 0.05  # 分数差距小于0.05认为接近
        
        # 找出分数接近的候选
        close_candidates = [c for c in candidates if top_score - c['score'] <= score_threshold]
        
        if len(close_candidates) == 1:
            # 只有一个最高分，直接返回
            return close_candidates[0]
        
        # 多个分数接近的候选，进行二次筛选
        logger.info(f"发现 {len(close_candidates)} 个分数接近的候选，进行二次筛选")
        
        scored_candidates = []
        
        for candidate in close_candidates:
            target_record = candidate['record']
            field_similarities = candidate['field_similarities']
            secondary_score = 0
            reasons = []
            
            # 1. 数据完整度 (30分)
            completeness = self._calculate_target_completeness(target_record)
            completeness_score = completeness * 30
            secondary_score += completeness_score
            if completeness_score > 20:
                reasons.append(f"数据完整度高({completeness:.2f})")
            
            # 2. 单位名称匹配质量 (40分)
            unit_name_sim = field_similarities.get('unit_name', {}).get('similarity', 0)
            unit_name_score = unit_name_sim * 40
            secondary_score += unit_name_score
            if unit_name_score > 30:
                reasons.append(f"单位名称高度匹配({unit_name_sim:.2f})")
            
            # 3. 地址匹配质量 (20分)
            address_sim = field_similarities.get('address', {}).get('similarity', 0)
            address_score = address_sim * 20
            secondary_score += address_score
            if address_score > 15:
                reasons.append(f"地址高度匹配({address_sim:.2f})")
            
            # 4. 法定代表人匹配 (10分)
            legal_person_sim = field_similarities.get('legal_person', {}).get('similarity', 0)
            legal_score = legal_person_sim * 10
            secondary_score += legal_score
            if legal_score > 7:
                reasons.append(f"法定代表人匹配({legal_person_sim:.2f})")
            
            scored_candidates.append({
                'candidate': candidate,
                'secondary_score': secondary_score,
                'reasons': reasons
            })
        
        # 按二次分数排序
        scored_candidates.sort(key=lambda x: x['secondary_score'], reverse=True)
        best = scored_candidates[0]
        
        logger.info(f"二次筛选完成，选择原因: {', '.join(best['reasons'])}")
        
        return best['candidate']
    
    def _calculate_target_completeness(self, record: Dict) -> float:
        """
        计算目标记录的数据完整度
        
        Args:
            record: 目标记录
            
        Returns:
            float: 完整度分数 (0-1)
        """
        important_fields = ['dwmc', 'dwdz', 'fddbr', 'tyshxydm', 'frlxdh']
        total_fields = len(important_fields)
        complete_fields = 0
        
        for field in important_fields:
            value = record.get(field, '')
            safe_value = self._safe_str(value)
            if safe_value and safe_value != '无':
                complete_fields += 1
        
        return complete_fields / total_fields
    
    def _calculate_record_similarity(self, source_record: Dict, target_record: Dict) -> Tuple[float, Dict]:
        """
        计算两条记录的相似度
        """
        field_similarities = {}
        weighted_score = 0.0
        total_weight = 0.0
        
        # 动态权重分配准备
        base_weights = {
            'unit_name': 0.4, 'address': 0.4,
            'legal_person': 0.1, 'security_person': 0.1
        }
        available_fields = [
            name for name, cfg in self.fields_config.items()
            if name == 'unit_name' or (source_record.get(cfg['source_field']) and target_record.get(cfg.get('target_field')))
        ]
        
        total_base_weight = sum(base_weights[k] for k in available_fields)
        weights = {k: base_weights[k] / total_base_weight for k in available_fields} if total_base_weight > 0 else {}
        
        for field_name, field_config in self.fields_config.items():
            if field_name not in available_fields:
                continue

            source_field = field_config['source_field']
            target_field = field_config['target_field']
            match_type = field_config['match_type']

            source_value_orig = source_record.get(source_field)
            target_value_orig = target_record.get(target_field)

            similarity = 0.0
            if match_type in ['string', 'address', 'phone']:
                source_str = str(source_value_orig) if source_value_orig is not None else ''
                target_str = str(target_value_orig) if target_value_orig is not None else ''
                if match_type == 'string':
                    similarity = self.similarity_calculator.calculate_string_similarity(source_str, target_str)
                elif match_type == 'address':
                    similarity = self.similarity_calculator.calculate_address_similarity(source_str, target_str)
                elif match_type == 'phone':
                    similarity = self.similarity_calculator.calculate_phone_similarity(source_str, target_str)
            elif match_type == 'numeric':
                try:
                    numeric_source = float(source_value_orig)
                    numeric_target = float(target_value_orig)
                    similarity = self.similarity_calculator.calculate_numeric_similarity(numeric_source, numeric_target, field_config)
                except (ValueError, TypeError, AttributeError):
                    similarity = 0.0
            
            field_similarities[field_name] = {
                'similarity': similarity,
                'source_value': source_value_orig,
                'target_value': target_value_orig,
                'weight': weights.get(field_name, 0.0)
            }
            weighted_score += similarity * weights.get(field_name, 0.0)
            total_weight += weights.get(field_name, 0.0)
            
        final_score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        # 动态阈值
        if len(available_fields) == 1:
            dynamic_threshold = 0.95
        elif len(available_fields) == 2:
            dynamic_threshold = 0.85
        else:
            dynamic_threshold = 0.75
        # 地址相似度二次验证与分数增强（如有）
        unit_name_similarity = field_similarities.get('unit_name', {}).get('similarity', 0.0)
        if unit_name_similarity >= 0.8 and 'address' in available_fields:
            address_config = self.fields_config.get('address')
            if address_config:
                source_addr = source_record.get(address_config['source_field'], '')
                target_addr = target_record.get(address_config.get('target_field'), '')
                if source_addr and target_addr:
                    address_similarity = self.similarity_calculator.calculate_address_similarity(source_addr, target_addr)
                    if address_similarity >= 0.7:
                        boost = 0.15
                        original_score = final_score
                        final_score = min(1.0, final_score + boost)
                        field_similarities['address_boost'] = {
                            'similarity': address_similarity,
                            'boost_applied': boost,
                            'original_score': original_score,
                            'final_score': final_score
                        }
        # 返回分数、字段详情（含权重）、动态阈值
        field_similarities['__meta__'] = {
            'dynamic_threshold': dynamic_threshold,
            'used_weights': weights,
            'available_fields': available_fields
        }
        return final_score, field_similarities
    
    def batch_match(self, source_records: List[Dict], target_records: List[Dict]) -> List[FuzzyMatchResult]:
        """
        批量模糊匹配
        
        Args:
            source_records: 源记录列表
            target_records: 目标记录列表
            
        Returns:
            List[FuzzyMatchResult]: 模糊匹配结果列表
        """
        results = []
        total_count = len(source_records)
        
        logger.info(f"开始批量模糊匹配，共 {total_count} 条记录，阈值: {self.threshold}")
        
        for i, source_record in enumerate(source_records):
            try:
                result = self.match_single_record(source_record, target_records)
                results.append(result)
                
                # 每处理50条记录输出进度
                if (i + 1) % 50 == 0:
                    matched_so_far = sum(1 for r in results if r.matched)
                    logger.info(f"模糊匹配进度: {i + 1}/{total_count}, "
                              f"匹配成功: {matched_so_far}")
                    
            except Exception as e:
                logger.error(f"处理第 {i + 1} 条记录时出错: {str(e)}")
                results.append(FuzzyMatchResult(
                    matched=False,
                    similarity_score=0.0,
                    source_record=source_record,
                    match_details={'error': str(e)}
                ))
        
        # 统计最终结果
        matched_count = sum(1 for r in results if r.matched)
        avg_score = np.mean([r.similarity_score for r in results if r.matched])
        
        logger.info(f"模糊匹配完成，匹配成功: {matched_count}/{total_count}, "
                   f"平均相似度: {avg_score:.3f}")
        
        return results

    def match_single_record_multi(self, source_record: Dict, target_records: List[Dict]) -> 'MultiMatchResult':
        """
        对单条记录进行一对多模糊匹配
        
        Args:
            source_record: 源记录（安全排查系统）
            target_records: 目标记录列表（消防监督系统）
            
        Returns:
            MultiMatchResult: 一对多匹配结果
        """
        try:
            # 收集所有超过阈值的候选匹配
            candidates = []
            
            for target_record in target_records:
                score, field_similarities = self._calculate_record_similarity(
                    source_record, target_record
                )
                
                if score >= self.threshold:
                    candidates.append({
                        'record': target_record,
                        'score': score,
                        'field_similarities': field_similarities,
                        'match_type': 'fuzzy'
                    })
            
            if not candidates:
                # 没有匹配的记录
                logger.debug(f"一对多模糊匹配失败: {source_record.get('UNIT_NAME', 'Unknown')}")
                return MultiMatchResult(
                    matched=False,
                    source_record=source_record,
                    matched_records=[],
                    match_summary={
                        'exact_matches': 0,
                        'fuzzy_matches': 0,
                        'total_matches': 0,
                        'match_method': 'fuzzy',
                        'threshold': self.threshold,
                        'candidates_count': len(target_records),
                        'qualified_candidates': 0,
                        'reason': 'no_candidates_above_threshold'
                    }
                )
            
            # 按相似度分数排序（最高分在前）
            candidates.sort(key=lambda x: x['score'], reverse=True)
            
            # 选择主要匹配记录（分数最高的）
            primary_candidate = candidates[0]
            primary_match = primary_candidate['record']
            
            # 构建匹配统计信息
            match_summary = {
                'exact_matches': 0,
                'fuzzy_matches': len(candidates),
                'total_matches': len(candidates),
                'match_method': 'fuzzy',
                'threshold': self.threshold,
                'candidates_count': len(target_records),
                'qualified_candidates': len(candidates),
                'highest_score': candidates[0]['score'],
                'lowest_score': candidates[-1]['score'],
                'average_score': sum(c['score'] for c in candidates) / len(candidates),
                'latest_inspection_date': self._get_latest_inspection_date([c['record'] for c in candidates]),
                'duplicate_handling': 'all_qualified_candidates_preserved'
            }
            
            # 为每个匹配记录添加匹配信息
            enriched_matches = []
            for i, candidate in enumerate(candidates):
                enriched_match = candidate['record'].copy()
                enriched_match['match_info'] = {
                    'match_type': 'fuzzy',
                    'similarity_score': candidate['score'],
                    'match_rank': i + 1,
                    'is_primary': candidate == primary_candidate,
                    'field_similarities': candidate['field_similarities'],
                    'inspection_date': self._extract_inspection_date(candidate['record']),
                    'data_completeness': self._calculate_target_completeness(candidate['record'])
                }
                enriched_matches.append(enriched_match)
            
            logger.info(f"一对多模糊匹配成功: {source_record.get('UNIT_NAME', 'Unknown')} -> {len(candidates)} 条记录 (最高分: {candidates[0]['score']:.3f})")
            
            return MultiMatchResult(
                matched=True,
                source_record=source_record,
                matched_records=enriched_matches,
                match_summary=match_summary,
                primary_match=primary_match
            )
            
        except Exception as e:
            logger.error(f"一对多模糊匹配过程出错: {str(e)}")
            return MultiMatchResult(
                matched=False,
                source_record=source_record,
                matched_records=[],
                match_summary={
                    'exact_matches': 0,
                    'fuzzy_matches': 0,
                    'total_matches': 0,
                    'match_method': 'fuzzy_error',
                    'error': str(e)
                }
            )
    
    def _get_latest_inspection_date(self, matches: List[Dict]) -> Optional[str]:
        """获取最新的检查日期"""
        if not matches:
            return None
        
        latest_date = None
        for record in matches:
            date = self._extract_inspection_date(record)
            if date and (not latest_date or date > latest_date):
                latest_date = date
        
        return latest_date
    
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