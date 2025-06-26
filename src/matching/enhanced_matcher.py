#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强匹配算法
实现多层次匹配策略，大幅提升匹配率
"""

import re
import jieba
import difflib
from fuzzywuzzy import fuzz, process
from difflib import SequenceMatcher
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class EnhancedMatcher:
    """增强匹配器"""
    
    def __init__(self):
        """初始化匹配器"""
        self.company_suffixes = [
            "有限公司", "股份有限公司", "有限责任公司", "集团有限公司",
            "科技有限公司", "贸易有限公司", "实业有限公司", "发展有限公司",
            "分公司", "经营部", "工作室", "合作社", "个体工商户"
        ]
        
        self.stop_words = {
            "上海", "上海市", "市", "区", "县", "镇", "街道", "路", "号",
            "的", "和", "与", "及", "或", "等", "中", "内", "外", "前", "后"
        }
        
        # 地区映射
        self.district_mapping = {
            "浦东新区": ["浦东", "新区"],
            "黄浦区": ["黄浦"],
            "徐汇区": ["徐汇"],
            "长宁区": ["长宁"],
            "静安区": ["静安"],
            "普陀区": ["普陀"],
            "虹口区": ["虹口"],
            "杨浦区": ["杨浦"],
            "闵行区": ["闵行"],
            "宝山区": ["宝山"],
            "嘉定区": ["嘉定"],
            "金山区": ["金山"],
            "松江区": ["松江"],
            "青浦区": ["青浦"],
            "奉贤区": ["奉贤"],
            "崇明区": ["崇明", "崇明县"]
        }
        
        logger.info("增强匹配器初始化完成")
    
    def normalize_company_name(self, name: str) -> str:
        """标准化公司名称"""
        if not name:
            return ""
        
        # 去除首尾空格
        name = name.strip()
        
        # 统一括号
        name = re.sub(r'[（(]', '(', name)
        name = re.sub(r'[）)]', ')', name)
        
        # 去除特殊字符
        name = re.sub(r'[^\w\u4e00-\u9fff()（）]', '', name)
        
        # 标准化常见词汇
        replacements = {
            "（有限）": "",
            "(有限)": "",
            "（个体工商户）": "",
            "(个体工商户)": "",
            "上海市": "上海",
        }
        
        for old, new in replacements.items():
            name = name.replace(old, new)
        
        return name
    
    def extract_core_name(self, name: str) -> str:
        """提取核心名称（去除公司后缀和地区前缀）"""
        normalized = self.normalize_company_name(name)
        
        # 去除公司后缀
        for suffix in sorted(self.company_suffixes, key=len, reverse=True):
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
                break
        
        # 去除地区前缀
        if normalized.startswith("上海"):
            normalized = normalized[2:]
        if normalized.startswith("上海市"):
            normalized = normalized[3:]
        
        # 去除区县信息
        for district in self.district_mapping.keys():
            if normalized.startswith(district):
                normalized = normalized[len(district):]
                break
        
        return normalized.strip()
    
    def calculate_similarity_score(self, name1: str, name2: str) -> Dict[str, float]:
        """计算多种相似度分数"""
        if not name1 or not name2:
            return {"overall": 0.0}
        
        # 标准化名称
        norm1 = self.normalize_company_name(name1)
        norm2 = self.normalize_company_name(name2)
        
        # 提取核心名称
        core1 = self.extract_core_name(name1)
        core2 = self.extract_core_name(name2)
        
        scores = {}
        
        # 1. 精确匹配
        scores["exact"] = 1.0 if norm1 == norm2 else 0.0
        scores["core_exact"] = 1.0 if core1 == core2 else 0.0
        
        # 2. 序列匹配器
        scores["sequence"] = SequenceMatcher(None, norm1, norm2).ratio()
        scores["core_sequence"] = SequenceMatcher(None, core1, core2).ratio()
        
        # 3. FuzzyWuzzy算法
        scores["fuzz_ratio"] = fuzz.ratio(norm1, norm2) / 100.0
        scores["fuzz_partial"] = fuzz.partial_ratio(norm1, norm2) / 100.0
        scores["fuzz_token_sort"] = fuzz.token_sort_ratio(norm1, norm2) / 100.0
        scores["fuzz_token_set"] = fuzz.token_set_ratio(norm1, norm2) / 100.0
        
        # 4. 核心名称FuzzyWuzzy
        scores["core_fuzz_ratio"] = fuzz.ratio(core1, core2) / 100.0
        scores["core_fuzz_token"] = fuzz.token_sort_ratio(core1, core2) / 100.0
        
        # 5. 分词匹配
        words1 = set(jieba.lcut(core1)) - self.stop_words
        words2 = set(jieba.lcut(core2)) - self.stop_words
        
        if words1 and words2:
            intersection = len(words1 & words2)
            union = len(words1 | words2)
            scores["jaccard"] = intersection / union if union > 0 else 0.0
            scores["word_overlap"] = intersection / min(len(words1), len(words2))
        else:
            scores["jaccard"] = 0.0
            scores["word_overlap"] = 0.0
        
        # 6. 计算综合分数
        weights = {
            "exact": 0.25,
            "core_exact": 0.20,
            "fuzz_token_set": 0.20,
            "core_fuzz_token": 0.15,
            "sequence": 0.10,
            "jaccard": 0.05,
            "word_overlap": 0.05
        }
        
        overall_score = sum(scores.get(key, 0) * weight for key, weight in weights.items())
        scores["overall"] = min(overall_score, 1.0)
        
        return scores
    
    def match_by_address(self, addr1: str, addr2: str) -> float:
        """基于地址的匹配"""
        if not addr1 or not addr2:
            return 0.0
        
        # 标准化地址
        addr1 = self.normalize_company_name(addr1)
        addr2 = self.normalize_company_name(addr2)
        
        # 提取关键地址信息
        def extract_address_keywords(addr):
            keywords = set()
            # 提取区县
            for district in self.district_mapping.keys():
                if district in addr:
                    keywords.add(district)
            
            # 提取街道、路名
            street_pattern = r'([^区县市]*?(?:街道|路|大道|街|弄|巷|里))'
            streets = re.findall(street_pattern, addr)
            keywords.update(streets)
            
            return keywords
        
        keywords1 = extract_address_keywords(addr1)
        keywords2 = extract_address_keywords(addr2)
        
        if not keywords1 or not keywords2:
            return fuzz.ratio(addr1, addr2) / 100.0
        
        # 计算关键词重叠度
        intersection = len(keywords1 & keywords2)
        union = len(keywords1 | keywords2)
        
        return intersection / union if union > 0 else 0.0
    
    def enhanced_match(self, unit1: Dict, unit2: Dict) -> Dict:
        """增强匹配算法"""
        # 获取单位名称
        name1 = unit1.get("dwmc") or unit1.get("UNIT_NAME") or ""
        name2 = unit2.get("dwmc") or unit2.get("UNIT_NAME") or ""
        
        # 获取地址信息
        addr1 = unit1.get("dwdz") or unit1.get("ADDRESS") or ""
        addr2 = unit2.get("dwdz") or unit2.get("ADDRESS") or ""
        
        # 计算名称相似度
        name_scores = self.calculate_similarity_score(name1, name2)
        
        # 计算地址相似度
        address_score = self.match_by_address(addr1, addr2)
        
        # 综合评分
        final_score = name_scores["overall"] * 0.8 + address_score * 0.2
        
        # 确定匹配类型
        match_type = self.determine_match_type(name_scores, address_score)
        
        return {
            "similarity_score": final_score,
            "name_similarity": name_scores["overall"],
            "address_similarity": address_score,
            "match_type": match_type,
            "detailed_scores": name_scores,
            "primary_unit_name": name1,
            "matched_unit_name": name2,
            "primary_address": addr1,
            "matched_address": addr2
        }
    
    def determine_match_type(self, name_scores: Dict[str, float], address_score: float) -> str:
        """确定匹配类型"""
        if name_scores.get("exact", 0) == 1.0:
            return "exact"
        elif name_scores.get("core_exact", 0) == 1.0:
            return "core_exact"
        elif name_scores.get("overall", 0) >= 0.9:
            return "high_similarity"
        elif name_scores.get("overall", 0) >= 0.7:
            return "medium_similarity"
        elif name_scores.get("overall", 0) >= 0.5:
            return "low_similarity"
        elif address_score >= 0.8:
            return "address_match"
        else:
            return "no_match"
    
    def batch_match(self, supervision_units: List[Dict], inspection_units: List[Dict], 
                   threshold: float = 0.5) -> List[Dict]:
        """批量匹配"""
        matches = []
        
        logger.info(f"开始批量匹配: {len(supervision_units)} vs {len(inspection_units)}")
        
        for i, sup_unit in enumerate(supervision_units):
            if i % 100 == 0:
                logger.info(f"处理进度: {i}/{len(supervision_units)}")
            
            best_match = None
            best_score = 0.0
            
            for ins_unit in inspection_units:
                match_result = self.enhanced_match(sup_unit, ins_unit)
                
                if match_result["similarity_score"] > best_score and match_result["similarity_score"] >= threshold:
                    best_score = match_result["similarity_score"]
                    best_match = match_result
                    best_match["supervision_unit"] = sup_unit
                    best_match["inspection_unit"] = ins_unit
            
            if best_match:
                matches.append(best_match)
        
        logger.info(f"批量匹配完成，找到 {len(matches)} 个匹配")
        return matches
    
    def find_potential_matches(self, supervision_units: List[Dict], inspection_units: List[Dict],
                             min_threshold: float = 0.3, max_results: int = 1000) -> List[Dict]:
        """寻找潜在匹配"""
        potential_matches = []
        
        logger.info(f"寻找潜在匹配: 阈值 {min_threshold}, 最大结果 {max_results}")
        
        for sup_unit in supervision_units[:500]:  # 限制样本大小以提高性能
            for ins_unit in inspection_units[:500]:
                match_result = self.enhanced_match(sup_unit, ins_unit)
                
                if match_result["similarity_score"] >= min_threshold:
                    match_result["supervision_unit"] = sup_unit
                    match_result["inspection_unit"] = ins_unit
                    potential_matches.append(match_result)
        
        # 按相似度排序
        potential_matches.sort(key=lambda x: x["similarity_score"], reverse=True)
        
        return potential_matches[:max_results] 