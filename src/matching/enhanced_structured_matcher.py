#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版结构化单位名称匹配器
集成到消防单位建筑数据关联系统中
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


@dataclass
class NameStructure:
    """单位名称结构"""
    region: str = ""          # 行政区域：如"上海"、"上海闵行"、"闵行"
    core_name: str = ""       # 核心名称：如"恒琳"、"恒虹" 
    business_type: str = ""   # 业务类型：如"工贸"、"贸易"、"电器"
    company_type: str = ""    # 公司性质：如"有限公司"、"股份公司"
    original: str = ""        # 原始名称
    confidence: float = 0.0   # 分解置信度


@dataclass
class EnhancedStructuredMatchResult:
    """增强版结构化匹配结果"""
    matched: bool
    overall_similarity: float
    region_similarity: float
    core_name_similarity: float
    business_type_similarity: float
    company_type_similarity: float
    address_similarity: float = 0.0
    source_structure: Optional[NameStructure] = None
    target_structure: Optional[NameStructure] = None
    match_details: Optional[Dict] = None
    match_type: str = "none"  # exact_credit_code, fuzzy_perfect, fuzzy, none


class EnhancedStructuredMatcher:
    """增强版结构化单位名称匹配器"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        
        # 权重配置 - 优化后的权重分配
        self.weights = {
            'region': 0.10,        # 进一步降低行政区域权重
            'core_name': 0.60,     # 提高核心名称权重，这是最关键的区分标志
            'business_type': 0.20, # 业务类型权重保持中等
            'company_type': 0.10   # 降低公司性质权重
        }
        
        # 地址匹配权重（当名称高度相似时启用）
        self.address_weight = 0.4
        self.name_weight_with_address = 0.6
        
        # 阈值配置 - 更严格的阈值
        self.thresholds = {
            'core_name_strict': 0.90,     # 提高核心名称严格阈值
            'overall_match': 0.85,        # 提高总体匹配阈值
            'address_verification': 0.88, # 需要地址验证的名称相似度阈值
            'address_match': 0.75,        # 提高地址匹配阈值
            'fuzzy_perfect': 0.95         # 模糊完美匹配阈值
        }
        
        # 初始化分词和模式
        self._init_patterns()
    
    def _init_patterns(self):
        """初始化正则表达式模式"""
        
        # 行政区域模式（按长度排序，优先匹配长的）
        self.region_patterns = [
            r'(上海市?(?:浦东新区|黄浦区|徐汇区|长宁区|静安区|普陀区|虹口区|杨浦区|闵行区|宝山区|嘉定区|金山区|松江区|青浦区|奉贤区|崇明区|崇明县))',
            r'(上海市?)',
            r'(北京市?(?:东城区|西城区|朝阳区|丰台区|石景山区|海淀区|门头沟区|房山区|通州区|顺义区|昌平区|大兴区|怀柔区|平谷区|密云区|延庆区))',
            r'(北京市?)',
            r'(天津市?(?:和平区|河东区|河西区|南开区|河北区|红桥区|东丽区|西青区|津南区|北辰区|武清区|宝坻区|滨海新区|宁河区|静海区|蓟州区))',
            r'(天津市?)',
            r'(重庆市?(?:渝中区|大渡口区|江北区|沙坪坝区|九龙坡区|南岸区|北碚区|綦江区|大足区|渝北区|巴南区|黔江区|长寿区|江津区|合川区|永川区|南川区|璧山区|铜梁区|潼南区|荣昌区|开州区|梁平区|武隆区|城口县|丰都县|垫江县|忠县|云阳县|奉节县|巫山县|巫溪县|石柱土家族自治县|秀山土家族苗族自治县|酉阳土家族苗族自治县|彭水苗族土家族自治县))',
            r'(重庆市?)',
            r'(广州市?(?:荔湾区|越秀区|海珠区|天河区|白云区|黄埔区|番禺区|花都区|南沙区|增城区|从化区))',
            r'(广州市?)',
            r'(深圳市?(?:罗湖区|福田区|南山区|宝安区|龙岗区|盐田区|龙华区|坪山区|光明区|大鹏新区))',
            r'(深圳市?)',
            r'([\u4e00-\u9fa5]{2,8}(?:市|区|县|省))',  # 通用行政区域
        ]
        
        # 公司性质模式（按长度排序，优先匹配长的）
        self.company_type_patterns = [
            r'(股份有限公司)$',
            r'(有限责任公司)$', 
            r'(有限公司)$',
            r'(股份公司)$',
            r'(集团有限公司)$',
            r'(集团公司)$',
            r'(集团)$',
            r'(公司)$',
            r'(企业)$',
            r'(工厂)$',
            r'(厂)$',
            r'(研究院)$',
            r'(研究所)$',
            r'(事务所)$',
            r'(所)$',
            r'(中心)$',
            r'(部)$'
        ]
        
        # 业务类型关键词（扩展的业务类型）
        self.business_keywords = [
            # 贸易类
            '工贸', '贸易', '商贸', '外贸', '进出口', '国际贸易',
            # 电子电器类
            '电器', '电子', '电气', '电力', '电机', '电缆', '电线',
            # 机械设备类
            '机械', '机电', '设备', '装备', '仪器', '仪表', '工具',
            # 科技信息类
            '科技', '技术', '信息', '软件', '网络', '通信', '数据',
            # 化工医药类
            '化工', '化学', '医药', '生物', '制药', '药业', '化纤',
            # 建筑工程类
            '建筑', '建设', '工程', '装饰', '装修', '设计', '规划',
            # 纺织服装类
            '纺织', '服装', '面料', '针织', '印染', '制衣', '时装',
            # 食品农业类
            '食品', '餐饮', '酒业', '饮料', '农业', '养殖', '种植',
            # 物流运输类
            '物流', '运输', '仓储', '快递', '货运', '配送', '供应链',
            # 金融投资类
            '金融', '投资', '证券', '保险', '银行', '担保', '租赁',
            # 房地产类
            '房地产', '地产', '置业', '开发', '物业', '中介',
            # 能源材料类
            '能源', '石油', '煤炭', '天然气', '新能源', '材料', '钢铁', '金属', '有色', '冶金',
            # 汽车类
            '汽车', '汽配', '摩托', '零部件', '轮胎', '维修',
            # 包装印刷类
            '塑料', '橡胶', '玻璃', '陶瓷', '包装', '印刷', '出版',
            # 广告传媒类
            '广告', '传媒', '文化', '影视', '娱乐',
            # 教育咨询类
            '教育', '培训', '咨询', '服务', '管理', '人力资源',
            # 医疗健康类
            '医疗', '健康', '养老', '体育', '康复',
            # 环保清洁类
            '环保', '节能', '清洁', '回收', '处理', '净化',
            # 其他常见类
            '实业', '控股', '发展', '创业', '创新', '智能'
        ]
        
        # 同义词映射
        self.synonym_mapping = {
            # 区域同义词
            '上海市': '上海',
            '北京市': '北京',
            '天津市': '天津',
            '重庆市': '重庆',
            '崇明县': '崇明区',
            # 业务类型同义词
            '工贸': ['贸易', '商贸'],
            '电器': ['电子'],
            '机械': ['机电', '设备'],
            '科技': ['技术'],
            '化工': ['化学'],
            # 公司性质同义词
            '有限公司': ['公司'],
            '股份有限公司': ['股份公司'],
        }
    
    def match_single_record_enhanced(self, source_record: Dict, target_records: List[Dict]) -> Dict:
        """
        增强版单记录匹配，集成结构化匹配逻辑
        
        Args:
            source_record: 源记录（安全隐患排查系统）
            target_records: 目标记录列表（消防监督检查系统）
            
        Returns:
            Dict: 匹配结果
        """
        source_name = source_record.get('UNIT_NAME', '')
        source_address = source_record.get('ADDRESS', '')
        source_credit_code = source_record.get('CREDIT_CODE', '')
        
        if not source_name:
            return {
                'matched': False,
                'similarity_score': 0.0,
                'source_record': source_record,
                'target_record': None,
                'match_details': {'reason': 'no_source_name'}
            }
        
        best_match = None
        best_score = 0.0
        best_match_type = "none"
        best_match_details = {}
        
        # 1. 优先进行信用代码精确匹配
        if source_credit_code:
            for target_record in target_records:
                target_credit_code = target_record.get('shxydm', '')
                if source_credit_code == target_credit_code and target_credit_code:
                    return {
                        'matched': True,
                        'similarity_score': 1.0,
                        'source_record': source_record,
                        'target_record': target_record,
                        'match_type': 'exact_credit_code',
                        'match_details': {'method': 'exact_credit_code_match'}
                    }
        
        # 2. 进行结构化名称匹配
        for target_record in target_records:
            target_name = target_record.get('dwmc', '')
            target_address = target_record.get('dwdz', '')
            
            if not target_name:
                continue
            
            # 使用结构化匹配
            structured_result = self.match_structured_names(
                source_name, target_name, source_address, target_address
            )
            
            if structured_result.matched and structured_result.overall_similarity > best_score:
                best_score = structured_result.overall_similarity
                best_match = target_record
                best_match_details = structured_result.match_details
                
                # 确定匹配类型
                if structured_result.overall_similarity >= self.thresholds['fuzzy_perfect']:
                    best_match_type = "fuzzy_perfect"
                else:
                    best_match_type = "fuzzy"
        
        if best_match:
            return {
                'matched': True,
                'similarity_score': best_score,
                'source_record': source_record,
                'target_record': best_match,
                'match_type': best_match_type,
                'match_details': best_match_details
            }
        else:
            return {
                'matched': False,
                'similarity_score': 0.0,
                'source_record': source_record,
                'target_record': None,
                'match_type': 'none',
                'match_details': {'reason': 'no_structured_match_found'}
            }
    
    def parse_company_name(self, company_name: str) -> NameStructure:
        """
        解析公司名称结构
        
        Args:
            company_name: 公司名称
            
        Returns:
            NameStructure: 解析后的名称结构
        """
        if not company_name:
            return NameStructure(original=company_name)
        
        name = company_name.strip()
        original_name = name
        
        # 预处理：移除括号内容（通常是备注信息）
        name = re.sub(r'[（(][^）)]*[）)]', '', name)
        
        # 1. 提取行政区域（从前面开始匹配）
        region = ""
        for pattern in self.region_patterns:
            match = re.match(pattern, name)
            if match:
                region = match.group(1)
                name = name[len(region):]  # 移除已匹配的区域
                break
        
        # 2. 提取公司性质（从后面开始匹配）
        company_type = ""
        for pattern in self.company_type_patterns:
            match = re.search(pattern, name)
            if match:
                company_type = match.group(1)
                name = name[:match.start()]  # 移除已匹配的公司性质
                break
        
        # 3. 在剩余部分中提取业务类型（优先匹配长的关键词）
        business_type = ""
        remaining_name = name
        
        # 按长度排序，优先匹配长的业务类型
        sorted_keywords = sorted(self.business_keywords, key=len, reverse=True)
        for keyword in sorted_keywords:
            if keyword in name:
                business_type = keyword
                remaining_name = remaining_name.replace(keyword, '', 1)
                break
        
        # 4. 剩余部分作为核心名称
        core_name = remaining_name.strip()
        
        # 计算分解置信度
        confidence = self._calculate_parse_confidence(
            original_name, region, core_name, business_type, company_type
        )
        
        return NameStructure(
            region=region,
            core_name=core_name,
            business_type=business_type,
            company_type=company_type,
            original=original_name,
            confidence=confidence
        )
    
    def _calculate_parse_confidence(self, original: str, region: str, core_name: str, 
                                  business_type: str, company_type: str) -> float:
        """计算名称解析置信度"""
        
        total_chars = len(original)
        if total_chars == 0:
            return 0.0
        
        parsed_chars = len(region) + len(core_name) + len(business_type) + len(company_type)
        coverage = parsed_chars / total_chars
        
        # 基础置信度基于覆盖率
        confidence = coverage * 0.5
        
        # 如果有核心名称，增加置信度
        if core_name:
            confidence += 0.3
        
        # 如果有业务类型，增加置信度
        if business_type:
            confidence += 0.1
        
        # 如果有公司性质，增加置信度
        if company_type:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def match_structured_names(self, source_name: str, target_name: str, 
                             source_address: str = "", target_address: str = "") -> EnhancedStructuredMatchResult:
        """
        结构化名称匹配
        
        Args:
            source_name: 源单位名称
            target_name: 目标单位名称  
            source_address: 源单位地址（可选）
            target_address: 目标单位地址（可选）
            
        Returns:
            EnhancedStructuredMatchResult: 结构化匹配结果
        """
        
        # 1. 解析两个名称的结构
        source_structure = self.parse_company_name(source_name)
        target_structure = self.parse_company_name(target_name)
        
        # 2. 计算各部分相似度
        region_sim = self._calculate_field_similarity(
            source_structure.region, target_structure.region, 'region'
        )
        
        core_name_sim = self._calculate_field_similarity(
            source_structure.core_name, target_structure.core_name, 'core_name'
        )
        
        business_type_sim = self._calculate_field_similarity(
            source_structure.business_type, target_structure.business_type, 'business_type'
        )
        
        company_type_sim = self._calculate_field_similarity(
            source_structure.company_type, target_structure.company_type, 'company_type'
        )
        
        # 3. 计算加权总体相似度
        overall_sim = (
            region_sim * self.weights['region'] +
            core_name_sim * self.weights['core_name'] +
            business_type_sim * self.weights['business_type'] +
            company_type_sim * self.weights['company_type']
        )
        
        # 4. 核心名称严格检查
        core_name_strict_pass = core_name_sim >= self.thresholds['core_name_strict']
        
        # 5. 地址验证（当名称高度相似但核心名称不够严格时）
        address_sim = 0.0
        need_address_verification = (
            overall_sim >= self.thresholds['address_verification'] and 
            not core_name_strict_pass and
            source_address and target_address
        )
        
        if need_address_verification:
            address_sim = self._calculate_address_similarity(source_address, target_address)
            
            # 重新计算包含地址的总体相似度
            overall_sim = (
                overall_sim * self.name_weight_with_address +
                address_sim * self.address_weight
            )
        
        # 6. 判断是否匹配
        matched = False
        match_type = "none"
        
        if core_name_strict_pass and overall_sim >= self.thresholds['overall_match']:
            # 核心名称严格通过且总体相似度达标
            matched = True
            if overall_sim >= self.thresholds['fuzzy_perfect']:
                match_type = "fuzzy_perfect"
            else:
                match_type = "fuzzy"
        elif need_address_verification and address_sim >= self.thresholds['address_match']:
            # 需要地址验证且地址匹配通过
            matched = True
            match_type = "fuzzy"
        
        # 7. 构建匹配详情
        match_details = {
            'core_name_strict_pass': core_name_strict_pass,
            'need_address_verification': need_address_verification,
            'weights_used': self.weights.copy(),
            'thresholds_used': self.thresholds.copy(),
            'source_parse_confidence': source_structure.confidence,
            'target_parse_confidence': target_structure.confidence,
            'match_method': 'enhanced_structured_matching'
        }
        
        return EnhancedStructuredMatchResult(
            matched=matched,
            overall_similarity=overall_sim,
            region_similarity=region_sim,
            core_name_similarity=core_name_sim,
            business_type_similarity=business_type_sim,
            company_type_similarity=company_type_sim,
            address_similarity=address_sim,
            source_structure=source_structure,
            target_structure=target_structure,
            match_details=match_details,
            match_type=match_type
        )
    
    def _calculate_field_similarity(self, source_field: str, target_field: str, field_type: str) -> float:
        """计算字段相似度，支持同义词映射"""
        
        if not source_field or not target_field:
            # 如果有一个字段为空，根据字段类型返回不同的默认值
            if field_type == 'core_name':
                return 0.0  # 核心名称为空则完全不匹配
            elif field_type in ['region', 'business_type', 'company_type']:
                return 0.5  # 其他字段为空时给予中等分数
            return 0.0
        
        # 完全相同
        if source_field == target_field:
            return 1.0
        
        # 检查同义词映射
        if field_type in ['region', 'business_type', 'company_type']:
            if self._check_synonyms(source_field, target_field, field_type):
                return 1.0
        
        # 根据字段类型选择不同的相似度算法
        if field_type == 'core_name':
            # 核心名称使用更严格的匹配
            return max(
                fuzz.ratio(source_field, target_field) / 100.0,
                fuzz.token_set_ratio(source_field, target_field) / 100.0
            )
        elif field_type == 'region':
            # 区域名称使用部分匹配
            return fuzz.partial_ratio(source_field, target_field) / 100.0
        else:
            # 业务类型和公司性质使用标准匹配
            return fuzz.ratio(source_field, target_field) / 100.0
    
    def _check_synonyms(self, field1: str, field2: str, field_type: str) -> bool:
        """检查两个字段是否为同义词"""
        
        for key, synonyms in self.synonym_mapping.items():
            if isinstance(synonyms, list):
                # 处理一对多同义词
                all_terms = [key] + synonyms
                if field1 in all_terms and field2 in all_terms:
                    return True
            else:
                # 处理一对一同义词
                if (field1 == key and field2 == synonyms) or (field1 == synonyms and field2 == key):
                    return True
        
        return False
    
    def _calculate_address_similarity(self, source_address: str, target_address: str) -> float:
        """计算地址相似度"""
        
        if not source_address or not target_address:
            return 0.0
        
        # 标准化地址
        source_addr = self._normalize_address(source_address)
        target_addr = self._normalize_address(target_address)
        
        if not source_addr or not target_addr:
            return 0.0
        
        # 使用多种算法计算地址相似度，取最高值
        similarities = [
            fuzz.ratio(source_addr, target_addr) / 100.0,
            fuzz.partial_ratio(source_addr, target_addr) / 100.0,
            fuzz.token_set_ratio(source_addr, target_addr) / 100.0,
            fuzz.token_sort_ratio(source_addr, target_addr) / 100.0
        ]
        
        return max(similarities)
    
    def _normalize_address(self, address: str) -> str:
        """标准化地址"""
        
        if not address:
            return ""
        
        # 移除常见的地址前缀和后缀
        addr = str(address).strip()
        
        # 移除邮编
        addr = re.sub(r'\d{6}', '', addr)
        
        # 移除电话号码
        addr = re.sub(r'[\d\-\(\)\s]{10,}', '', addr)
        
        # 标准化空格
        addr = re.sub(r'\s+', '', addr)
        
        # 移除特殊字符，保留中文、英文、数字
        addr = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', addr)
        
        return addr.lower() 