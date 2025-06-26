#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生产版结构化单位名称匹配器
基于测试结果优化的最终版本
"""

import re
from typing import Dict, Optional, Set, List, Tuple
from dataclasses import dataclass
from rapidfuzz import fuzz
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
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
class StructuredMatchResult:
    """结构化匹配结果"""
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


class StructuredNameMatcher:
    """生产版结构化单位名称匹配器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化匹配器
        
        Args:
            config: 配置参数字典
        """
        self.config = config or {}
        
        # 权重配置 - 基于测试结果优化
        self.weights = {
            'region': 0.05,        # 行政区域权重较低
            'core_name': 0.70,     # 核心名称权重最高
            'business_type': 0.15, # 业务类型权重适中
            'company_type': 0.10   # 公司性质权重较低
        }
        
        # 地址匹配权重（当名称高度相似时启用）
        self.address_weight = 0.40
        self.name_weight_with_address = 0.60
        
        # 阈值配置 - 基于测试结果优化
        self.thresholds = {
            'core_name_strict': 0.90,     # 核心名称严格阈值
            'overall_match': 0.85,        # 总体匹配阈值
            'address_verification': 0.75, # 地址验证触发阈值（降低以便触发）
            'address_match': 0.70,        # 地址匹配阈值（降低以便匹配）
            'fuzzy_perfect': 0.95,        # 模糊完美匹配阈值
            'business_type_conflict': 0.3 # 业务类型冲突阈值
        }
        
        # 初始化分词和模式
        self._init_patterns()
        self._init_business_conflicts()
    
    def _init_patterns(self):
        """初始化正则表达式模式和关键词"""
        
        # 行政区域模式
        self.region_patterns = [
            # 直辖市具体区县
            r'(上海市?(?:浦东新区|黄浦区|徐汇区|长宁区|静安区|普陀区|虹口区|杨浦区|闵行区|宝山区|嘉定区|金山区|松江区|青浦区|奉贤区|崇明区|崇明县))',
            r'(北京市?(?:东城区|西城区|朝阳区|丰台区|石景山区|海淀区|门头沟区|房山区|通州区|顺义区|昌平区|大兴区|怀柔区|平谷区|密云区|延庆区))',
            r'(天津市?(?:和平区|河东区|河西区|南开区|河北区|红桥区|东丽区|西青区|津南区|北辰区|武清区|宝坻区|滨海新区|宁河区|静海区|蓟州区))',
            r'(重庆市?(?:渝中区|大渡口区|江北区|沙坪坝区|九龙坡区|南岸区|北碚区|綦江区|大足区|渝北区|巴南区|黔江区|长寿区|江津区|合川区|永川区|南川区|璧山区|铜梁区|潼南区|荣昌区|开州区|梁平区|武隆区|城口县|丰都县|垫江县|忠县|云阳县|奉节县|巫山县|巫溪县|石柱县|秀山县|酉阳县|彭水县))',
            
            # 省会城市具体区县
            r'(深圳市?(?:罗湖区|福田区|南山区|宝安区|龙岗区|盐田区|龙华区|坪山区|光明区|大鹏新区))',
            r'(广州市?(?:荔湾区|越秀区|海珠区|天河区|白云区|黄埔区|番禺区|花都区|南沙区|增城区|从化区))',
            r'(杭州市?(?:上城区|下城区|江干区|拱墅区|西湖区|滨江区|萧山区|余杭区|富阳区|临安区|桐庐县|淳安县|建德市))',
            
            # 直辖市
            r'(上海市?)',
            r'(北京市?)',
            r'(天津市?)',
            r'(重庆市?)',
            
            # 省会城市
            r'(深圳市?)',
            r'(广州市?)',
            r'(杭州市?)',
            
            # 通用行政区域（最后匹配）
            r'([\u4e00-\u9fa5]{2,8}(?:市|区|县|省))',
        ]
        
        # 公司性质模式（按优先级排序）
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
        
        # 业务类型关键词（按长度排序，优先匹配长的）
        self.business_keywords = [
            # 贸易类
            '国际贸易', '进出口', '工贸', '贸易', '商贸', '外贸',
            
            # 电子电器类
            '电器', '电子', '电气', '电力', '电机', '电缆', '电线',
            
            # 机械设备类
            '机械', '机电', '设备', '装备', '仪器', '仪表', '工具',
            
            # 科技信息类
            '网络技术', '信息技术', '科技', '技术', '信息', '软件', '网络', '通信', '数据',
            
            # 化工医药类
            '化工', '化学', '医药', '生物', '制药', '药业', '化纤',
            
            # 建筑工程类
            '建筑', '建设', '工程', '装饰', '装修', '设计', '规划',
            
            # 纺织服装类
            '纺织', '服装', '面料', '针织', '印染', '制衣', '时装',
            
            # 食品农业类
            '食品', '餐饮', '酒业', '饮料', '农业', '养殖', '种植',
            
            # 物流运输类
            '供应链', '物流', '运输', '仓储', '快递', '货运', '配送',
            
            # 金融投资类
            '金融', '投资', '证券', '保险', '银行', '担保', '租赁',
            
            # 房地产类
            '房地产', '地产', '置业', '开发', '物业', '中介',
            
            # 能源材料类
            '新能源', '天然气', '能源', '石油', '煤炭', '材料', '钢铁', '金属', '有色', '冶金',
            
            # 汽车类
            '零部件', '汽车', '汽配', '摩托', '轮胎', '维修',
            
            # 包装印刷类
            '塑料', '橡胶', '玻璃', '陶瓷', '包装', '印刷', '出版',
            
            # 广告传媒类
            '广告', '传媒', '文化', '影视', '娱乐',
            
            # 教育咨询类
            '人力资源', '教育', '培训', '咨询', '服务', '管理',
            
            # 医疗健康类
            '医疗', '健康', '养老', '体育', '康复',
            
            # 环保清洁类
            '环保', '节能', '清洁', '回收', '处理', '净化',
            
            # 零售商业类
            '购物中心', '百货', '超市', '商厦', '商场', '购物', '零售', '批发',
            
            # 其他常见类
            '实业', '控股', '发展', '创业', '创新', '智能'
        ]
        
        # 同义词映射
        self.synonym_mapping = {
            # 区域同义词
            '上海市': ['上海'],
            '北京市': ['北京'],
            '天津市': ['天津'],
            '重庆市': ['重庆'],
            '深圳市': ['深圳'],
            '广州市': ['广州'],
            '杭州市': ['杭州'],
            '崇明县': ['崇明区'],
            
            # 业务类型同义词（细化处理）
            '电器': ['电子'],
            '机械': ['机电', '设备'],
            '科技': ['技术'],
            '化工': ['化学'],
            '医药': ['药业', '制药'],
            
            # 公司性质同义词
            '有限公司': ['公司'],
            '股份有限公司': ['股份公司'],
            '集团有限公司': ['集团公司', '集团'],
            
            # 核心名称同义词（简写映射）
            '浦发': ['浦东发展'],
            '同仁堂': ['同仁堂药业', '同仁堂医药'],
        }
    
    def _init_business_conflicts(self):
        """初始化业务类型冲突检测"""
        
        # 定义互相冲突的业务类型组
        self.business_conflict_groups = [
            # 零售业态冲突
            {'超市', '商厦', '商场', '购物中心', '百货'},
            
            # 制造业态冲突（不包括相关的）
            {'化工', '纺织', '食品'},
            
            # 服务业态冲突
            {'物流', '广告', '咨询', '教育', '医疗'},
        ]
        
        # 特殊冲突对（一对一冲突）
        self.special_conflicts = [
            ('超市', '商厦'),
            ('商厦', '超市'),
            ('工贸', '科技'),  # 工贸和科技业务差异较大
            ('科技', '工贸'),
        ]
    
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
        
        # 特殊处理：如果核心名称包含业务类型，进行二次分离
        if core_name and not business_type:
            for keyword in sorted_keywords:
                if keyword in core_name:
                    business_type = keyword
                    core_name = core_name.replace(keyword, '', 1).strip()
                    break
        
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
    
    def _has_business_type_conflict(self, business_type1: str, business_type2: str) -> bool:
        """
        检查两个业务类型是否存在冲突
        
        Args:
            business_type1: 第一个业务类型
            business_type2: 第二个业务类型
            
        Returns:
            bool: 是否存在冲突
        """
        if not business_type1 or not business_type2:
            return False
        
        # 检查是否在同一个冲突组中
        for conflict_group in self.business_conflict_groups:
            if business_type1 in conflict_group and business_type2 in conflict_group:
                return True
        
        # 检查特殊冲突对
        for conflict1, conflict2 in self.special_conflicts:
            if (business_type1 == conflict1 and business_type2 == conflict2):
                return True
        
        return False
    
    def match_structured_names(self, source_name: str, target_name: str, 
                             source_address: str = "", target_address: str = "") -> StructuredMatchResult:
        """
        结构化名称匹配
        
        Args:
            source_name: 源单位名称
            target_name: 目标单位名称  
            source_address: 源单位地址（可选）
            target_address: 目标单位地址（可选）
            
        Returns:
            StructuredMatchResult: 结构化匹配结果
        """
        
        # 1. 解析两个名称的结构
        source_structure = self.parse_company_name(source_name)
        target_structure = self.parse_company_name(target_name)
        
        # 2. 检查业务类型冲突
        business_conflict = self._has_business_type_conflict(
            source_structure.business_type, target_structure.business_type
        )
        
        # 3. 计算各部分相似度
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
        
        # 4. 业务类型冲突惩罚
        if business_conflict:
            business_type_sim = self.thresholds['business_type_conflict']
        
        # 5. 计算加权总体相似度
        overall_sim = (
            region_sim * self.weights['region'] +
            core_name_sim * self.weights['core_name'] +
            business_type_sim * self.weights['business_type'] +
            company_type_sim * self.weights['company_type']
        )
        
        # 6. 核心名称严格检查
        core_name_strict_pass = core_name_sim >= self.thresholds['core_name_strict']
        
        # 7. 地址验证（当名称相似但核心名称不够严格时）
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
        
        # 8. 判断是否匹配
        matched = False
        
        if core_name_strict_pass and overall_sim >= self.thresholds['overall_match'] and not business_conflict:
            # 核心名称严格通过且总体相似度达标且无业务冲突
            matched = True
        elif need_address_verification and address_sim >= self.thresholds['address_match'] and not business_conflict:
            # 需要地址验证且地址匹配通过且无业务冲突
            matched = True
        
        # 9. 构建匹配详情
        match_details = {
            'core_name_strict_pass': core_name_strict_pass,
            'need_address_verification': need_address_verification,
            'business_conflict': business_conflict,
            'weights_used': self.weights.copy(),
            'thresholds_used': self.thresholds.copy(),
            'source_parse_confidence': source_structure.confidence,
            'target_parse_confidence': target_structure.confidence,
            'match_method': 'structured_name_matching'
        }
        
        return StructuredMatchResult(
            matched=matched,
            overall_similarity=overall_sim,
            region_similarity=region_sim,
            core_name_similarity=core_name_sim,
            business_type_similarity=business_type_sim,
            company_type_similarity=company_type_sim,
            address_similarity=address_sim,
            source_structure=source_structure,
            target_structure=target_structure,
            match_details=match_details
        )
    
    def _calculate_field_similarity(self, source_field: str, target_field: str, field_type: str) -> float:
        """
        计算字段相似度，支持同义词映射
        
        Args:
            source_field: 源字段值
            target_field: 目标字段值
            field_type: 字段类型
            
        Returns:
            float: 相似度分数 (0.0-1.0)
        """
        
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
        if self._check_synonyms(source_field, target_field, field_type):
            return 1.0
        
        # 根据字段类型选择不同的相似度算法
        if field_type == 'core_name':
            # 核心名称使用多种算法，取最高值
            similarities = [
                fuzz.ratio(source_field, target_field) / 100.0,
                fuzz.partial_ratio(source_field, target_field) / 100.0,
                fuzz.token_set_ratio(source_field, target_field) / 100.0,
                fuzz.token_sort_ratio(source_field, target_field) / 100.0
            ]
            return max(similarities)
        elif field_type == 'region':
            # 区域名称使用部分匹配
            return fuzz.partial_ratio(source_field, target_field) / 100.0
        else:
            # 业务类型和公司性质使用标准匹配
            return fuzz.ratio(source_field, target_field) / 100.0
    
    def _check_synonyms(self, field1: str, field2: str, field_type: str) -> bool:
        """
        检查两个字段是否为同义词
        
        Args:
            field1: 第一个字段值
            field2: 第二个字段值
            field_type: 字段类型
            
        Returns:
            bool: 是否为同义词
        """
        
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
        
        # 特殊处理：检查是否包含关系（如"同仁堂"包含在"同仁堂药业"中）
        if field_type == 'core_name':
            if (field1 in field2 and len(field1) >= 2) or (field2 in field1 and len(field2) >= 2):
                return True
        
        return False
    
    def _calculate_address_similarity(self, source_address: str, target_address: str) -> float:
        """
        计算地址相似度
        
        Args:
            source_address: 源地址
            target_address: 目标地址
            
        Returns:
            float: 地址相似度分数 (0.0-1.0)
        """
        
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
        """
        标准化地址
        
        Args:
            address: 原始地址
            
        Returns:
            str: 标准化后的地址
        """
        
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
    
    def get_match_type(self, result: StructuredMatchResult) -> str:
        """
        根据匹配结果确定匹配类型
        
        Args:
            result: 结构化匹配结果
            
        Returns:
            str: 匹配类型
        """
        
        if not result.matched:
            return 'none'
        
        if result.overall_similarity >= self.thresholds['fuzzy_perfect']:
            return 'fuzzy_perfect'
        elif result.match_details and result.match_details.get('core_name_strict_pass'):
            return 'fuzzy'
        else:
            return 'fuzzy'
    
    def format_match_result(self, result: StructuredMatchResult) -> Dict:
        """
        格式化匹配结果用于输出
        
        Args:
            result: 结构化匹配结果
            
        Returns:
            Dict: 格式化的匹配结果
        """
        
        return {
            'matched': result.matched,
            'match_type': self.get_match_type(result),
            'overall_similarity': round(result.overall_similarity, 4),
            'region_similarity': round(result.region_similarity, 4),
            'core_name_similarity': round(result.core_name_similarity, 4),
            'business_type_similarity': round(result.business_type_similarity, 4),
            'company_type_similarity': round(result.company_type_similarity, 4),
            'address_similarity': round(result.address_similarity, 4),
            'source_structure': {
                'region': result.source_structure.region,
                'core_name': result.source_structure.core_name,
                'business_type': result.source_structure.business_type,
                'company_type': result.source_structure.company_type,
                'confidence': round(result.source_structure.confidence, 4)
            },
            'target_structure': {
                'region': result.target_structure.region,
                'core_name': result.target_structure.core_name,
                'business_type': result.target_structure.business_type,
                'company_type': result.target_structure.company_type,
                'confidence': round(result.target_structure.confidence, 4)
            },
            'match_details': result.match_details
        }


# 创建全局匹配器实例
structured_matcher = StructuredNameMatcher()


def match_company_names(source_name: str, target_name: str, 
                       source_address: str = "", target_address: str = "") -> Dict:
    """
    匹配公司名称的便捷函数
    
    Args:
        source_name: 源公司名称
        target_name: 目标公司名称
        source_address: 源公司地址（可选）
        target_address: 目标公司地址（可选）
        
    Returns:
        Dict: 格式化的匹配结果
    """
    
    result = structured_matcher.match_structured_names(
        source_name, target_name, source_address, target_address
    )
    
    return structured_matcher.format_match_result(result)


if __name__ == "__main__":
    # 测试代码
    test_cases = [
        ("上海恒琳工贸有限公司", "上海恒虹工贸有限公司"),
        ("上海华联超市有限公司", "上海华联商厦有限公司"),
        ("北京同仁堂药业有限公司", "北京同仁堂医药有限公司"),
        ("上海浦发银行股份有限公司", "上海浦东发展银行股份有限公司"),
    ]
    
    for source, target in test_cases:
        result = match_company_names(source, target)
        print(f"\n源名称: {source}")
        print(f"目标名称: {target}")
        print(f"匹配结果: {'✅ 匹配' if result['matched'] else '❌ 不匹配'}")
        print(f"总体相似度: {result['overall_similarity']:.3f}")
        print(f"匹配类型: {result['match_type']}")