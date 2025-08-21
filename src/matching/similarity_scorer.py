"""
相似度计算器模块
实现多种相似度计算算法
"""

import logging
import re
from typing import Dict, List, Optional, Tuple, Any
import jieba
import pypinyin
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from src.utils.helpers import normalize_string, normalize_phone, safe_float_convert, calculate_percentage_diff

logger = logging.getLogger(__name__)


class SimilarityCalculator:
    """相似度计算器"""
    
    def __init__(self, config: Dict):
        """
        初始化相似度计算器
        
        Args:
            config: 配置参数
        """
        self.config = config
        self.string_config = config.get('string_similarity', {})
        self.chinese_config = self.string_config.get('chinese_processing', {})
        self.numeric_config = config.get('numeric_similarity', {})
        self.phone_config = config.get('phone_similarity', {})
        self.address_config = config.get('address_similarity', {})
        
        # 初始化TF-IDF向量化器
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words=None,
            ngram_range=(1, 2)
        )
        
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
            
        # 标准化字符串
        str1 = normalize_string(str1)
        str2 = normalize_string(str2)
        
        if str1 == str2:
            return 1.0
            
        # 获取算法配置
        algorithms = self.string_config.get('algorithms', [])
        if not algorithms:
            algorithms = [
                {'name': 'levenshtein', 'weight': 0.3},
                {'name': 'jaro_winkler', 'weight': 0.3},
                {'name': 'cosine', 'weight': 0.4}
            ]
        
        total_score = 0.0
        total_weight = 0.0
        
        for algorithm in algorithms:
            name = algorithm['name']
            weight = algorithm['weight']
            
            if name == 'levenshtein':
                score = self._levenshtein_similarity(str1, str2)
            elif name == 'jaro_winkler':
                score = self._jaro_winkler_similarity(str1, str2)
            elif name == 'cosine':
                score = self._cosine_similarity(str1, str2)
            else:
                continue
                
            total_score += score * weight
            total_weight += weight
        
        # 中文处理增强
        if self.chinese_config.get('enable_pinyin', True):
            pinyin_score = self._pinyin_similarity(str1, str2)
            total_score += pinyin_score * 0.2
            total_weight += 0.2
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def _levenshtein_similarity(self, str1: str, str2: str) -> float:
        """Levenshtein距离相似度"""
        return fuzz.ratio(str1, str2) / 100.0
    
    def _jaro_winkler_similarity(self, str1: str, str2: str) -> float:
        """Jaro-Winkler相似度"""
        return fuzz.token_sort_ratio(str1, str2) / 100.0
    
    def _cosine_similarity(self, str1: str, str2: str) -> float:
        """余弦相似度"""
        try:
            # 使用TF-IDF向量化
            corpus = [str1, str2]
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(corpus)
            similarity_matrix = cosine_similarity(tfidf_matrix)
            
            return similarity_matrix[0, 1]
        except Exception as e:
            logger.debug(f"余弦相似度计算失败: {str(e)}")
            return 0.0
    
    def _pinyin_similarity(self, str1: str, str2: str) -> float:
        """拼音相似度"""
        try:
            # 转换为拼音
            pinyin1 = ''.join([item[0] for item in pypinyin.pinyin(str1, style=pypinyin.NORMAL)])
            pinyin2 = ''.join([item[0] for item in pypinyin.pinyin(str2, style=pypinyin.NORMAL)])
            
            if not pinyin1 or not pinyin2:
                return 0.0
            
            return fuzz.ratio(pinyin1, pinyin2) / 100.0
        except Exception as e:
            logger.debug(f"拼音相似度计算失败: {str(e)}")
            return 0.0
    
    def calculate_numeric_similarity(self, num1: Any, num2: Any, field_config: Dict) -> float:
        """
        计算数值相似度
        
        Args:
            num1: 数值1
            num2: 数值2
            field_config: 字段配置
            
        Returns:
            float: 相似度分数 (0-1)
        """
        # 转换为浮点数
        val1 = safe_float_convert(num1)
        val2 = safe_float_convert(num2)
        
        if val1 == 0 and val2 == 0:
            return 1.0
        
        # 获取容差配置
        tolerance = field_config.get('tolerance', 0.1)
        
        # 计算百分比差异
        diff = calculate_percentage_diff(val1, val2)
        
        # 如果差异在容差范围内，返回高相似度
        if diff <= tolerance:
            return 1.0 - (diff / tolerance) * 0.2  # 最高0.8-1.0
        else:
            # 超出容差范围，相似度快速下降
            return max(0.0, 1.0 - diff)
    
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
        norm_phone1 = normalize_phone(phone1)
        norm_phone2 = normalize_phone(phone2)
        
        if not norm_phone1 or not norm_phone2:
            return 0.0
        
        # 完全匹配
        if norm_phone1 == norm_phone2:
            return 1.0
        
        # 部分匹配（后7位）
        if len(norm_phone1) >= 7 and len(norm_phone2) >= 7:
            suffix1 = norm_phone1[-7:]
            suffix2 = norm_phone2[-7:]
            if suffix1 == suffix2:
                return 0.8
        
        # 使用编辑距离
        return fuzz.ratio(norm_phone1, norm_phone2) / 100.0 * 0.6
    
    def calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """
        计算地址相似度（增强版 - 支持模糊地址与标准地址匹配）
        
        Args:
            addr1: 地址1（可能是模糊描述地址）
            addr2: 地址2（可能是标准地址）
            
        Returns:
            float: 相似度分数 (0-1)
        """
        if not addr1 or not addr2:
            return 0.0
        
        # 地址标准化预处理
        from .address_normalizer import normalize_address_for_matching
        normalized_addr1 = normalize_address_for_matching(addr1)
        normalized_addr2 = normalize_address_for_matching(addr2)
        
        logger.debug(f"地址标准化: '{addr1}' -> '{normalized_addr1}'")
        logger.debug(f"地址标准化: '{addr2}' -> '{normalized_addr2}'")
        
        # 【调试】记录标准化效果
        if addr1 != normalized_addr1 or addr2 != normalized_addr2:
            logger.info(f"🔧 地址标准化生效: 原始1='{addr1}' 标准化1='{normalized_addr1}' | 原始2='{addr2}' 标准化2='{normalized_addr2}'")
        
        if normalized_addr1 == normalized_addr2:
            return 1.0
        
        # 使用标准化后的地址进行语义分析
        return self._enhanced_address_semantic_similarity(normalized_addr1, normalized_addr2)
    
    def _enhanced_address_semantic_similarity(self, addr1: str, addr2: str) -> float:
        """
        增强的地址语义相似度分析
        专门处理模糊地址描述与标准地址的匹配
        
        Args:
            addr1: 地址1
            addr2: 地址2
            
        Returns:
            float: 相似度分数 (0-1)
        """
        try:
            # 1. 提取地址核心组件
            components1 = self._extract_enhanced_address_components(addr1)
            components2 = self._extract_enhanced_address_components(addr2)
            
            # 2. 计算组件权重化相似度
            return self._calculate_weighted_address_similarity(components1, components2)
            
        except Exception as e:
            logger.debug(f"增强地址语义分析失败，回退到基础方法: {str(e)}")
            # 回退到原有的分段匹配
            if self.address_config.get('enable_segmentation', True):
                return self._segmented_address_similarity(addr1, addr2)
            else:
                return self.calculate_string_similarity(addr1, addr2)
    
    def _extract_enhanced_address_components(self, address: str) -> Dict[str, str]:
        """
        提取增强的地址组件
        支持更复杂的地址解析，包括建筑物名称、门牌号等
        
        【关键修复】确保输入地址已经过标准化处理
        """
        components = {
            'province': '',      # 省份
            'city': '',         # 城市
            'district': '',     # 区县
            'street': '',       # 街道/路
            'number': '',       # 门牌号
            'building': '',     # 建筑物名称
            'detail': ''        # 其他详细信息
        }
        
        # 【修复】确保输入地址已标准化（调用方已处理，这里做二次确认）
        # 注意：此时address应该已经是标准化后的地址
        logger.debug(f"提取地址组件，输入地址: '{address}'")
        
        # 地址解析正则表达式（优化版 - 适配标准化后的地址格式）
        patterns = {
            'province': r'([^省市区县]{2,8}(?:省|市|自治区))',
            'city': r'([^省市区县]{2,8}(?:市|州|县))',
            'district': r'([^省市区县]{2,8}(?:区|县|镇|开发区|高新区|经济区))',
            'street': r'([^路街道巷弄里号]{1,20}(?:路|街|道|巷|弄|里|街道))',  # 【修复】移除"大街|大道"因为已标准化为"路|街"
            'number': r'(\d+(?:号|弄|栋|幢|座|楼|室|层)(?:\d+(?:号|室|层)?)?)',  # 【修复】支持复合门牌号
            'building': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:养老院|敬老院|老年公寓|护理院|福利院|大厦|大楼|广场|中心|院|园|村|小区|公司|厂|店|馆|所|站|场))'  # 【修复】优先匹配养老相关建筑
        }
        
        # 【修复】预处理已在标准化阶段完成，这里直接使用标准化后的地址
        remaining_address = address
        
        # 按顺序提取各组件
        for component, pattern in patterns.items():
            matches = re.findall(pattern, remaining_address)
            if matches:
                # 取最长的匹配（通常更准确）
                matched_component = max(matches, key=len)
                components[component] = matched_component
                # 从剩余地址中移除已匹配的部分
                remaining_address = remaining_address.replace(matched_component, '', 1)
                logger.debug(f"提取到{component}: '{matched_component}'")
        
        # 剩余部分作为详细信息
        components['detail'] = remaining_address.strip()
        
        logger.debug(f"地址组件提取结果: {components}")
        return components
    
    def _calculate_weighted_address_similarity(self, comp1: Dict[str, str], comp2: Dict[str, str]) -> float:
        """
        严格层级终止地址匹配算法
        
        核心原则（按用户要求）：
        1. 按从上往下的层级进行匹配：省→市→区→镇→小区/路→弄→门牌号
        2. 如果同一级出现不匹配则应该终止匹配，因为一定不是同一地址
        3. 只有当该级的地址相同、高度相似或有空值时，才继续向下一级进行匹配
        """
        logger.debug(f"🏛️ 开始严格层级终止地址匹配")
        logger.debug(f"地址1组件: {comp1}")
        logger.debug(f"地址2组件: {comp2}")
        
        # 【核心算法】严格层级终止匹配 - 按用户要求实现
        hierarchy_levels = [
            ('province', '省级', 0.85),    # 省/直辖市
            ('city', '市级', 0.85),        # 市
            ('district', '区县级', 0.80),   # 区/县 - 关键区分级别
            ('town', '镇街级', 0.85),       # 镇/街道 - 关键区分级别
            ('community', '小区级', 0.75),  # 小区/社区 - 关键区分级别
            ('street', '路级', 0.70),      # 路/村
            ('lane', '弄级', 0.80),        # 弄号 - 关键区分级别
            ('number', '门牌级', 0.60),    # 门牌号/组号
        ]
        
        logger.debug(f"🔍 开始严格层级终止验证（共{len(hierarchy_levels)}级）")
        
        for level_idx, (level, level_name, threshold) in enumerate(hierarchy_levels):
            val1 = comp1.get(level, '').strip()
            val2 = comp2.get(level, '').strip()
            
            # 【关键逻辑1】两个都有值 → 必须匹配，否则立即终止
            if val1 and val2:
                level_similarity = self._calculate_component_similarity(val1, val2, level)
                logger.debug(f"🏛️ 第{level_idx+1}级 {level_name}验证: '{val1}' vs '{val2}' = {level_similarity:.3f}")
                
                if level_similarity < threshold:
                    logger.debug(f"❌ 第{level_idx+1}级 {level_name}不匹配 → 立即终止匹配")
                    logger.debug(f"   终止原因: {level_similarity:.3f} < {threshold:.2f}")
                    logger.debug(f"   逻辑依据: 同级地址不匹配，不可能是同一地址")
                    return 0.0
                else:
                    logger.debug(f"✅ 第{level_idx+1}级 {level_name}匹配通过: {level_similarity:.3f} ≥ {threshold:.2f}")
            
            # 【关键逻辑2】有空值 → 允许继续（缺省推断）
            elif not val1 or not val2:
                logger.debug(f"🔄 第{level_idx+1}级 {level_name}存在空值: '{val1}' vs '{val2}' → 继续下级验证")
                continue
        
        logger.debug(f"🎯 严格层级终止验证通过！所有级别均匹配或存在合理空值")
        
        # 【地址类型冲突检测】- 特殊逻辑
        community1, community2 = comp1.get('community', '').strip(), comp2.get('community', '').strip()
        street1, street2 = comp1.get('street', '').strip(), comp2.get('street', '').strip()
        lane1, lane2 = comp1.get('lane', '').strip(), comp2.get('lane', '').strip()
        
        # 小区地址 vs 路弄地址冲突检测
        if community1 and (street2 or lane2):
            logger.debug(f"❌ 地址类型冲突: 小区地址 '{community1}' vs 路弄地址 '{street2}{lane2}'")
            return 0.0
        elif community2 and (street1 or lane1):
            logger.debug(f"❌ 地址类型冲突: 小区地址 '{community2}' vs 路弄地址 '{street1}{lane1}'")
            return 0.0
        
        # 【关键修复6】路/村级强制验证 - 这是最关键的修复！
        # 提取门牌号用于后续逻辑
        number1, number2 = comp1.get('number', '').strip(), comp2.get('number', '').strip()
        
        if street1 and street2:
            street_sim = self._calculate_component_similarity(street1, street2, 'street')
            logger.debug(f"🛣️ 路/村级匹配分析: '{street1}' vs '{street2}' = {street_sim:.3f}")
            
            # 【核心逻辑】路/村名必须高度相似，不能仅凭门牌号匹配
            if street_sim < 0.60:  # 路/村级相似度阈值
                logger.debug(f"❌ 路/村级不匹配强制过滤: '{street1}' vs '{street2}' = {street_sim:.3f}")
                logger.debug(f"   即使门牌号相同('{number1}' vs '{number2}')也不能匹配不同路/村的地址")
                return 0.0
            else:
                logger.debug(f"✅ 路/村级匹配通过: {street_sim:.3f} ≥ 0.60")
        
        # 【关键修复4】防止仅凭门牌号匹配的逻辑
        if number1 and number2 and number1 == number2:
            # 门牌号相同，但必须确保上级地址也匹配
            if not street1 or not street2:
                logger.debug(f"⚠️ 门牌号相同('{number1}')但缺少路/村信息，降低可信度")
            elif street1 != street2:
                # 这种情况已经在上面被过滤了，这里是双重保险
                logger.debug(f"❌ 门牌号相同('{number1}')但路/村不同('{street1}' vs '{street2}')，强制过滤")
                return 0.0
        
        # 【第二阶段】计算综合相似度 - 只有通过层级验证才能到这里
        weights = {
            'province': 0.05,    # 省份权重最低（通常都是上海市）
            'city': 0.08,        # 城市权重较低
            'district': 0.20,    # 区县权重中等（重要区分）
            'town': 0.25,        # 镇/街道权重较高（关键区分）
            'community': 0.35,   # 【新增】小区权重很高（岚皋馨苑vs其他小区必须区分）
            'street': 0.30,      # 路/村权重高（具体定位）
            'lane': 0.30,        # 【新增】弄号权重高（251弄vs其他弄必须区分）
            'number': 0.35,      # 门牌号权重最高（精确定位）
            'building': 0.02,    # 建筑物权重最低
            'detail': 0.01       # 其他详细信息权重最低
        }
        
        total_score = 0.0
        total_weight = 0.0
        matched_components = 0
        critical_matches = 0  # 关键组件匹配数
        
        for component, weight in weights.items():
            val1 = comp1.get(component, '').strip()
            val2 = comp2.get(component, '').strip()
            
            if val1 and val2:
                similarity = self._calculate_component_similarity(val1, val2, component)
                total_score += similarity * weight
                total_weight += weight
                matched_components += 1
                
                # 统计关键组件匹配
                if component in ['district', 'town', 'street', 'number']:
                    critical_matches += 1
                
                logger.debug(f"📊 组件 '{component}': '{val1}' vs '{val2}' = {similarity:.3f} (权重: {weight})")
        
        # 【第三阶段】计算最终得分
        if total_weight > 0 and matched_components >= 2:  # 至少需要2个组件匹配
            weighted_similarity = total_score / total_weight
            
            # 【可信度调整】根据关键组件匹配数量调整可信度
            # 至少需要2个关键组件匹配才能获得高可信度
            if critical_matches >= 2:
                confidence_factor = 1.0
            elif critical_matches == 1:
                confidence_factor = 0.7  # 只有1个关键组件匹配，降低可信度
            else:
                confidence_factor = 0.3  # 没有关键组件匹配，大幅降低可信度
            
            final_score = weighted_similarity * confidence_factor
            
            logger.debug(f"🎯 层级递进匹配结果: 基础得分={weighted_similarity:.3f}, 关键组件={critical_matches}, 可信度={confidence_factor:.3f}, 最终得分={final_score:.3f}")
            return final_score
        
        logger.debug(f"❌ 匹配组件不足或权重为0，返回0分")
        return 0.0
    
    def _calculate_component_similarity(self, val1: str, val2: str, component_type: str) -> float:
        """
        计算特定组件的相似度
        根据组件类型使用不同的匹配策略
        
        【关键修复】确保所有地址组件相似度计算都使用地址标准化后的数据
        """
        if val1 == val2:
            return 1.0
        
        # 【修复】对地址组件进行二次标准化，确保数据一致性
        from .address_normalizer import normalize_address_for_matching
        normalized_val1 = normalize_address_for_matching(val1) if val1 else ""
        normalized_val2 = normalize_address_for_matching(val2) if val2 else ""
        
        # 标准化后再次检查完全匹配
        if normalized_val1 == normalized_val2:
            return 1.0
        
        if component_type == 'number':
            # 门牌号：数字部分完全匹配得分更高
            num1 = re.findall(r'\d+', normalized_val1)
            num2 = re.findall(r'\d+', normalized_val2)
            if num1 and num2 and num1[0] == num2[0]:
                return 0.95  # 数字相同，但可能单位不同
            else:
                return fuzz.ratio(normalized_val1, normalized_val2) / 100.0 * 0.6
        
        elif component_type in ['street', 'building']:
            # 街道和建筑物：使用多种算法综合评估（使用标准化数据）
            scores = [
                fuzz.ratio(normalized_val1, normalized_val2) / 100.0,
                fuzz.partial_ratio(normalized_val1, normalized_val2) / 100.0,
                fuzz.token_set_ratio(normalized_val1, normalized_val2) / 100.0
            ]
            return max(scores)  # 取最高分
        
        else:
            # 其他组件：使用地址专用相似度计算（而非通用字符串相似度）
            # 【关键修复】使用标准化后的数据进行fuzz计算，而不是通用字符串相似度
            scores = [
                fuzz.ratio(normalized_val1, normalized_val2) / 100.0,
                fuzz.token_set_ratio(normalized_val1, normalized_val2) / 100.0
            ]
            return max(scores)
    
    def _apply_address_matching_bonus(self, base_score: float, comp1: Dict[str, str], comp2: Dict[str, str]) -> float:
        """
        应用地址匹配奖励机制
        当多个关键组件匹配时给予额外奖励
        """
        bonus = 0.0
        
        # 关键组件完全匹配奖励
        key_components = ['number', 'street', 'district']
        matched_key_components = 0
        
        for component in key_components:
            val1 = comp1.get(component, '').strip()
            val2 = comp2.get(component, '').strip()
            if val1 and val2 and val1 == val2:
                matched_key_components += 1
        
        # 根据匹配的关键组件数量给予奖励
        if matched_key_components >= 2:
            bonus += 0.1  # 两个或以上关键组件匹配
        elif matched_key_components == 1:
            bonus += 0.05  # 一个关键组件匹配
        
        # 建筑物名称匹配奖励
        building1 = comp1.get('building', '').strip()
        building2 = comp2.get('building', '').strip()
        if building1 and building2:
            building_sim = self.calculate_string_similarity(building1, building2)
            if building_sim > 0.8:
                bonus += 0.05
        
        return base_score + bonus
    
    def _segmented_address_similarity(self, addr1: str, addr2: str) -> float:
        """分段地址相似度"""
        try:
            # 提取地址组件
            components1 = self._extract_address_components(addr1)
            components2 = self._extract_address_components(addr2)
            
            # 权重配置
            weights = {
                'province': self.address_config.get('province_weight', 0.2),
                'city': self.address_config.get('city_weight', 0.3),
                'district': self.address_config.get('district_weight', 0.3),
                'detail': self.address_config.get('detail_weight', 0.2)
            }
            
            total_score = 0.0
            total_weight = 0.0
            
            for component, weight in weights.items():
                comp1 = components1.get(component, '')
                comp2 = components2.get(component, '')
                
                if comp1 or comp2:
                    score = self.calculate_string_similarity(comp1, comp2)
                    total_score += score * weight
                    total_weight += weight
            
            return total_score / total_weight if total_weight > 0 else 0.0
            
        except Exception as e:
            logger.debug(f"分段地址相似度计算失败: {str(e)}")
            return self.calculate_string_similarity(addr1, addr2)
    
    def _extract_address_components(self, address: str) -> Dict[str, str]:
        """提取地址组件"""
        components = {
            'province': '',
            'city': '',
            'district': '',
            'town': '',        # 【新增】镇级行政区划
            'community': '',   # 【新增】小区/社区名称
            'street': '',
            'lane': '',        # 【新增】弄号
            'number': '',
            'building': '',
            'detail': ''
        }
        
        # 预处理：移除行政层级词汇
        remaining_address = address
        remaining_address = re.sub(r'市辖区', '', remaining_address)  # 移除"市辖区"
        remaining_address = re.sub(r'县辖区', '', remaining_address)  # 移除"县辖区"
        
        # 【关键修复】地址解析正则表达式 - 区分小区地址和路弄地址
        patterns = {
            'province': r'([^省市区县]{2,8}(?:省|市|自治区))',
            'city': r'([^省市区县]{2,8}(?:市|州|县))',
            'district': r'([^省市区县镇]{2,8}(?:区|县|开发区|高新区|经济区))',
            'town': r'([^省市区县]{2,8}镇)',  # 【关键修复】单独提取镇级行政区划
            'community': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:小区|公寓|花园|苑|庄|村|新村|家园|城|园区|广场|中心|大厦))',  # 【新增】小区/社区名称
            'street': r'([^路街道巷弄里]{1,20}(?:路|街|道|巷|弄|里|大街|大道|街道))',
            'lane': r'(\d+弄)',  # 【新增】弄号（如251弄）
            'number': r'(\d+(?:号|栋|幢|座|楼|室|层))',
            'building': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:养老院|敬老院|老年公寓|护理院|福利院|公司|厂|店|馆|所|站|场|生态园|科技园|工业园|产业园))'
        }
        
        # 按顺序提取各组件
        for component, pattern in patterns.items():
            matches = re.findall(pattern, remaining_address)
            if matches:
                components[component] = matches[0]
                remaining_address = remaining_address.replace(matches[0], '', 1)
        
        # 剩余部分作为详细地址
        components['detail'] = remaining_address.strip()
        
        return components
    
    def calculate_comprehensive_similarity(self, source_record: Dict, target_record: Dict, 
                                        field_configs: Dict) -> Tuple[float, Dict]:
        """
        计算综合相似度
        
        Args:
            source_record: 源记录
            target_record: 目标记录
            field_configs: 字段配置
            
        Returns:
            Tuple[float, Dict]: (综合相似度, 字段相似度详情)
        """
        
        field_similarities = {}
        weighted_score = 0.0
        total_weight = 0.0
        
        for field_name, field_config in field_configs.items():
            source_field = field_config['source_field']
            target_field = field_config.get('target_field')
            weight = field_config['weight']
            match_type = field_config['match_type']
            
            # 跳过目标字段为空的情况
            if not target_field:
                continue
            
            source_value = source_record.get(source_field)
            target_value = target_record.get(target_field)
            
            # 计算字段相似度
            if match_type == 'string':
                similarity = self.calculate_string_similarity(
                    str(source_value) if source_value else '',
                    str(target_value) if target_value else ''
                )
            elif match_type == 'numeric':
                similarity = self.calculate_numeric_similarity(
                    source_value, target_value, field_config
                )
            elif match_type == 'phone':
                similarity = self.calculate_phone_similarity(
                    str(source_value) if source_value else '',
                    str(target_value) if target_value else ''
                )
            elif match_type == 'address':
                similarity = self.calculate_address_similarity(
                    str(source_value) if source_value else '',
                    str(target_value) if target_value else ''
                )
            else:
                similarity = 0.0
            
            field_similarities[field_name] = {
                'similarity': similarity,
                'weight': weight,
                'source_value': source_value,
                'target_value': target_value
            }
            
            weighted_score += similarity * weight
            total_weight += weight
        
        # 计算综合相似度
        comprehensive_score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        return comprehensive_score, field_similarities 