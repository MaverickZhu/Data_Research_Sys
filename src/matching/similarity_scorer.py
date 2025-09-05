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
        # 恢复地址标准化 - 这是地址匹配的核心功能
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
            'province': '',    # 1省级：省、自治区、直辖市
            'city': '',        # 2地级：地级市、地区、自治州、盟
            'district': '',    # 3县级：县、县级市、区、旗
            'town': '',        # 4乡级：街道、镇、乡
            'street': '',      # 5村级：社区、行政村、新村、里弄、道路
            'number': '',      # 6门牌：门牌号、楼号、单元号、室号
            'building': '',    # 建筑物名称（不参与6级匹配）
            'detail': ''       # 其他详细信息（不参与6级匹配）
        }
        
        # 【修复】确保输入地址已标准化（调用方已处理，这里做二次确认）
        # 注意：此时address应该已经是标准化后的地址
        logger.debug(f"提取地址组件，输入地址: '{address}'")
        
        # 【2025年8月22日修复】6级地址组件提取 - 严格按照6级层级结构
        # 1省级：省、自治区、直辖市；2地级：地级市、地区、自治州、盟；3县级：县、县级市、区、旗；
        # 4乡级：街道、镇、乡；5村级：社区、行政村、新村、里弄、道路；6门牌：门牌号、楼号、单元号、室号
        patterns = {
            'province': r'([^省市区县]{2,8}(?:省|市|自治区))',  # 1省级
            'city': r'([^省市区县]{2,8}(?:市|州|县))',          # 2地级  
            'district': r'([^省市区县镇]{2,8}(?:区|县|开发区|高新区|经济区))',  # 3县级
            'town': r'([^省市区县]{2,8}(?:镇|乡|街道))',        # 4乡级：街道、镇、乡
            'street': r'([^路街道巷弄里号栋幢座楼室层]{1,20}(?:路|街|道|巷|里|大街|大道|街道|村|邨|社区|新村|小区|公寓|花园|苑|庄|家园|城|园区))',  # 5村级：道路、村、邨、社区、小区等（移除弄，避免与门牌冲突）
            'number': r'(\d+(?:弄\d*号?|号|栋|幢|座|楼|室|层))',  # 6门牌：门牌号、楼号、单元号、室号
            'building': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:养老院|敬老院|老年公寓|护理院|福利院|公司|厂|店|馆|所|站|场|生态园|科技园|工业园|产业园|广场|中心|大厦))'
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
        
        # 【核心算法】严格6级分层匹配 - 按用户2025年8月21日要求实现
        # 1省级：省、自治区、直辖市；2地级：地级市、地区、自治州、盟；3县级：县、县级市、区、旗；
        # 4乡级：街道、镇、乡；5村级：社区、行政村、新村、里弄、道路；6门牌：门牌号、楼号、单元号、室号
        hierarchy_levels = [
            ('province', '1省级', 0.90),   # 省、自治区、直辖市
            ('city', '2地级', 0.90),       # 地级市、地区、自治州、盟
            ('district', '3县级', 0.90),   # 县、县级市、区、旗
            ('town', '4乡级', 0.90),       # 街道、镇、乡
            ('street', '5村级', 0.90),     # 社区、行政村、新村、里弄、道路
            ('number', '6门牌', 0.85),     # 门牌号、楼号、单元号、室号
        ]
        
        logger.debug(f"🔍 开始严格6级分层匹配验证（共{len(hierarchy_levels)}级）")
        
        # 【2025年8月22日修复】先检查地址完整性级别是否匹配
        def get_address_max_level(comp):
            """获取地址的最高级别"""
            for level_idx, (level, _, _) in enumerate(hierarchy_levels):
                if comp.get(level, '').strip():
                    max_level = level_idx + 1
            return max_level if 'max_level' in locals() else 0
        
        max_level1 = get_address_max_level(comp1)
        max_level2 = get_address_max_level(comp2)
        
        logger.debug(f"📊 地址完整性检查: 地址1最高级别={max_level1}, 地址2最高级别={max_level2}")
        
        # 【2025年8月22日修复】第6级（门牌）严格匹配检查
        # 跳空仅对6级以前的生效，第6级必须严格匹配
        val1_level6 = comp1.get('number', '').strip()  # 第6级：门牌
        val2_level6 = comp2.get('number', '').strip()
        
        # 如果一个有第6级，另一个没有第6级 → 匹配失败
        if (val1_level6 and not val2_level6) or (not val1_level6 and val2_level6):
            logger.debug(f"❌ 第6级（门牌）不对等: 地址1='{val1_level6}', 地址2='{val2_level6}'")
            logger.debug(f"   逻辑依据: 跳空仅对6级以前生效，第6级必须严格匹配")
            return 0.0
        
        logger.debug(f"✅ 第6级（门牌）对等性检查通过: 地址1='{val1_level6}', 地址2='{val2_level6}'")
        
        # 【严格6级匹配】逐级验证
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
            
            # 【关键逻辑2】有空值的处理 - 2025年8月22日修复
            elif not val1 or not val2:
                current_level = level_idx + 1
                
                # 检查是否在最高级别范围内
                if current_level <= max(max_level1, max_level2):
                    logger.debug(f"🔄 第{level_idx+1}级 {level_name}存在空值: '{val1}' vs '{val2}' → 继续下级验证")
                    continue
                else:
                    logger.debug(f"⏹️ 第{level_idx+1}级 {level_name}超出地址最高级别 → 停止验证")
                    break
        
        logger.debug(f"🎯 严格6级分层匹配验证通过！开始计算综合相似度")
        
        # 【按用户要求】6级匹配通过后，直接进行评分，不需要额外的冲突检测
        
        # 【第二阶段】严格6级权重配置 - 按用户2025年8月21日要求
        # 1省级：省、自治区、直辖市；2地级：地级市、地区、自治州、盟；3县级：县、县级市、区、旗；
        # 4乡级：街道、镇、乡；5村级：社区、行政村、新村、里弄、道路；6门牌：门牌号、楼号、单元号、室号
        weights = {
            'province': 0.10,    # 1省级：省、自治区、直辖市
            'city': 0.15,        # 2地级：地级市、地区、自治州、盟
            'district': 0.20,    # 3县级：县、县级市、区、旗
            'town': 0.20,        # 4乡级：街道、镇、乡
            'street': 0.25,      # 5村级：社区、行政村、新村、里弄、道路
            'number': 0.30,      # 6门牌：门牌号、楼号、单元号、室号
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
                
                # 统计关键组件匹配（6级中的关键级别）
                if component in ['district', 'town', 'street', 'number']:  # 3县级、4乡级、5村级、6门牌
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
        
        【2025年8月21日新增】增加人为输入数据错误的容错处理
        处理如：上海虹口天宝路881号 vs 上海市虹口区天宝路881号
        """
        if val1 == val2:
            return 1.0
        
        # 【2025年8月22日修复】对地址组件进行二次标准化，确保数据一致性
        # 对于street组件（第5级村级），不使用会移除小区村名的标准化
        if component_type == 'street':
            # 对于村级组件，只进行基本标准化，不移除小区村名
            normalized_val1 = val1.strip() if val1 else ""
            normalized_val2 = val2.strip() if val2 else ""
        else:
            # 对于其他组件，使用完整的地址标准化
            # 恢复地址标准化 - 这是地址匹配的核心功能
            from .address_normalizer import normalize_address_for_matching
            normalized_val1 = normalize_address_for_matching(val1) if val1 else ""
            normalized_val2 = normalize_address_for_matching(val2) if val2 else ""
        
        # 标准化后再次检查完全匹配
        if normalized_val1 == normalized_val2:
            return 1.0
        
        # 【新增】人为输入错误容错处理
        tolerance_score = self._calculate_tolerance_similarity(val1, val2, component_type)
        if tolerance_score > 0.0:
            return tolerance_score
        
        if component_type == 'number':
            # 【2025年8月22日修复】门牌号必须精确匹配，不允许模糊匹配
            # 门牌号是地址的关键标识，必须完全一致才能认为是同一地址
            
            # 提取所有数字进行完整比较
            num1 = re.findall(r'\d+', normalized_val1)
            num2 = re.findall(r'\d+', normalized_val2)
            
            # 只有当所有数字完全匹配时才认为相似
            if num1 == num2 and len(num1) > 0:
                # 数字完全匹配，检查格式是否相似（如：27号 vs 27室）
                if normalized_val1 == normalized_val2:
                    return 1.0  # 完全匹配
                else:
                    return 0.95  # 数字相同但格式略有差异
            else:
                # 数字不匹配，门牌号不同，返回0（严格匹配）
                return 0.0
        
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
            'province': '',    # 1省级：省、自治区、直辖市
            'city': '',        # 2地级：地级市、地区、自治州、盟
            'district': '',    # 3县级：县、县级市、区、旗
            'town': '',        # 4乡级：街道、镇、乡
            'street': '',      # 5村级：社区、行政村、新村、里弄、道路
            'number': '',      # 6门牌：门牌号、楼号、单元号、室号
            'building': '',    # 建筑物名称（不参与6级匹配）
            'detail': ''       # 其他详细信息（不参与6级匹配）
        }
        
        # 预处理：移除行政层级词汇
        remaining_address = address
        remaining_address = re.sub(r'市辖区', '', remaining_address)  # 移除"市辖区"
        remaining_address = re.sub(r'县辖区', '', remaining_address)  # 移除"县辖区"
        
        # 【2025年8月22日修复】6级地址组件提取 - 严格按照6级层级结构
        # 1省级：省、自治区、直辖市；2地级：地级市、地区、自治州、盟；3县级：县、县级市、区、旗；
        # 4乡级：街道、镇、乡；5村级：社区、行政村、新村、里弄、道路；6门牌：门牌号、楼号、单元号、室号
        patterns = {
            'province': r'([^省市区县]{2,8}(?:省|市|自治区))',  # 1省级
            'city': r'([^省市区县]{2,8}(?:市|州|县))',          # 2地级  
            'district': r'([^省市区县镇]{2,8}(?:区|县|开发区|高新区|经济区))',  # 3县级
            'town': r'([^省市区县]{2,8}(?:镇|乡|街道))',        # 4乡级：街道、镇、乡
            'street': r'([^路街道巷弄里号栋幢座楼室层]{1,20}(?:路|街|道|巷|里|大街|大道|街道|村|邨|社区|新村|小区|公寓|花园|苑|庄|家园|城|园区))',  # 5村级：道路、村、邨、社区、小区等（移除弄，避免与门牌冲突）
            'number': r'(\d+(?:弄\d*号?|号|栋|幢|座|楼|室|层))',  # 6门牌：门牌号、楼号、单元号、室号
            'building': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:养老院|敬老院|老年公寓|护理院|福利院|公司|厂|店|馆|所|站|场|生态园|科技园|工业园|产业园|广场|中心|大厦))'
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
    
    def _calculate_tolerance_similarity(self, val1: str, val2: str, component_type: str) -> float:
        """
        处理人为输入数据错误的容错匹配
        
        常见错误模式：
        1. 缺少行政级别后缀：上海 vs 上海市，虹口 vs 虹口区
        2. 格式不一致：天宝路881号 vs 天宝路881号
        3. 简写vs全称：上海 vs 上海市
        
        Args:
            val1, val2: 待比较的地址组件
            component_type: 组件类型（province, city, district, town, street, number）
            
        Returns:
            float: 容错相似度分数，0.0表示无法容错匹配
        """
        if not val1 or not val2:
            return 0.0
        
        # 【容错策略1】行政级别后缀容错
        if component_type in ['province', 'city', 'district', 'town']:
            # 定义各级别的常见后缀
            suffixes_map = {
                'province': ['省', '市', '自治区', '特别行政区'],  # 省级后缀
                'city': ['市', '地区', '自治州', '盟'],           # 地级后缀  
                'district': ['区', '县', '县级市', '旗'],         # 县级后缀
                'town': ['街道', '镇', '乡', '办事处']            # 乡级后缀
            }
            
            suffixes = suffixes_map.get(component_type, [])
            
            # 尝试移除后缀进行匹配
            core1 = self._remove_admin_suffixes(val1, suffixes)
            core2 = self._remove_admin_suffixes(val2, suffixes)
            
            if core1 and core2 and core1 == core2:
                logger.debug(f"🔧 {component_type}级容错匹配成功: '{val1}' vs '{val2}' → 核心部分 '{core1}'")
                return 0.95  # 高分但略低于完全匹配
        
        # 【容错策略2】街道/道路名称容错
        if component_type == 'street':
            # 移除常见道路后缀进行匹配
            road_suffixes = ['路', '街', '道', '大道', '大街', '巷', '弄', '里', '村']
            core1 = self._remove_road_suffixes(val1, road_suffixes)
            core2 = self._remove_road_suffixes(val2, road_suffixes)
            
            if core1 and core2 and core1 == core2:
                logger.debug(f"🔧 街道容错匹配成功: '{val1}' vs '{val2}' → 核心部分 '{core1}'")
                return 0.95
        
        # 【容错策略3】门牌号格式容错
        if component_type == 'number':
            # 提取数字部分进行匹配
            import re
            nums1 = re.findall(r'\d+', val1)
            nums2 = re.findall(r'\d+', val2)
            
            if nums1 and nums2 and nums1 == nums2:
                logger.debug(f"🔧 门牌号容错匹配成功: '{val1}' vs '{val2}' → 数字部分 {nums1}")
                return 0.90  # 数字相同但格式可能不同
        
        # 【容错策略4】模糊匹配兜底
        # 如果核心内容高度相似，给予一定容错分数
        from fuzzywuzzy import fuzz
        fuzzy_score = fuzz.ratio(val1, val2) / 100.0
        
        if fuzzy_score >= 0.85:  # 85%以上相似度认为是容错匹配
            logger.debug(f"🔧 模糊容错匹配: '{val1}' vs '{val2}' = {fuzzy_score:.3f}")
            return fuzzy_score * 0.9  # 降低一些分数作为容错惩罚
        
        return 0.0  # 无法容错匹配
    
    def _remove_admin_suffixes(self, text: str, suffixes: list) -> str:
        """移除行政级别后缀，返回核心名称"""
        if not text:
            return ""
        
        for suffix in suffixes:
            if text.endswith(suffix):
                core = text[:-len(suffix)].strip()
                if core:  # 确保移除后缀后还有内容
                    return core
        
        return text  # 没有匹配的后缀，返回原文
    
    def _remove_road_suffixes(self, text: str, suffixes: list) -> str:
        """移除道路后缀，返回核心名称"""
        if not text:
            return ""
        
        for suffix in suffixes:
            if text.endswith(suffix):
                core = text[:-len(suffix)].strip()
                if core:  # 确保移除后缀后还有内容
                    return core
        
        return text  # 没有匹配的后缀，返回原文 