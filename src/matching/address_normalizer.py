#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
地址标准化处理器
将非标准地址转换为标准格式，提升匹配精准度
"""

import re
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class AddressNormalizer:
    """地址标准化处理器"""
    
    def __init__(self):
        """初始化地址标准化器"""
        # 行政区划标准化映射
        self.admin_mappings = {
            # 省级标准化
            '上海': '上海市',
            '北京': '北京市', 
            '天津': '天津市',
            '重庆': '重庆市',
            
            # 市辖区处理
            '市辖区': '',
            '县辖区': '',
            '地区': '',
            
            # 区县标准化
            '浦东新区': '浦东新区',
            '黄浦': '黄浦区',
            '徐汇': '徐汇区',
            '长宁': '长宁区',
            '静安': '静安区',
            '普陀': '普陀区',
            '虹口': '虹口区',
            '杨浦': '杨浦区',
            '闵行': '闵行区',
            '宝山': '宝山区',
            '嘉定': '嘉定区',
            '金山': '金山区',
            '松江': '松江区',
            '青浦': '青浦区',
            '奉贤': '奉贤区',
            '崇明': '崇明区'
        }
        
        # 街道路名标准化
        self.street_mappings = {
            '大道': '路',
            '大街': '街',
            '马路': '路'
        }
        
        # 建筑物类型标准化
        self.building_mappings = {
            '养老院': '养老院',
            '敬老院': '敬老院', 
            '老年公寓': '老年公寓',
            '护理院': '护理院',
            '福利院': '福利院',
            '老人院': '养老院',
            '颐养院': '养老院'
        }
    
    def normalize_address(self, address: str) -> str:
        """
        标准化地址
        
        Args:
            address: 原始地址
            
        Returns:
            str: 标准化后的地址
        """
        if not address or not isinstance(address, str):
            return ""
        
        # 简化的地址标准化策略
        normalized = address.strip()
        
        # 1. 移除干扰词汇
        normalized = normalized.replace('市辖区', '')
        normalized = normalized.replace('县辖区', '')
        
        # 2. 标准化省市格式
        if normalized.startswith('上海') and not normalized.startswith('上海市'):
            normalized = '上海市' + normalized[2:]
        
        # 3. 标准化区县格式  
        if '虹口' in normalized and '虹口区' not in normalized:
            normalized = normalized.replace('虹口', '虹口区')
        
        # 4. 标准化门牌号
        normalized = re.sub(r'(\d+)#', r'\1号', normalized)  # 881# -> 881号
        normalized = re.sub(r'(\d{1,4})(?![号弄栋幢座楼室层\d])', r'\1号', normalized)  # 881 -> 881号
        
        # 5. 标准化街道后缀
        normalized = normalized.replace('大道', '路')
        normalized = normalized.replace('大街', '街')
        
        # 6. 清理多余空格
        normalized = re.sub(r'\s+', '', normalized)
        
        logger.debug(f"地址标准化: '{address}' -> '{normalized}'")
        return normalized
    
    def _basic_cleanup(self, address: str) -> str:
        """基础清理"""
        # 移除多余空格和特殊字符
        address = re.sub(r'\s+', '', address)  # 移除所有空格
        address = re.sub(r'[，。；：！？""''（）【】]', '', address)  # 移除标点符号
        address = address.strip()
        return address
    
    def _normalize_administrative_divisions(self, address: str) -> str:
        """标准化行政区划"""
        # 处理省市标准化
        for old_name, new_name in self.admin_mappings.items():
            if old_name in address:
                if new_name:  # 替换
                    address = address.replace(old_name, new_name)
                else:  # 删除
                    address = address.replace(old_name, '')
        
        # 确保省市区的正确顺序和格式
        address = self._fix_admin_order(address)
        
        return address
    
    def _fix_admin_order(self, address: str) -> str:
        """修正行政区划顺序"""
        # 简化处理：只确保基本的省市区格式正确
        # 不进行复杂的重新组装，避免破坏原有地址结构
        
        # 确保上海补全为上海市
        if address.startswith('上海') and not address.startswith('上海市'):
            address = address.replace('上海', '上海市', 1)
        
        # 确保区县后缀
        if '虹口' in address and '虹口区' not in address:
            address = address.replace('虹口', '虹口区', 1)
        
        return address
    
    def _normalize_streets(self, address: str) -> str:
        """标准化街道路名"""
        for old_suffix, new_suffix in self.street_mappings.items():
            # 替换街道后缀
            pattern = r'([^路街道巷弄里]{2,20})' + re.escape(old_suffix)
            replacement = r'\1' + new_suffix
            address = re.sub(pattern, replacement, address)
        
        return address
    
    def _normalize_house_numbers(self, address: str) -> str:
        """标准化门牌号"""
        # 统一门牌号格式
        # 处理各种门牌号格式: 881号, 881, 881#, 881-1号等
        patterns = [
            (r'(\d+)#', r'\1号'),  # 881# -> 881号
            (r'(\d+)-(\d+)号?', r'\1号\2室'),  # 881-1 -> 881号1室
            (r'(\d+)弄(\d+)号', r'\1弄\2号'),  # 保持弄号格式
            (r'(\d{1,4})(?![号弄栋幢座楼室层\d])', r'\1号')  # 纯数字补号，避免拆分长数字
        ]
        
        for pattern, replacement in patterns:
            address = re.sub(pattern, replacement, address)
        
        return address
    
    def _normalize_buildings(self, address: str) -> str:
        """标准化建筑物名称"""
        for old_type, new_type in self.building_mappings.items():
            if old_type in address and old_type != new_type:
                address = address.replace(old_type, new_type)
        
        return address
    
    def _final_cleanup(self, address: str) -> str:
        """最终清理"""
        # 移除重复的行政区划
        address = re.sub(r'(上海市){2,}', '上海市', address)
        address = re.sub(r'(虹口区){2,}', '虹口区', address)
        
        # 移除多余的连续标点
        address = re.sub(r'号+', '号', address)
        
        return address.strip()
    
    def extract_standard_components(self, address: str) -> Dict[str, str]:
        """
        提取标准化地址组件
        
        Args:
            address: 标准化后的地址
            
        Returns:
            Dict[str, str]: 地址组件字典
        """
        components = {
            'province': '',
            'city': '',
            'district': '',
            'street': '',
            'number': '',
            'building': '',
            'detail': ''
        }
        
        remaining = address
        
        # 按优先级提取组件
        patterns = {
            'province': r'([^省市区县]{2,8}(?:省|市|自治区))',
            'city': r'([^省市区县]{2,8}(?:市|州|县))', 
            'district': r'([^省市区县]{2,8}(?:区|县|镇|开发区|高新区|经济区))',
            'street': r'([^路街道巷弄里号栋幢座楼室层]{1,20}(?:路|街|道|巷|弄|里|大街|大道|街道))',
            'number': r'(\d+(?:号|弄|栋|幢|座|楼|室|层)(?:\d+(?:号|室|层)?)?)',
            'building': r'([^路街道巷弄里号栋幢座楼室层]{2,20}(?:养老院|敬老院|老年公寓|护理院|福利院|大厦|大楼|广场|中心|院|园|村|小区|公司|厂|店|馆|所|站|场))'
        }
        
        for component, pattern in patterns.items():
            matches = re.findall(pattern, remaining)
            if matches:
                components[component] = matches[0]
                remaining = remaining.replace(matches[0], '', 1)
        
        components['detail'] = remaining.strip()
        
        return components


def normalize_address_for_matching(address: str) -> str:
    """
    地址匹配专用标准化函数
    
    Args:
        address: 原始地址
        
    Returns:
        str: 标准化后的地址
    """
    normalizer = AddressNormalizer()
    return normalizer.normalize_address(address)
