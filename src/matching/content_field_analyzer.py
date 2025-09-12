#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于内容的字段类型分析器
通过分析字段内容来判断是否为单位名称或地址字段
"""

import re
from typing import Dict, List, Any, Tuple, Optional
from loguru import logger

class ContentFieldAnalyzer:
    """基于内容的字段类型分析器"""
    
    def __init__(self):
        # 单位名称常见后缀
        self.company_suffixes = [
            '有限公司', '股份有限公司', '有限责任公司', '集团有限公司',
            '公司', '企业', '集团', '中心', '研究所', '研究院', '学院',
            '医院', '诊所', '养老院', '护理院', '康复中心', '服务中心',
            '工厂', '厂', '店', '商店', '超市', '市场', '银行',
            '合作社', '协会', '基金会', '事务所', '工作室', '部',
            'Co.', 'Ltd.', 'Inc.', 'Corp.', 'LLC', 'LLP'
        ]
        
        # 地址关键词
        self.address_keywords = [
            # 行政区划
            '省', '市', '区', '县', '镇', '乡', '街道', '村', '社区',
            # 道路类型
            '路', '街', '巷', '弄', '胡同', '大道', '大街', '小区',
            # 建筑类型
            '号', '栋', '楼', '层', '室', '单元', '幢', '座',
            # 方位词
            '东', '南', '西', '北', '中', '内', '外', '上', '下',
            # 特殊地标
            '广场', '公园', '商场', '大厦', '中心', '城', '园区', '基地'
        ]
        
        # 单位名称关键词（用于辅助判断）
        self.company_keywords = [
            '科技', '技术', '工程', '建设', '发展', '投资', '贸易',
            '制造', '生产', '加工', '销售', '服务', '咨询', '管理',
            '教育', '培训', '医疗', '健康', '养老', '物业', '保安',
            '清洁', '绿化', '装修', '设计', '广告', '传媒', '文化'
        ]
    
    def analyze_field_type(self, field_values: List[str], sample_size: int = 10) -> Dict[str, Any]:
        """
        分析字段类型
        
        Args:
            field_values: 字段值列表
            sample_size: 分析的样本数量
            
        Returns:
            Dict: 分析结果
        """
        if not field_values:
            return {
                'field_type': 'unknown',
                'confidence': 0.0,
                'analysis': '无数据'
            }
        
        # 取样本进行分析
        sample_values = [str(v).strip() for v in field_values[:sample_size] if v and str(v).strip()]
        
        if not sample_values:
            return {
                'field_type': 'unknown',
                'confidence': 0.0,
                'analysis': '无有效数据'
            }
        
        # 分析每个样本
        company_scores = []
        address_scores = []
        
        for value in sample_values:
            company_score = self._analyze_company_name(value)
            address_score = self._analyze_address(value)
            
            company_scores.append(company_score)
            address_scores.append(address_score)
        
        # 计算平均得分
        avg_company_score = sum(company_scores) / len(company_scores)
        avg_address_score = sum(address_scores) / len(address_scores)
        
        # 判断字段类型
        if avg_company_score > avg_address_score and avg_company_score > 0.3:
            field_type = 'company_name'
            confidence = avg_company_score
        elif avg_address_score > avg_company_score and avg_address_score > 0.3:
            field_type = 'address'
            confidence = avg_address_score
        else:
            field_type = 'unknown'
            confidence = max(avg_company_score, avg_address_score)
        
        return {
            'field_type': field_type,
            'confidence': confidence,
            'analysis': {
                'sample_count': len(sample_values),
                'avg_company_score': avg_company_score,
                'avg_address_score': avg_address_score,
                'samples': sample_values[:3]  # 显示前3个样本
            }
        }
    
    def _analyze_company_name(self, text: str) -> float:
        """
        分析文本是否为单位名称
        
        Args:
            text: 待分析文本
            
        Returns:
            float: 单位名称得分 (0-1)
        """
        if not text or len(text.strip()) < 2:
            return 0.0
        
        text = text.strip()
        score = 0.0
        
        # 1. 检查单位后缀（权重最高）
        suffix_score = 0.0
        for suffix in self.company_suffixes:
            if text.endswith(suffix):
                suffix_score = 0.8  # 有明确后缀，高分
                break
        
        # 2. 检查单位关键词
        keyword_score = 0.0
        keyword_count = 0
        for keyword in self.company_keywords:
            if keyword in text:
                keyword_count += 1
        
        if keyword_count > 0:
            keyword_score = min(0.4, keyword_count * 0.1)  # 最多0.4分
        
        # 3. 检查是否包含地址特征（负分）
        address_penalty = 0.0
        address_keyword_count = 0
        for keyword in self.address_keywords:
            if keyword in text:
                address_keyword_count += 1
        
        if address_keyword_count > 2:  # 如果包含多个地址关键词，可能是地址
            address_penalty = -0.3
        
        # 4. 长度和格式检查
        length_score = 0.0
        if 4 <= len(text) <= 50:  # 合理的单位名称长度
            length_score = 0.2
        elif len(text) > 50:  # 太长可能是地址
            length_score = -0.2
        
        # 5. 特殊字符检查
        special_char_penalty = 0.0
        if re.search(r'[0-9]{2,}', text):  # 包含多位数字，可能是地址
            special_char_penalty = -0.1
        
        # 综合得分
        total_score = suffix_score + keyword_score + address_penalty + length_score + special_char_penalty
        
        return max(0.0, min(1.0, total_score))
    
    def _analyze_address(self, text: str) -> float:
        """
        分析文本是否为地址
        
        Args:
            text: 待分析文本
            
        Returns:
            float: 地址得分 (0-1)
        """
        if not text or len(text.strip()) < 3:
            return 0.0
        
        text = text.strip()
        score = 0.0
        
        # 1. 检查地址关键词
        keyword_score = 0.0
        keyword_count = 0
        for keyword in self.address_keywords:
            if keyword in text:
                keyword_count += 1
        
        if keyword_count > 0:
            keyword_score = min(0.8, keyword_count * 0.15)  # 地址关键词越多得分越高
        
        # 2. 检查数字模式（门牌号等）
        number_score = 0.0
        # 匹配门牌号模式：数字+号
        if re.search(r'\d+号', text):
            number_score += 0.3
        # 匹配楼层模式：数字+楼/层
        if re.search(r'\d+[楼层]', text):
            number_score += 0.2
        # 匹配室号模式：数字+室
        if re.search(r'\d+室', text):
            number_score += 0.2
        
        # 3. 检查行政区划模式
        admin_score = 0.0
        # 省市区模式
        if re.search(r'[^市]+市.*[区县]', text):
            admin_score = 0.4
        elif re.search(r'[省市区县]', text):
            admin_score = 0.2
        
        # 4. 检查是否包含单位名称特征（负分）
        company_penalty = 0.0
        for suffix in self.company_suffixes:
            if suffix in text:
                company_penalty = -0.4  # 包含单位后缀，可能不是纯地址
                break
        
        # 5. 长度检查
        length_score = 0.0
        if 8 <= len(text) <= 100:  # 合理的地址长度
            length_score = 0.1
        elif len(text) < 8:  # 太短可能不是完整地址
            length_score = -0.2
        
        # 综合得分
        total_score = keyword_score + number_score + admin_score + company_penalty + length_score
        
        return max(0.0, min(1.0, total_score))
    
    def analyze_field_mapping(self, source_data: List[Dict], target_data: List[Dict], 
                            mappings: List[Dict]) -> Dict[str, Any]:
        """
        分析字段映射的类型
        
        Args:
            source_data: 源数据样本
            target_data: 目标数据样本
            mappings: 字段映射配置
            
        Returns:
            Dict: 分析结果
        """
        results = {}
        
        for mapping in mappings:
            source_field = mapping.get('source_field')
            target_field = mapping.get('target_field')
            
            # 分析源字段
            source_values = [record.get(source_field, '') for record in source_data if record.get(source_field)]
            source_analysis = self.analyze_field_type(source_values)
            
            # 分析目标字段
            target_values = [record.get(target_field, '') for record in target_data if record.get(target_field)]
            target_analysis = self.analyze_field_type(target_values)
            
            # 判断映射是否合理
            mapping_key = f"{source_field}->{target_field}"
            results[mapping_key] = {
                'source_analysis': source_analysis,
                'target_analysis': target_analysis,
                'mapping_reasonable': self._is_mapping_reasonable(source_analysis, target_analysis),
                'weight': mapping.get('weight', 1.0)
            }
        
        return results
    
    def _is_mapping_reasonable(self, source_analysis: Dict, target_analysis: Dict) -> Dict[str, Any]:
        """
        判断字段映射是否合理
        
        Args:
            source_analysis: 源字段分析结果
            target_analysis: 目标字段分析结果
            
        Returns:
            Dict: 合理性分析
        """
        source_type = source_analysis.get('field_type', 'unknown')
        target_type = target_analysis.get('field_type', 'unknown')
        
        if source_type == target_type and source_type != 'unknown':
            return {
                'reasonable': True,
                'confidence': min(source_analysis.get('confidence', 0), target_analysis.get('confidence', 0)),
                'reason': f'两个字段都被识别为{source_type}'
            }
        elif source_type == 'unknown' or target_type == 'unknown':
            return {
                'reasonable': None,
                'confidence': 0.0,
                'reason': '无法确定字段类型'
            }
        else:
            return {
                'reasonable': False,
                'confidence': 0.0,
                'reason': f'字段类型不匹配：源字段={source_type}, 目标字段={target_type}'
            }

# 使用示例
if __name__ == "__main__":
    analyzer = ContentFieldAnalyzer()
    
    # 测试单位名称识别
    company_names = [
        "上海朝阳高科技有限公司",
        "北京市第一人民医院",
        "中国建设银行股份有限公司",
        "天宝养老院"
    ]
    
    # 测试地址识别
    addresses = [
        "上海市虹口区天宝路881号",
        "北京市朝阳区建国路1号",
        "广东省深圳市南山区科技园南区"
    ]
    
    print("单位名称分析:")
    for name in company_names:
        result = analyzer.analyze_field_type([name])
        print(f"  {name}: {result['field_type']} (置信度: {result['confidence']:.2f})")
    
    print("\n地址分析:")
    for addr in addresses:
        result = analyzer.analyze_field_type([addr])
        print(f"  {addr}: {result['field_type']} (置信度: {result['confidence']:.2f})")
