#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据匹配分析脚本
分析当前匹配情况，识别优化机会
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymongo
import pandas as pd
import re
from collections import Counter, defaultdict
from datetime import datetime
import jieba
from difflib import SequenceMatcher

class MatchDataAnalyzer:
    def __init__(self):
        """初始化分析器"""
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["Unit_Info"]
        
        # 集合引用
        self.supervision_collection = self.db["xxj_shdwjbxx"]  # 监督管理系统
        self.inspection_collection = self.db["xfaqpc_jzdwxx"]  # 安全排查系统
        self.match_collection = self.db["unit_match_results"]  # 匹配结果
        
        print("🔍 数据匹配分析器初始化完成")
    
    def get_basic_stats(self):
        """获取基础统计信息"""
        print("\n" + "="*60)
        print("📊 基础数据统计")
        print("="*60)
        
        # 数据源统计
        supervision_count = self.supervision_collection.count_documents({})
        inspection_count = self.inspection_collection.count_documents({})
        match_count = self.match_collection.count_documents({})
        
        print(f"监督管理系统: {supervision_count:,} 条")
        print(f"安全排查系统: {inspection_count:,} 条")
        print(f"匹配结果: {match_count:,} 条")
        
        # 匹配率计算
        total_records = supervision_count + inspection_count
        match_rate = (match_count / total_records) * 100 if total_records > 0 else 0
        print(f"总记录数: {total_records:,} 条")
        print(f"匹配率: {match_rate:.4f}%")
        
        return {
            'supervision_count': supervision_count,
            'inspection_count': inspection_count,
            'match_count': match_count,
            'total_records': total_records,
            'match_rate': match_rate
        }
    
    def analyze_match_types(self):
        """分析匹配类型分布"""
        print("\n" + "="*60)
        print("🎯 匹配类型分析")
        print("="*60)
        
        # 匹配类型统计
        pipeline = [
            {"$group": {
                "_id": "$match_type",
                "count": {"$sum": 1},
                "avg_score": {"$avg": "$similarity_score"}
            }},
            {"$sort": {"count": -1}}
        ]
        
        match_types = list(self.match_collection.aggregate(pipeline))
        
        for match_type in match_types:
            match_type_name = match_type["_id"] or "未知"
            count = match_type["count"]
            avg_score = match_type["avg_score"] or 0
            print(f"{match_type_name}: {count} 条 (平均相似度: {avg_score:.3f})")
        
        return match_types
    
    def analyze_similarity_distribution(self):
        """分析相似度分布"""
        print("\n" + "="*60)
        print("📈 相似度分布分析")
        print("="*60)
        
        # 相似度区间统计
        pipeline = [
            {"$bucket": {
                "groupBy": "$similarity_score",
                "boundaries": [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                "default": "其他",
                "output": {
                    "count": {"$sum": 1},
                    "examples": {"$push": {
                        "primary_unit_name": "$primary_unit_name",
                        "matched_unit_name": "$matched_unit_name",
                        "score": "$similarity_score"
                    }}
                }
            }}
        ]
        
        similarity_dist = list(self.match_collection.aggregate(pipeline))
        
        for bucket in similarity_dist:
            range_start = bucket["_id"]
            count = bucket["count"]
            if isinstance(range_start, (int, float)):
                range_end = range_start + 0.1
                print(f"相似度 {range_start:.1f}-{range_end:.1f}: {count} 条")
            else:
                print(f"{range_start}: {count} 条")
        
        return similarity_dist
    
    def analyze_unit_name_patterns(self):
        """分析单位名称模式"""
        print("\n" + "="*60)
        print("🏢 单位名称模式分析")
        print("="*60)
        
        # 分析监督管理系统的单位名称
        print("监督管理系统单位名称特征:")
        supervision_sample = list(self.supervision_collection.find({}, {"dwmc": 1}).limit(1000))
        self._analyze_name_patterns(supervision_sample, "dwmc")
        
        print("\n安全排查系统单位名称特征:")
        inspection_sample = list(self.inspection_collection.find({}, {"UNIT_NAME": 1}).limit(1000))
        self._analyze_name_patterns(inspection_sample, "UNIT_NAME")
    
    def _analyze_name_patterns(self, data, field_name):
        """分析名称模式的辅助方法"""
        names = [item.get(field_name, "") for item in data if item.get(field_name)]
        
        # 长度分布
        lengths = [len(name) for name in names]
        avg_length = sum(lengths) / len(lengths) if lengths else 0
        print(f"  平均长度: {avg_length:.1f} 字符")
        print(f"  长度范围: {min(lengths) if lengths else 0} - {max(lengths) if lengths else 0}")
        
        # 常见后缀
        suffixes = []
        for name in names:
            if len(name) > 2:
                suffixes.append(name[-2:])
                if len(name) > 3:
                    suffixes.append(name[-3:])
        
        suffix_counter = Counter(suffixes)
        print("  常见后缀:")
        for suffix, count in suffix_counter.most_common(10):
            print(f"    {suffix}: {count} 次")
        
        # 常见关键词
        keywords = []
        for name in names:
            # 使用jieba分词
            words = jieba.lcut(name)
            keywords.extend([word for word in words if len(word) > 1])
        
        keyword_counter = Counter(keywords)
        print("  常见关键词:")
        for keyword, count in keyword_counter.most_common(10):
            print(f"    {keyword}: {count} 次")
    
    def analyze_unmatched_data(self):
        """分析未匹配数据"""
        print("\n" + "="*60)
        print("❌ 未匹配数据分析")
        print("="*60)
        
        # 获取已匹配的单位名称
        matched_supervision = set()
        matched_inspection = set()
        
        for match in self.match_collection.find({}):
            if match.get("primary_unit_name"):
                matched_supervision.add(match["primary_unit_name"])
            if match.get("matched_unit_name"):
                matched_inspection.add(match["matched_unit_name"])
        
        print(f"已匹配监督管理系统单位: {len(matched_supervision)} 个")
        print(f"已匹配安全排查系统单位: {len(matched_inspection)} 个")
        
        # 分析未匹配的数据样本
        print("\n未匹配监督管理系统单位样本:")
        unmatched_supervision = list(self.supervision_collection.find({
            "dwmc": {"$nin": list(matched_supervision)}
        }, {"dwmc": 1}).limit(10))
        
        for item in unmatched_supervision:
            print(f"  {item.get('dwmc', 'N/A')}")
        
        print("\n未匹配安全排查系统单位样本:")
        unmatched_inspection = list(self.inspection_collection.find({
            "UNIT_NAME": {"$nin": list(matched_inspection)}
        }, {"UNIT_NAME": 1}).limit(10))
        
        for item in unmatched_inspection:
            print(f"  {item.get('UNIT_NAME', 'N/A')}")
    
    def find_potential_matches(self):
        """寻找潜在匹配"""
        print("\n" + "="*60)
        print("🔍 潜在匹配分析")
        print("="*60)
        
        # 获取样本数据进行分析
        supervision_sample = list(self.supervision_collection.find({}, {"dwmc": 1}).limit(100))
        inspection_sample = list(self.inspection_collection.find({}, {"UNIT_NAME": 1}).limit(100))
        
        potential_matches = []
        
        for sup_item in supervision_sample:
            sup_name = sup_item.get("dwmc", "")
            if not sup_name:
                continue
                
            for ins_item in inspection_sample:
                ins_name = ins_item.get("UNIT_NAME", "")
                if not ins_name:
                    continue
                
                # 计算相似度
                similarity = self._calculate_similarity(sup_name, ins_name)
                
                if similarity > 0.6:  # 相似度阈值
                    potential_matches.append({
                        'supervision_name': sup_name,
                        'inspection_name': ins_name,
                        'similarity': similarity
                    })
        
        # 排序并显示潜在匹配
        potential_matches.sort(key=lambda x: x['similarity'], reverse=True)
        
        print(f"发现 {len(potential_matches)} 个潜在匹配 (相似度 > 0.6):")
        for i, match in enumerate(potential_matches[:10]):
            print(f"  {i+1}. 相似度: {match['similarity']:.3f}")
            print(f"     监督: {match['supervision_name']}")
            print(f"     排查: {match['inspection_name']}")
            print()
        
        return potential_matches
    
    def _calculate_similarity(self, name1, name2):
        """计算两个名称的相似度"""
        if not name1 or not name2:
            return 0.0
        
        # 使用SequenceMatcher计算相似度
        return SequenceMatcher(None, name1, name2).ratio()
    
    def analyze_data_quality(self):
        """分析数据质量"""
        print("\n" + "="*60)
        print("🔍 数据质量分析")
        print("="*60)
        
        # 监督管理系统数据质量
        print("监督管理系统数据质量:")
        supervision_stats = self._analyze_collection_quality(self.supervision_collection)
        
        print("\n安全排查系统数据质量:")
        inspection_stats = self._analyze_collection_quality(self.inspection_collection)
        
        return {
            'supervision': supervision_stats,
            'inspection': inspection_stats
        }
    
    def _analyze_collection_quality(self, collection):
        """分析集合数据质量的辅助方法"""
        total_count = collection.count_documents({})
        
        # 空值统计 (根据集合类型使用不同字段)
        if collection.name == "xxj_shdwjbxx":
            empty_unit_name = collection.count_documents({"dwmc": {"$in": ["", None]}})
            empty_address = collection.count_documents({"dwdz": {"$in": ["", None]}})
        else:
            empty_unit_name = collection.count_documents({"UNIT_NAME": {"$in": ["", None]}})
            empty_address = collection.count_documents({"ADDRESS": {"$in": ["", None]}})
        
        print(f"  总记录数: {total_count:,}")
        print(f"  空单位名称: {empty_unit_name:,} ({empty_unit_name/total_count*100:.2f}%)")
        print(f"  空地址: {empty_address:,} ({empty_address/total_count*100:.2f}%)")
        
        # 重复数据统计
        unit_field = "dwmc" if collection.name == "xxj_shdwjbxx" else "UNIT_NAME"
        pipeline = [
            {"$group": {
                "_id": f"${unit_field}",
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "duplicate_names"}
        ]
        
        duplicate_result = list(collection.aggregate(pipeline))
        duplicate_count = duplicate_result[0]["duplicate_names"] if duplicate_result else 0
        print(f"  重复单位名称: {duplicate_count:,}")
        
        return {
            'total_count': total_count,
            'empty_unit_name': empty_unit_name,
            'empty_address': empty_address,
            'duplicate_count': duplicate_count
        }
    
    def generate_recommendations(self):
        """生成优化建议"""
        print("\n" + "="*60)
        print("💡 优化建议")
        print("="*60)
        
        recommendations = [
            "1. 数据预处理优化:",
            "   - 清理空值和无效数据",
            "   - 标准化单位名称格式",
            "   - 处理重复数据",
            "",
            "2. 匹配算法改进:",
            "   - 实现模糊匹配算法",
            "   - 添加地址信息匹配",
            "   - 使用多字段组合匹配",
            "",
            "3. 相似度计算优化:",
            "   - 集成多种相似度算法",
            "   - 根据业务特点调整权重",
            "   - 添加语义相似度计算",
            "",
            "4. 性能优化:",
            "   - 创建适当的数据库索引",
            "   - 实现分批处理机制",
            "   - 添加缓存策略",
            "",
            "5. 监控和评估:",
            "   - 建立匹配质量评估体系",
            "   - 实现实时监控",
            "   - 定期评估和调优"
        ]
        
        for rec in recommendations:
            print(rec)
    
    def run_full_analysis(self):
        """运行完整分析"""
        print("🔥 消防单位建筑数据关联系统 - 数据匹配分析")
        print("="*60)
        print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 执行各项分析
            basic_stats = self.get_basic_stats()
            match_types = self.analyze_match_types()
            similarity_dist = self.analyze_similarity_distribution()
            self.analyze_unit_name_patterns()
            self.analyze_unmatched_data()
            potential_matches = self.find_potential_matches()
            quality_stats = self.analyze_data_quality()
            self.generate_recommendations()
            
            print("\n" + "="*60)
            print("✅ 分析完成")
            print("="*60)
            print(f"基础匹配率: {basic_stats['match_rate']:.4f}%")
            print(f"发现潜在匹配: {len(potential_matches)} 个")
            print("详细建议请参考上述分析结果")
            
        except Exception as e:
            print(f"❌ 分析过程中出现错误: {e}")
        finally:
            self.client.close()

def main():
    """主函数"""
    analyzer = MatchDataAnalyzer()
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main() 