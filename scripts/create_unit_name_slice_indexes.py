#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统 - 单位名称切片索引优化脚本
通过创建单位名称的N-gram切片索引，实现更快速的模糊匹配
"""

import pymongo
import re
import jieba
from datetime import datetime
import time
from collections import defaultdict
import json

class UnitNameSliceIndexer:
    def __init__(self):
        """初始化索引器"""
        self.client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.client['Unit_Info']
        self.stats = {
            'slice_indexes_created': 0,
            'keyword_indexes_created': 0,
            'processing_time': 0,
            'total_slices': 0
        }
        
        print("🔧 单位名称切片索引器初始化完成")
        
    def create_ngram_slices(self, text, n: int = 3) -> set:
        """创建N-gram切片"""
        # 确保text是字符串类型并且有效
        if not text:
            return set()
        
        # 转换为字符串并检查长度
        text_str = str(text).strip()
        if not text_str or len(text_str) < n:
            return set()
        
        # 清理文本
        clean_text = re.sub(r'[^\u4e00-\u9fff\w]', '', text_str)
        
        # 生成N-gram切片
        slices = set()
        for i in range(len(clean_text) - n + 1):
            slice_text = clean_text[i:i+n]
            if len(slice_text) == n:
                slices.add(slice_text)
        
        return slices
    
    def extract_keywords(self, text) -> set:
        """提取关键词"""
        if not text:
            return set()
        
        # 确保是字符串类型
        text_str = str(text).strip()
        if not text_str:
            return set()
        
        # 使用jieba分词
        words = jieba.cut(text_str)
        keywords = set()
        
        for word in words:
            word = word.strip()
            # 过滤掉长度小于2的词和常见停用词
            if len(word) >= 2 and word not in {'有限', '公司', '企业', '集团', '工厂', '商店', '中心'}:
                keywords.add(word)
        
        return keywords
    
    def create_slice_indexes_for_collection(self, collection_name: str, field_name: str):
        """为指定集合和字段创建切片索引"""
        print(f"\n📊 为 {collection_name}.{field_name} 创建切片索引...")
        
        collection = self.db[collection_name]
        slice_collection_name = f"{collection_name}_name_slices"
        keyword_collection_name = f"{collection_name}_name_keywords"
        
        # 创建切片索引集合
        slice_collection = self.db[slice_collection_name]
        keyword_collection = self.db[keyword_collection_name]
        
        # 清空现有索引
        slice_collection.drop()
        keyword_collection.drop()
        
        # 批量处理数据
        batch_size = 1000
        total_processed = 0
        slice_data = []
        keyword_data = []
        
        cursor = collection.find({field_name: {"$exists": True, "$ne": ""}}, {field_name: 1})
        
        for doc in cursor:
            unit_name = doc.get(field_name, '')
            if not unit_name:
                continue
            
            doc_id = doc['_id']
            
            # 生成3-gram切片
            slices_3 = self.create_ngram_slices(unit_name, 3)
            # 生成2-gram切片（用于短名称）
            slices_2 = self.create_ngram_slices(unit_name, 2)
            # 提取关键词
            keywords = self.extract_keywords(unit_name)
            
            # 准备切片数据
            for slice_text in slices_3.union(slices_2):
                slice_data.append({
                    'slice': slice_text,
                    'doc_id': doc_id,
                    'unit_name': str(unit_name),
                    'slice_type': '3gram' if slice_text in slices_3 else '2gram'
                })
            
            # 准备关键词数据
            for keyword in keywords:
                keyword_data.append({
                    'keyword': keyword,
                    'doc_id': doc_id,
                    'unit_name': str(unit_name)
                })
            
            total_processed += 1
            
            # 批量插入
            if len(slice_data) >= batch_size:
                if slice_data:
                    slice_collection.insert_many(slice_data)
                slice_data = []
                
            if len(keyword_data) >= batch_size:
                if keyword_data:
                    keyword_collection.insert_many(keyword_data)
                keyword_data = []
            
            if total_processed % 10000 == 0:
                print(f"   已处理: {total_processed:,} 条记录")
        
        # 插入剩余数据
        if slice_data:
            slice_collection.insert_many(slice_data)
        if keyword_data:
            keyword_collection.insert_many(keyword_data)
        
        # 创建索引
        print(f"   创建切片索引...")
        slice_collection.create_index([('slice', 1)])
        slice_collection.create_index([('slice', 1), ('slice_type', 1)])
        
        print(f"   创建关键词索引...")
        keyword_collection.create_index([('keyword', 1)])
        keyword_collection.create_index([('keyword', 'text')])
        
        # 统计信息
        slice_count = slice_collection.count_documents({})
        keyword_count = keyword_collection.count_documents({})
        
        print(f"   ✅ 完成 - 切片: {slice_count:,} 条, 关键词: {keyword_count:,} 条")
        
        self.stats['slice_indexes_created'] += 1
        self.stats['total_slices'] += slice_count
        
        return {
            'collection': collection_name,
            'field': field_name,
            'slice_count': slice_count,
            'keyword_count': keyword_count,
            'records_processed': total_processed
        }
    
    def create_fast_lookup_indexes(self):
        """创建快速查找索引"""
        print(f"\n🚀 创建快速查找索引...")
        
        # 为原始集合创建前缀索引
        collections_fields = [
            ('xfaqpc_jzdwxx', 'UNIT_NAME'),
            ('xxj_shdwjbxx', 'dwmc')
        ]
        
        for collection_name, field_name in collections_fields:
            collection = self.db[collection_name]
            
            try:
                # 创建前缀索引（用于快速前缀匹配）
                collection.create_index([(field_name, 1)])
                
                # 创建文本索引（用于全文搜索）
                collection.create_index([(field_name, 'text')], 
                                      default_language='none',  # 禁用语言处理以支持中文
                                      name=f"{field_name}_text_idx")
                
                print(f"   ✅ {collection_name}.{field_name} 索引创建完成")
                
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"   ⚠️ {collection_name}.{field_name} 索引创建警告: {e}")
    
    def create_similarity_cache_collection(self):
        """创建相似度缓存集合"""
        print(f"\n💾 创建相似度缓存集合...")
        
        cache_collection = self.db['unit_name_similarity_cache']
        
        # 创建缓存索引
        try:
            cache_collection.create_index([('source_name', 1), ('target_name', 1)], unique=True)
            cache_collection.create_index([('similarity_score', -1)])
            cache_collection.create_index([('created_time', 1)], expireAfterSeconds=7*24*3600)  # 7天过期
            
            print(f"   ✅ 相似度缓存集合创建完成")
            
        except Exception as e:
            if "already exists" not in str(e).lower():
                print(f"   ⚠️ 缓存集合创建警告: {e}")
    
    def test_slice_index_performance(self):
        """测试切片索引性能"""
        print(f"\n🧪 测试切片索引性能...")
        
        # 测试查询
        test_names = [
            "上海玛尔斯制冷设备有限公司",
            "上海由鹏资产管理有限公司", 
            "上海扬发金属制品有限公司"
        ]
        
        for test_name in test_names:
            print(f"\n   测试单位: {test_name}")
            
            # 测试3-gram切片查询
            slices = self.create_ngram_slices(test_name, 3)
            if slices:
                slice_collection = self.db['xxj_shdwjbxx_name_slices']
                
                start_time = time.time()
                results = list(slice_collection.find({'slice': {'$in': list(slices)}}).limit(10))
                query_time = time.time() - start_time
                
                print(f"     3-gram切片查询: {len(results)} 条结果, 耗时: {query_time:.4f}秒")
            
            # 测试关键词查询
            keywords = self.extract_keywords(test_name)
            if keywords:
                keyword_collection = self.db['xxj_shdwjbxx_name_keywords']
                
                start_time = time.time()
                results = list(keyword_collection.find({'keyword': {'$in': list(keywords)}}).limit(10))
                query_time = time.time() - start_time
                
                print(f"     关键词查询: {len(results)} 条结果, 耗时: {query_time:.4f}秒")
    
    def run_optimization(self):
        """运行完整优化"""
        start_time = time.time()
        
        print("🔥" * 80)
        print("🔥 消防单位建筑数据关联系统 - 单位名称切片索引优化")
        print("🔥 通过N-gram切片和关键词索引实现超快速模糊匹配")
        print("🔥" * 80)
        
        results = []
        
        # 1. 创建切片索引
        print("\n📊 第1步: 创建单位名称切片索引")
        collections_fields = [
            ('xfaqpc_jzdwxx', 'UNIT_NAME'),
            ('xxj_shdwjbxx', 'dwmc')
        ]
        
        for collection_name, field_name in collections_fields:
            result = self.create_slice_indexes_for_collection(collection_name, field_name)
            results.append(result)
        
        # 2. 创建快速查找索引
        print("\n📊 第2步: 创建快速查找索引")
        self.create_fast_lookup_indexes()
        
        # 3. 创建相似度缓存
        print("\n📊 第3步: 创建相似度缓存集合")
        self.create_similarity_cache_collection()
        
        # 4. 测试性能
        print("\n📊 第4步: 测试索引性能")
        self.test_slice_index_performance()
        
        # 统计结果
        self.stats['processing_time'] = time.time() - start_time
        
        print(f"\n🎉 单位名称切片索引优化完成!")
        print(f"📊 优化统计:")
        print(f"   - 切片索引集合: {self.stats['slice_indexes_created']} 个")
        print(f"   - 总切片数量: {self.stats['total_slices']:,} 条")
        print(f"   - 处理时间: {self.stats['processing_time']:.2f} 秒")
        
        # 保存优化报告
        report = {
            'optimization_time': datetime.now().isoformat(),
            'stats': self.stats,
            'results': results,
            'collections_created': [
                'xfaqpc_jzdwxx_name_slices',
                'xfaqpc_jzdwxx_name_keywords',
                'xxj_shdwjbxx_name_slices', 
                'xxj_shdwjbxx_name_keywords',
                'unit_name_similarity_cache'
            ]
        }
        
        with open('unit_name_slice_index_optimization_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"📄 优化报告已保存: unit_name_slice_index_optimization_report.json")
        
        return results

def main():
    """主函数"""
    indexer = UnitNameSliceIndexer()
    return indexer.run_optimization()

if __name__ == "__main__":
    main() 