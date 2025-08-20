#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
地址字段切片索引创建脚本
为hztj_hzxx和dwd_yljgxx表的地址字段创建N-gram切片索引，实现高效地址匹配
"""

import pymongo
import re
import time
from datetime import datetime

class AddressSliceIndexer:
    def __init__(self):
        """初始化地址索引器"""
        self.client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.client['Unit_Info']
        self.stats = {
            'slice_indexes_created': 0,
            'keyword_indexes_created': 0,
            'processing_time': 0,
            'total_slices': 0
        }
        
        print("🔧 地址切片索引器初始化完成")
        
    def extract_address_keywords(self, address: str) -> set:
        """提取地址关键词"""
        if not address:
            return set()
        
        address_str = str(address).strip()
        keywords = set()
        
        # 1. 提取门牌号（数字+号）
        door_numbers = re.findall(r'\d+号', address_str)
        keywords.update(door_numbers)
        
        # 2. 提取街道路名
        street_patterns = [
            r'[\u4e00-\u9fff]+路\d*号?',  # XX路
            r'[\u4e00-\u9fff]+街\d*号?',  # XX街  
            r'[\u4e00-\u9fff]+道\d*号?',  # XX道
            r'[\u4e00-\u9fff]+大道\d*号?', # XX大道
        ]
        for pattern in street_patterns:
            streets = re.findall(pattern, address_str)
            keywords.update(streets)
        
        # 3. 提取区县
        district_pattern = r'[\u4e00-\u9fff]+区'
        districts = re.findall(district_pattern, address_str)
        keywords.update(districts)
        
        # 4. 提取建筑物名称（包含特定后缀的词）
        building_patterns = [
            r'[\u4e00-\u9fff]+大厦',
            r'[\u4e00-\u9fff]+大楼', 
            r'[\u4e00-\u9fff]+中心',
            r'[\u4e00-\u9fff]+广场',
            r'[\u4e00-\u9fff]+养老院',
            r'[\u4e00-\u9fff]+医院',
            r'[\u4e00-\u9fff]+学校',
        ]
        for pattern in building_patterns:
            buildings = re.findall(pattern, address_str)
            keywords.update(buildings)
        
        # 5. 提取连续的地址片段（3-6个字符）
        clean_address = re.sub(r'[^\u4e00-\u9fff\d]', '', address_str)
        for i in range(len(clean_address) - 2):
            for length in [3, 4, 5, 6]:
                if i + length <= len(clean_address):
                    segment = clean_address[i:i+length]
                    if len(segment) >= 3:
                        keywords.add(segment)
        
        # 过滤掉太短或太常见的关键词
        filtered_keywords = set()
        common_words = {'上海市', '市辖区', '中国', '有限公司', '股份'}
        
        for keyword in keywords:
            if len(keyword) >= 3 and keyword not in common_words:
                filtered_keywords.add(keyword)
        
        return filtered_keywords
    
    def create_address_indexes_for_table(self, table_name: str, address_field: str):
        """为指定表的地址字段创建索引"""
        print(f"\n📍 为表 {table_name} 的字段 {address_field} 创建地址索引...")
        
        start_time = time.time()
        
        # 获取源表
        collection = self.db[table_name]
        
        # 创建索引表名
        slice_collection_name = f"{table_name}_address_slices"
        keyword_collection_name = f"{table_name}_address_keywords"
        
        # 清空该字段的现有索引（而不是整个表）
        keyword_collection = self.db[keyword_collection_name]
        keyword_collection.delete_many({"table_name": table_name, "field_name": address_field})
        
        slice_collection = self.db[slice_collection_name]
        keyword_collection = self.db[keyword_collection_name]
        
        # 批量处理数据
        batch_size = 1000
        slice_data = []
        keyword_data = []
        total_processed = 0
        
        print(f"   正在处理地址数据...")
        
        # 查询有地址的记录
        query = {address_field: {"$exists": True, "$ne": ""}}
        total_records = collection.count_documents(query)
        print(f"   表中 {address_field} 字段有数据的记录数: {total_records}")
        
        if total_records == 0:
            print(f"   ⚠️  字段 {address_field} 没有数据，跳过")
            return {'processed_records': 0, 'keywords_created': 0, 'processing_time': 0}
        
        cursor = collection.find(query, {address_field: 1})
        
        for doc in cursor:
            address = doc.get(address_field, '')
            if not address:
                continue
            
            doc_id = doc['_id']
            
            # 提取地址关键词
            keywords = self.extract_address_keywords(address)
            
            # 准备关键词数据
            for keyword in keywords:
                keyword_data.append({
                    'keyword': keyword,
                    'doc_id': doc_id,
                    'address': str(address),
                    'table_name': table_name,
                    'field_name': address_field
                })
            
            total_processed += 1
            
            # 批量插入
            if len(keyword_data) >= batch_size:
                if keyword_data:
                    keyword_collection.insert_many(keyword_data)
                keyword_data = []
            
            if total_processed % 1000 == 0:
                print(f"   已处理: {total_processed:,} 条记录")
        
        # 插入剩余数据
        if keyword_data:
            keyword_collection.insert_many(keyword_data)
        
        # 创建索引
        print(f"   创建关键词索引...")
        keyword_collection.create_index([('keyword', 1)])
        keyword_collection.create_index([('doc_id', 1)])
        keyword_collection.create_index([('table_name', 1)])
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # 统计结果
        keyword_count = keyword_collection.count_documents({})
        
        print(f"   ✅ 完成！")
        print(f"   处理记录数: {total_processed:,}")
        print(f"   生成关键词: {keyword_count:,}")
        print(f"   处理时间: {processing_time:.2f}秒")
        
        self.stats['keyword_indexes_created'] += keyword_count
        self.stats['processing_time'] += processing_time
        
        return {
            'processed_records': total_processed,
            'keywords_created': keyword_count,
            'processing_time': processing_time
        }
    
    def create_all_address_indexes(self):
        """为所有相关表创建地址索引"""
        print("🚀 开始创建地址切片索引...")
        
        # 定义要处理的表和字段
        address_tables = [
            ('hztj_hzxx', '起火地点'),  # 源表：火灾统计信息
            ('dwd_yljgxx', 'ZCDZ'),    # 目标表：养老机构信息的注册地址
            ('dwd_yljgxx', 'FWCS_DZ'), # 目标表：养老机构信息的服务场所地址
            ('dwd_yljgxx', 'NSYLJGDZ'), # 目标表：养老机构信息的内设养老机构地址
        ]
        
        total_start_time = time.time()
        
        for table_name, address_field in address_tables:
            try:
                result = self.create_address_indexes_for_table(table_name, address_field)
                print(f"   {table_name}.{address_field}: {result['keywords_created']} 个关键词")
            except Exception as e:
                print(f"   ❌ {table_name}.{address_field} 处理失败: {e}")
        
        total_end_time = time.time()
        total_time = total_end_time - total_start_time
        
        print(f"\n🎉 地址索引创建完成！")
        print(f"总处理时间: {total_time:.2f}秒")
        print(f"总关键词数: {self.stats['keyword_indexes_created']:,}")
        
    def close(self):
        """关闭连接"""
        self.client.close()

def main():
    """主函数"""
    indexer = AddressSliceIndexer()
    
    try:
        indexer.create_all_address_indexes()
    except Exception as e:
        print(f"❌ 创建地址索引失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        indexer.close()

if __name__ == "__main__":
    main()
