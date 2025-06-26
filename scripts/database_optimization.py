#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
消防单位建筑数据关联系统数据库优化脚本
创建必要的索引，优化查询性能，解决处理速度瓶颈
"""
import sys
import os
import time
from datetime import datetime
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

class DatabaseOptimizer:
    def __init__(self):
        self.client = None
        self.db = None
        self.connect_to_database()
        
    def print_header(self, title):
        """打印标题"""
        print("\n" + "=" * 80)
        print(f"🗄️ {title}")
        print("=" * 80)
    
    def connect_to_database(self):
        """连接到数据库"""
        try:
            self.client = MongoClient('mongodb://localhost:27017/')
            self.db = self.client['Unit_Info']
            print(f"✅ 数据库连接成功")
        except Exception as e:
            print(f"❌ 数据库连接失败: {str(e)}")
            return False
        return True
    
    def analyze_collections(self):
        """分析集合结构"""
        print(f"🔍 分析数据库集合结构...")
        
        collections = self.db.list_collection_names()
        print(f"   发现集合: {collections}")
        
        collection_stats = {}
        for collection_name in collections:
            try:
                collection = self.db[collection_name]
                count = collection.count_documents({})
                
                # 获取样本文档
                sample = collection.find_one()
                fields = list(sample.keys()) if sample else []
                
                collection_stats[collection_name] = {
                    'count': count,
                    'fields': fields
                }
                
                print(f"   {collection_name}: {count:,} 条记录")
                print(f"     字段: {fields[:10]}{'...' if len(fields) > 10 else ''}")
                
            except Exception as e:
                print(f"   {collection_name}: 分析失败 - {str(e)}")
        
        return collection_stats
    
    def check_existing_indexes(self):
        """检查现有索引"""
        print(f"🔍 检查现有索引...")
        
        collections = ['supervision_data', 'inspection_data', 'match_results']
        existing_indexes = {}
        
        for collection_name in collections:
            try:
                collection = self.db[collection_name]
                indexes = list(collection.list_indexes())
                existing_indexes[collection_name] = indexes
                
                print(f"   {collection_name} 现有索引:")
                for idx in indexes:
                    index_name = idx.get('name', 'unknown')
                    index_keys = idx.get('key', {})
                    print(f"     - {index_name}: {dict(index_keys)}")
                    
            except Exception as e:
                print(f"   {collection_name}: 检查失败 - {str(e)}")
        
        return existing_indexes
    
    def create_performance_indexes(self):
        """创建性能优化索引"""
        print(f"🚀 创建性能优化索引...")
        
        # 监督管理数据索引
        supervision_indexes = [
            # 单位名称索引（最重要）
            {'keys': [('unit_name', ASCENDING)], 'name': 'idx_unit_name'},
            {'keys': [('unit_name', TEXT)], 'name': 'idx_unit_name_text'},
            
            # 地址相关索引
            {'keys': [('address', ASCENDING)], 'name': 'idx_address'},
            {'keys': [('address', TEXT)], 'name': 'idx_address_text'},
            
            # 复合索引
            {'keys': [('unit_name', ASCENDING), ('address', ASCENDING)], 'name': 'idx_unit_address'},
            
            # 状态和时间索引
            {'keys': [('status', ASCENDING)], 'name': 'idx_status'},
            {'keys': [('created_at', DESCENDING)], 'name': 'idx_created_at'},
            
            # 匹配状态索引
            {'keys': [('is_matched', ASCENDING)], 'name': 'idx_is_matched'},
        ]
        
        # 安全排查数据索引
        inspection_indexes = [
            # 单位名称索引（最重要）
            {'keys': [('unit_name', ASCENDING)], 'name': 'idx_unit_name'},
            {'keys': [('unit_name', TEXT)], 'name': 'idx_unit_name_text'},
            
            # 地址相关索引
            {'keys': [('address', ASCENDING)], 'name': 'idx_address'},
            {'keys': [('address', TEXT)], 'name': 'idx_address_text'},
            
            # 复合索引
            {'keys': [('unit_name', ASCENDING), ('address', ASCENDING)], 'name': 'idx_unit_address'},
            
            # 检查时间索引
            {'keys': [('inspection_date', DESCENDING)], 'name': 'idx_inspection_date'},
            
            # 匹配状态索引
            {'keys': [('is_matched', ASCENDING)], 'name': 'idx_is_matched'},
        ]
        
        # 匹配结果索引
        match_result_indexes = [
            # 源数据ID索引
            {'keys': [('supervision_id', ASCENDING)], 'name': 'idx_supervision_id'},
            {'keys': [('inspection_id', ASCENDING)], 'name': 'idx_inspection_id'},
            
            # 复合索引
            {'keys': [('supervision_id', ASCENDING), ('inspection_id', ASCENDING)], 'name': 'idx_match_pair'},
            
            # 匹配分数索引
            {'keys': [('match_score', DESCENDING)], 'name': 'idx_match_score'},
            
            # 创建时间索引
            {'keys': [('created_at', DESCENDING)], 'name': 'idx_created_at'},
        ]
        
        # 创建索引
        index_results = {}
        
        # 监督管理数据索引
        index_results['supervision_data'] = self.create_indexes_for_collection(
            'supervision_data', supervision_indexes
        )
        
        # 安全排查数据索引
        index_results['inspection_data'] = self.create_indexes_for_collection(
            'inspection_data', inspection_indexes
        )
        
        # 匹配结果索引
        index_results['match_results'] = self.create_indexes_for_collection(
            'match_results', match_result_indexes
        )
        
        return index_results
    
    def create_indexes_for_collection(self, collection_name, indexes):
        """为指定集合创建索引"""
        print(f"   创建 {collection_name} 索引...")
        
        try:
            collection = self.db[collection_name]
            results = []
            
            for index_def in indexes:
                try:
                    keys = index_def['keys']
                    name = index_def['name']
                    
                    # 检查索引是否已存在
                    existing_indexes = [idx['name'] for idx in collection.list_indexes()]
                    
                    if name in existing_indexes:
                        print(f"     ✅ 索引已存在: {name}")
                        results.append({'name': name, 'status': 'exists'})
                        continue
                    
                    # 创建索引
                    start_time = time.time()
                    collection.create_index(keys, name=name, background=True)
                    elapsed = time.time() - start_time
                    
                    print(f"     ✅ 索引创建成功: {name} ({elapsed:.2f}秒)")
                    results.append({'name': name, 'status': 'created', 'time': elapsed})
                    
                except Exception as e:
                    print(f"     ❌ 索引创建失败: {index_def['name']} - {str(e)}")
                    results.append({'name': index_def['name'], 'status': 'failed', 'error': str(e)})
            
            return results
            
        except Exception as e:
            print(f"   ❌ 集合 {collection_name} 索引创建失败: {str(e)}")
            return []
    
    def optimize_query_performance(self):
        """优化查询性能"""
        print(f"🔧 优化查询性能...")
        
        # 设置数据库参数
        optimizations = []
        
        try:
            # 设置读偏好
            self.db.read_preference = pymongo.ReadPreference.SECONDARY_PREFERRED
            optimizations.append("设置读偏好为SECONDARY_PREFERRED")
            
            # 设置写关注
            self.db.write_concern = pymongo.WriteConcern(w=1, j=False)
            optimizations.append("设置写关注为w=1, j=False")
            
            print(f"   ✅ 查询优化完成:")
            for opt in optimizations:
                print(f"     - {opt}")
                
        except Exception as e:
            print(f"   ❌ 查询优化失败: {str(e)}")
    
    def test_query_performance(self):
        """测试查询性能"""
        print(f"📊 测试查询性能...")
        
        test_queries = [
            {
                'name': '单位名称查询',
                'collection': 'supervision_data',
                'query': {'unit_name': {'$regex': '上海', '$options': 'i'}},
                'limit': 100
            },
            {
                'name': '地址查询',
                'collection': 'supervision_data', 
                'query': {'address': {'$regex': '浦东', '$options': 'i'}},
                'limit': 100
            },
            {
                'name': '复合查询',
                'collection': 'supervision_data',
                'query': {'unit_name': {'$exists': True}, 'address': {'$exists': True}},
                'limit': 100
            },
            {
                'name': '匹配状态查询',
                'collection': 'supervision_data',
                'query': {'is_matched': {'$ne': True}},
                'limit': 1000
            }
        ]
        
        performance_results = []
        
        for test in test_queries:
            try:
                collection = self.db[test['collection']]
                
                # 执行查询并计时
                start_time = time.time()
                cursor = collection.find(test['query']).limit(test['limit'])
                results = list(cursor)
                elapsed = time.time() - start_time
                
                result = {
                    'name': test['name'],
                    'elapsed': elapsed,
                    'count': len(results),
                    'speed': len(results) / elapsed if elapsed > 0 else 0
                }
                
                performance_results.append(result)
                
                print(f"   {test['name']}: {len(results)} 条记录, {elapsed:.3f}秒, {result['speed']:.1f} 记录/秒")
                
            except Exception as e:
                print(f"   {test['name']}: 测试失败 - {str(e)}")
        
        return performance_results
    
    def generate_optimization_report(self, index_results, performance_results):
        """生成优化报告"""
        print(f"\n📊 数据库优化报告:")
        
        # 索引创建统计
        total_indexes = 0
        created_indexes = 0
        existing_indexes = 0
        failed_indexes = 0
        
        for collection, results in index_results.items():
            for result in results:
                total_indexes += 1
                if result['status'] == 'created':
                    created_indexes += 1
                elif result['status'] == 'exists':
                    existing_indexes += 1
                elif result['status'] == 'failed':
                    failed_indexes += 1
        
        print(f"   索引优化:")
        print(f"     总索引数: {total_indexes}")
        print(f"     新创建: {created_indexes}")
        print(f"     已存在: {existing_indexes}")
        print(f"     创建失败: {failed_indexes}")
        
        # 查询性能统计
        if performance_results:
            avg_speed = sum(r['speed'] for r in performance_results) / len(performance_results)
            max_speed = max(r['speed'] for r in performance_results)
            min_speed = min(r['speed'] for r in performance_results)
            
            print(f"   查询性能:")
            print(f"     平均速度: {avg_speed:.1f} 记录/秒")
            print(f"     最高速度: {max_speed:.1f} 记录/秒")
            print(f"     最低速度: {min_speed:.1f} 记录/秒")
        
        # 优化建议
        print(f"   优化建议:")
        if created_indexes > 0:
            print(f"     ✅ 已创建 {created_indexes} 个新索引，查询性能应有显著提升")
        if failed_indexes > 0:
            print(f"     ⚠️ {failed_indexes} 个索引创建失败，需要检查")
        print(f"     💡 建议重新启动匹配任务以验证性能提升")
    
    def run_database_optimization(self):
        """运行数据库优化"""
        self.print_header("消防单位建筑数据关联系统 - 数据库优化")
        
        print(f"🕒 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 目标: 创建必要索引，优化数据库查询性能")
        
        if not self.client:
            print(f"❌ 数据库连接失败，无法进行优化")
            return False
        
        # 分析集合结构
        collection_stats = self.analyze_collections()
        
        # 检查现有索引
        existing_indexes = self.check_existing_indexes()
        
        # 创建性能索引
        index_results = self.create_performance_indexes()
        
        # 优化查询性能
        self.optimize_query_performance()
        
        # 测试查询性能
        performance_results = self.test_query_performance()
        
        # 生成优化报告
        self.generate_optimization_report(index_results, performance_results)
        
        self.print_header("数据库优化完成")
        print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ 数据库优化执行成功")
        print(f"💡 建议重新启动匹配任务以验证性能提升")
        
        return True

def main():
    """主函数"""
    optimizer = DatabaseOptimizer()
    optimizer.run_database_optimization()

if __name__ == "__main__":
    main() 