#!/usr/bin/env python3
"""
重建知识图谱
清空现有数据并使用优化后的关系抽取器重新构建
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
from src.knowledge_graph.falkordb_store import FalkorDBStore
from src.knowledge_graph.kg_builder import KnowledgeGraphBuilder
import pandas as pd
from datetime import datetime

def clear_existing_data():
    """清空现有的知识图谱数据"""
    print("=== 清空现有知识图谱数据 ===")
    
    try:
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        
        # 获取清空前的统计
        stats_before = falkor_store.get_statistics()
        print(f"清空前统计: 实体={stats_before.get('total_entities', 0)}, 关系={stats_before.get('total_relations', 0)}")
        
        # 删除所有节点和关系
        print("正在清空数据...")
        result = falkor_store.graph.query('MATCH (n) DETACH DELETE n')
        print("✅ 成功清空所有数据")
        
        # 获取清空后的统计
        stats_after = falkor_store.get_statistics()
        print(f"清空后统计: 实体={stats_after.get('total_entities', 0)}, 关系={stats_after.get('total_relations', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ 清空数据失败: {e}")
        return False

def rebuild_knowledge_graph():
    """重新构建知识图谱"""
    print("\n=== 重新构建知识图谱 ===")
    
    # 初始化配置
    config_manager = ConfigManager()
    db_config = config_manager.get_database_config()
    db_manager = DatabaseManager(db_config)
    
    # 初始化FalkorDB存储和构建器
    falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph', project_name='optimized_rebuild')
    kg_builder = KnowledgeGraphBuilder(falkor_store)
    
    # 测试表列表
    test_tables = ['dwd_yljgxx', 'dwd_zzzhzj']
    
    total_entities = 0
    total_relations = 0
    
    for table_name in test_tables:
        print(f"\n--- 构建表: {table_name} ---")
        
        try:
            collection = db_manager.get_collection(table_name)
            # 获取更多数据进行测试（50条）
            sample_data = list(collection.find({}).limit(50))
            
            if not sample_data:
                print(f"  {table_name} 表中没有数据，跳过")
                continue
            
            df = pd.DataFrame(sample_data)
            print(f"  数据形状: {df.shape}")
            
            # 构建知识图谱
            start_time = datetime.now()
            
            build_result = kg_builder.build_knowledge_graph_from_dataframe(
                df, table_name, project_id='optimized_rebuild'
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"  构建结果:")
            print(f"    状态: {build_result['status']}")
            print(f"    实体数: {build_result['entities_created']}")
            print(f"    三元组数: {build_result['triples_created']}")
            print(f"    耗时: {duration:.2f}秒")
            
            if build_result['status'] == 'completed':
                total_entities += build_result['entities_created']
                total_relations += build_result['triples_created']
            else:
                print(f"    错误: {build_result.get('error', '未知错误')}")
        
        except Exception as e:
            print(f"  构建表 {table_name} 时出错: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n=== 构建完成 ===")
    print(f"总实体数: {total_entities}")
    print(f"总关系数: {total_relations}")
    
    return total_entities, total_relations

def verify_results():
    """验证构建结果"""
    print("\n=== 验证构建结果 ===")
    
    try:
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        
        # 获取统计信息
        stats = falkor_store.get_statistics()
        print(f"FalkorDB统计:")
        print(f"  实体数: {stats.get('total_entities', 0)}")
        print(f"  关系数: {stats.get('total_relations', 0)}")
        print(f"  三元组数: {stats.get('triples_stored', 0)}")
        
        # 查询一些样本数据
        print(f"\n实体样本:")
        entities = falkor_store.search_entities(limit=5)
        for i, entity in enumerate(entities, 1):
            print(f"  {i}. {entity.get('label', 'N/A')} ({entity.get('type', 'N/A')})")
        
        # 查询关系样本
        print(f"\n关系样本:")
        try:
            query = "MATCH (s)-[r]->(o) RETURN s.label AS subject, type(r) AS relation, o.label AS object LIMIT 5"
            result = falkor_store.graph.query(query)
            
            if result.result_set:
                for i, row in enumerate(result.result_set, 1):
                    subject, relation, obj = row
                    print(f"  {i}. {subject} -[{relation}]-> {obj}")
            else:
                print("  没有找到关系")
        except Exception as e:
            print(f"  查询关系失败: {e}")
        
        return stats
        
    except Exception as e:
        print(f"验证失败: {e}")
        return None

def main():
    """主函数"""
    print("开始重建知识图谱...")
    print("=" * 60)
    
    # 1. 清空现有数据
    if not clear_existing_data():
        print("清空数据失败，退出")
        return
    
    # 2. 重新构建知识图谱
    total_entities, total_relations = rebuild_knowledge_graph()
    
    # 3. 验证结果
    stats = verify_results()
    
    print("\n" + "=" * 60)
    print("重建完成！")
    
    if stats:
        improvement_ratio = stats.get('total_relations', 0) / max(1, 1237)  # 原来的关系数
        print(f"关系数量提升: {improvement_ratio:.1f}倍")
        print(f"从 1237 个关系 → {stats.get('total_relations', 0)} 个关系")

if __name__ == "__main__":
    main()
