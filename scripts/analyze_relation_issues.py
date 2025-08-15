#!/usr/bin/env python3
"""
分析关系生成问题
检查为什么知识图谱中关系数量为0
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
from src.knowledge_graph.falkordb_store import FalkorDBStore
from src.knowledge_graph.entity_extractor import EntityExtractor
from src.knowledge_graph.relation_extractor import RelationExtractor
import pandas as pd

def analyze_kg_build_tasks():
    """分析知识图谱构建任务"""
    print("=== 分析知识图谱构建任务 ===")
    
    # 初始化配置和数据库
    config_manager = ConfigManager()
    db_config = config_manager.get_database_config()
    db_manager = DatabaseManager(db_config)
    
    # 检查最近的构建任务
    kg_tasks = list(db_manager.get_collection('kg_build_tasks').find({}).sort('created_time', -1).limit(3))
    
    print(f"找到 {len(kg_tasks)} 个构建任务")
    
    for i, task in enumerate(kg_tasks, 1):
        print(f"\n--- 任务 {i} ---")
        print(f"任务ID: {task.get('task_id', 'N/A')}")
        print(f"项目ID: {task.get('project_id', 'N/A')}")
        print(f"状态: {task.get('status', 'N/A')}")
        print(f"表列表: {task.get('table_list', [])}")
        print(f"实体数量: {task.get('entities_count', 0)}")
        print(f"关系数量: {task.get('relations_count', 0)}")
        print(f"三元组数量: {task.get('triples_count', 0)}")
        print(f"错误信息: {task.get('error_message', 'None')}")
        print(f"完成时间: {task.get('completed_time', '未完成')}")
        
        # 分析构建详情
        build_details = task.get('build_details', {})
        if build_details:
            print(f"构建详情:")
            for table_name, details in build_details.items():
                print(f"  {table_name}: 实体={details.get('entities', 0)}, 关系={details.get('relations', 0)}")

def analyze_falkordb_content():
    """分析FalkorDB中的内容"""
    print("\n=== 分析FalkorDB内容 ===")
    
    try:
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        
        # 获取统计信息
        stats = falkor_store.get_statistics()
        print(f"FalkorDB统计信息:")
        print(f"  总实体数: {stats.get('total_entities', 0)}")
        print(f"  总关系数: {stats.get('total_relations', 0)}")
        print(f"  三元组数: {stats.get('triples_stored', 0)}")
        print(f"  标签数: {stats.get('total_labels', 0)}")
        
        # 查询一些实体
        entities = falkor_store.search_entities(limit=5)
        print(f"\n前5个实体:")
        for entity in entities:
            print(f"  实体ID: {entity.get('entity_id', 'N/A')}")
            print(f"  类型: {entity.get('type', 'N/A')}")
            print(f"  标签: {entity.get('label', 'N/A')}")
            print(f"  项目: {entity.get('project_name', 'N/A')}")
            print("  ---")
        
        # 查询关系
        print(f"\n查询关系:")
        try:
            # 直接查询关系节点
            query = "MATCH (r:Relation) RETURN r LIMIT 5"
            result = falkor_store.graph.query(query)
            
            if result.result_set:
                print(f"找到 {len(result.result_set)} 个关系:")
                for row in result.result_set:
                    relation = row[0]
                    print(f"  关系: {relation}")
            else:
                print("  没有找到任何关系节点")
                
        except Exception as e:
            print(f"  查询关系失败: {e}")
            
        # 查询三元组
        print(f"\n查询三元组:")
        try:
            query = "MATCH (s)-[r]->(o) RETURN s, r, o LIMIT 5"
            result = falkor_store.graph.query(query)
            
            if result.result_set:
                print(f"找到 {len(result.result_set)} 个三元组:")
                for row in result.result_set:
                    subject, relation, obj = row
                    print(f"  {subject} -[{relation}]-> {obj}")
            else:
                print("  没有找到任何三元组")
                
        except Exception as e:
            print(f"  查询三元组失败: {e}")
        
    except Exception as e:
        print(f"连接FalkorDB失败: {e}")

def test_relation_extraction():
    """测试关系抽取功能"""
    print("\n=== 测试关系抽取功能 ===")
    
    try:
        # 初始化配置
        config_manager = ConfigManager()
        db_config = config_manager.get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 获取测试数据
        print("获取测试数据...")
        collection_names = ['dwd_yljgxx', 'dwd_zzzhzj']
        
        for collection_name in collection_names:
            print(f"\n--- 测试表: {collection_name} ---")
            
            try:
                collection = db_manager.get_collection(collection_name)
                sample_data = list(collection.find({}).limit(10))
                
                if not sample_data:
                    print(f"  {collection_name} 表中没有数据")
                    continue
                
                print(f"  获取到 {len(sample_data)} 条样本数据")
                
                # 转换为DataFrame
                df = pd.DataFrame(sample_data)
                print(f"  DataFrame形状: {df.shape}")
                print(f"  字段列表: {list(df.columns)}")
                
                # 测试实体抽取
                entity_extractor = EntityExtractor()
                entities = entity_extractor.extract_entities_from_dataframe(df, collection_name)
                print(f"  抽取到 {len(entities)} 个实体")
                
                if entities:
                    print(f"  前3个实体:")
                    for i, entity in enumerate(entities[:3]):
                        print(f"    实体{i+1}: {entity.label} ({entity.type.value})")
                
                # 测试关系抽取
                if entities:
                    relation_extractor = RelationExtractor()
                    triples = relation_extractor.extract_relations_from_dataframe(df, entities, collection_name)
                    print(f"  抽取到 {len(triples)} 个关系")
                    
                    if triples:
                        print(f"  前3个关系:")
                        for i, triple in enumerate(triples[:3]):
                            print(f"    关系{i+1}: {triple.subject.label} -[{triple.predicate.type.value}]-> {triple.object.label}")
                    else:
                        print(f"  ❌ 没有抽取到任何关系！")
                        print(f"  可能的原因:")
                        print(f"    - 实体类型不匹配关系规则")
                        print(f"    - 字段模式匹配失败")
                        print(f"    - 置信度阈值过高")
                        
                        # 分析实体类型分布
                        entity_types = {}
                        for entity in entities:
                            entity_type = entity.type.value
                            entity_types[entity_type] = entity_types.get(entity_type, 0) + 1
                        print(f"    实体类型分布: {entity_types}")
                else:
                    print(f"  ❌ 没有抽取到任何实体！")
                
            except Exception as e:
                print(f"  处理表 {collection_name} 时出错: {e}")
        
    except Exception as e:
        print(f"测试关系抽取失败: {e}")

def main():
    """主函数"""
    print("开始分析关系生成问题...")
    print("=" * 60)
    
    # 1. 分析构建任务
    analyze_kg_build_tasks()
    
    # 2. 分析FalkorDB内容
    analyze_falkordb_content()
    
    # 3. 测试关系抽取
    test_relation_extraction()
    
    print("\n" + "=" * 60)
    print("分析完成！")

if __name__ == "__main__":
    main()
