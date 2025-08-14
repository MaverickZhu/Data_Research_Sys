#!/usr/bin/env python3
"""
重新构建知识图谱三元组数据
将现有的kg_relations数据转换为新的简化格式存储到kg_triples
"""

try:
    from pymongo import MongoClient
except ImportError as e:
    print(f"缺少依赖: {e}")
    print("请运行: pip install pymongo")
    exit(1)

def rebuild_kg_triples():
    """重新构建知识图谱三元组"""
    print("🔄 开始重新构建知识图谱三元组")
    print("=" * 60)
    
    try:
        # 连接MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['Unit_Info']
        
        # 获取现有数据
        kg_entities_collection = db['kg_entities']
        kg_relations_collection = db['kg_relations']
        kg_triples_collection = db['kg_triples']
        
        print("✅ MongoDB连接成功")
        
        # 检查现有数据
        entities_count = kg_entities_collection.count_documents({})
        relations_count = kg_relations_collection.count_documents({})
        old_triples_count = kg_triples_collection.count_documents({})
        
        print(f"📊 现有数据统计:")
        print(f"   - 实体: {entities_count:,}")
        print(f"   - 关系: {relations_count:,}")
        print(f"   - 旧三元组: {old_triples_count:,}")
        
        if relations_count == 0:
            print("⚠️  没有关系数据，无法构建三元组")
            return
        
        # 清空旧的三元组数据
        print("\n🗑️  清空旧的三元组数据...")
        kg_triples_collection.delete_many({})
        
        # 创建实体ID到实体信息的映射
        print("📝 构建实体映射...")
        entity_map = {}
        for entity in kg_entities_collection.find():
            entity_map[entity['id']] = {
                'label': entity.get('label', 'unknown'),
                'type': entity.get('type', 'unknown')
            }
        
        print(f"   ✅ 实体映射完成: {len(entity_map)} 个实体")
        
        # 处理关系数据，转换为简化的三元组格式
        print("🔄 转换关系数据为三元组...")
        
        new_triples = []
        processed_count = 0
        
        for relation in kg_relations_collection.find():
            try:
                # 从关系数据中提取三元组信息
                subject_id = relation.get('subject_id')
                object_id = relation.get('object_id')
                predicate_info = relation.get('predicate', {})
                
                # 获取实体信息
                subject_info = entity_map.get(subject_id, {'label': 'unknown', 'type': 'unknown'})
                object_info = entity_map.get(object_id, {'label': 'unknown', 'type': 'unknown'})
                
                # 创建简化的三元组
                triple = {
                    'id': relation.get('id', f"triple_{processed_count}"),
                    'subject_id': subject_id,
                    'subject_label': subject_info['label'],
                    'subject_type': subject_info['type'],
                    'predicate_id': predicate_info.get('id', 'unknown'),
                    'predicate_label': predicate_info.get('label', 'unknown'),
                    'predicate_type': predicate_info.get('type', 'unknown'),
                    'object_id': object_id,
                    'object_label': object_info['label'],
                    'object_type': object_info['type'],
                    'confidence': relation.get('confidence', 0.0),
                    'source': relation.get('source', 'unknown'),
                    'evidence': relation.get('evidence', []),
                    'created_time': relation.get('created_time'),
                    'updated_time': relation.get('updated_time')
                }
                
                new_triples.append(triple)
                processed_count += 1
                
                # 批量插入
                if len(new_triples) >= 1000:
                    kg_triples_collection.insert_many(new_triples)
                    print(f"   💾 已处理: {processed_count:,}")
                    new_triples = []
            
            except Exception as e:
                print(f"   ⚠️  处理关系失败: {e}")
                continue
        
        # 插入剩余的三元组
        if new_triples:
            kg_triples_collection.insert_many(new_triples)
        
        # 检查结果
        final_triples_count = kg_triples_collection.count_documents({})
        
        print(f"\n📊 重建完成统计:")
        print(f"   - 处理关系: {processed_count:,}")
        print(f"   - 生成三元组: {final_triples_count:,}")
        print(f"   - 成功率: {(final_triples_count/processed_count*100):.1f}%" if processed_count > 0 else "   - 成功率: 0%")
        
        # 创建索引
        print("\n🔍 创建索引...")
        try:
            kg_triples_collection.create_index([("subject_id", 1)])
            kg_triples_collection.create_index([("object_id", 1)])
            kg_triples_collection.create_index([("predicate_type", 1)])
            kg_triples_collection.create_index([("confidence", -1)])
            print("   ✅ 索引创建完成")
        except Exception as e:
            print(f"   ⚠️  索引创建失败: {e}")
        
        if final_triples_count > 0:
            print("\n🎉 知识图谱三元组重建成功！")
            print("现在可以通过API正常查看知识图谱数据了")
        else:
            print("\n❌ 重建失败，请检查数据格式")
        
    except Exception as e:
        print(f"💥 重建失败: {e}")

if __name__ == "__main__":
    rebuild_kg_triples()
