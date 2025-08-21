#!/usr/bin/env python3
"""
分析实际运行日志和测试结果的差异
找出为什么实际系统显示0候选，而测试显示34候选
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
from src.matching.user_data_matcher import UserDataMatcher
from src.matching.universal_text_matcher import FieldType, FieldProcessingConfig
import logging

# 设置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """主函数"""
    try:
        # 初始化数据库连接
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config_manager.get_database_config())
        db = db_manager.get_db()
        
        print("🔍 分析实际运行日志和测试结果的差异")
        print("=" * 60)
        
        # 检查实际运行任务的配置
        tasks_collection = db["user_matching_tasks"]
        latest_task = tasks_collection.find_one(sort=[("created_at", -1)])
        
        if not latest_task:
            print("❌ 找不到最新的匹配任务")
            return
            
        print(f"📊 最新任务信息:")
        print(f"  任务ID: {latest_task.get('task_id')}")
        print(f"  源表: {latest_task.get('source_table')}")
        print(f"  目标表: {latest_task.get('target_table')}")
        print(f"  字段映射: {latest_task.get('field_mappings')}")
        
        # 获取实际的字段映射配置
        field_mappings = latest_task.get('field_mappings', [])
        source_table = latest_task.get('source_table')
        target_table = latest_task.get('target_table')
        
        print(f"\n🔍 实际运行的字段映射配置:")
        print("-" * 40)
        for mapping in field_mappings:
            print(f"  源字段: {mapping.get('source_field')}")
            print(f"  目标字段: {mapping.get('target_field')}")
            print(f"  字段类型: {mapping.get('field_type')}")
            print(f"  目标表: {mapping.get('target_table')}")
            print()
        
        # 检查源数据表是否存在天宝记录
        print(f"📊 检查源表 {source_table} 中的天宝记录:")
        print("-" * 40)
        
        source_collection = db[source_table]
        
        # 尝试不同的字段名查找天宝记录
        possible_address_fields = ['起火地点', 'ZCDZ', '地址', 'ADDRESS', 'address']
        tianbao_record = None
        used_field = None
        
        for field in possible_address_fields:
            record = source_collection.find_one({field: {"$regex": "天宝路881号"}})
            if record:
                tianbao_record = record
                used_field = field
                break
        
        if tianbao_record:
            print(f"✅ 在字段 '{used_field}' 中找到天宝记录")
            print(f"  记录ID: {tianbao_record['_id']}")
            print(f"  地址值: {tianbao_record[used_field]}")
            
            # 检查这个字段是否在映射配置中
            mapped_field = None
            for mapping in field_mappings:
                if mapping.get('source_field') == used_field:
                    mapped_field = mapping
                    break
            
            if mapped_field:
                print(f"✅ 字段 '{used_field}' 在映射配置中")
                print(f"  映射到目标字段: {mapped_field.get('target_field')}")
                print(f"  字段类型: {mapped_field.get('field_type')}")
            else:
                print(f"❌ 字段 '{used_field}' 不在映射配置中！")
                print("这可能是问题所在 - 天宝记录的字段没有被映射")
                
        else:
            print("❌ 在源表中找不到天宝记录")
            print("可能的原因:")
            print("1. 数据已被删除或修改")
            print("2. 字段名不匹配")
            print("3. 数据在不同的表中")
        
        # 模拟实际的匹配流程
        if tianbao_record and field_mappings:
            print(f"\n🚀 使用实际配置模拟匹配流程:")
            print("-" * 40)
            
            # 初始化匹配器
            user_matcher = UserDataMatcher(db_manager, config_manager)
            
            # 构建与实际系统一致的字段映射
            actual_field_mappings = []
            for mapping in field_mappings:
                field_config = FieldProcessingConfig(
                    field_type=FieldType.ADDRESS if mapping.get('field_type') == 'address' else FieldType.TEXT,
                    preprocessing_func='_preprocess_address' if mapping.get('field_type') == 'address' else '_preprocess_text',
                    keyword_extraction_func='_extract_address_keywords' if mapping.get('field_type') == 'address' else '_extract_text_keywords',
                    similarity_threshold=0.6,
                    max_candidates=50
                )
                
                actual_field_mappings.append({
                    'source_field': mapping.get('source_field'),
                    'target_field': mapping.get('target_field'),
                    'target_table': mapping.get('target_table'),
                    'field_type': FieldType.ADDRESS if mapping.get('field_type') == 'address' else FieldType.TEXT,
                    'config': field_config
                })
            
            # 测试天宝记录
            test_records = [tianbao_record]
            
            print(f"测试记录: {tianbao_record[used_field]}")
            
            # 执行预过滤
            prefilter_results = user_matcher.universal_query_engine.query_batch_records(
                test_records, actual_field_mappings
            )
            
            record_id = str(tianbao_record['_id'])
            if record_id in prefilter_results and prefilter_results[record_id]:
                query_result = prefilter_results[record_id]
                candidates = query_result.candidates if hasattr(query_result, 'candidates') else []
                print(f"✅ 使用实际配置找到 {len(candidates)} 个候选")
                if candidates:
                    print(f"第一个候选: {candidates[0].get(mapped_field.get('target_field'), 'N/A')}")
            else:
                print("❌ 使用实际配置没有找到候选")
                print("这说明实际配置和测试配置不同！")
        
    except Exception as e:
        logger.error(f"分析失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
