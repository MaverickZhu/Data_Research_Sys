#!/usr/bin/env python3
"""
使用通用框架重新构建地址索引
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
from src.matching.universal_index_builder import UniversalIndexBuilder

def rebuild_address_indexes():
    """重新构建地址索引"""
    print("🔧 使用通用框架重新构建地址索引")
    print("=" * 50)
    
    # 初始化组件
    config_manager = ConfigManager()
    db_config = config_manager.get_database_config()
    db_manager = DatabaseManager(db_config)
    
    index_builder = UniversalIndexBuilder(db_manager)
    
    # 重新构建目标表的地址索引
    target_table = "dwd_yljgxx"
    target_fields = ["ZCDZ", "FWCS_DZ", "NSYLJGDZ"]  # 地址相关字段
    
    print(f"目标表: {target_table}")
    print(f"目标字段: {target_fields}")
    
    for field in target_fields:
        print(f"\n🚀 重新构建字段 '{field}' 的索引...")
        
        try:
            # 删除旧索引
            old_index_name = f"{target_table}_address_keywords"
            old_collection = db_manager.get_collection(old_index_name)
            old_collection.drop()
            print(f"✅ 删除旧索引表: {old_index_name}")
            
            # 构建新索引
            result = index_builder.build_field_index(
                table_name=target_table,
                field_name=field,
                field_type="address"
            )
            
            print(f"✅ 构建完成: {result}")
            
        except Exception as e:
            print(f"❌ 构建失败: {e}")
    
    print(f"\n🎉 地址索引重建完成！")

if __name__ == "__main__":
    rebuild_address_indexes()
