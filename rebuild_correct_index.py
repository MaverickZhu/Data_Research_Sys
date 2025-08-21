#!/usr/bin/env python3
"""
重新构建正确名称的索引表
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
from src.matching.universal_index_builder import UniversalIndexBuilder
from src.matching.universal_text_matcher import FieldType
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """主函数"""
    try:
        # 初始化数据库连接
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config_manager.get_database_config())
        db = db_manager.get_db()
        
        print("🔧 重新构建正确名称的索引表")
        print("=" * 60)
        
        # 正确的索引表名称
        correct_index_name = "dwd_yljgxx_address_keywords"
        old_index_name = "dwd_yljgxx_ZCDZ_keywords"
        
        print(f"目标索引表名: {correct_index_name}")
        print(f"旧索引表名: {old_index_name}")
        print()
        
        # 1. 删除旧索引表（如果存在）
        if correct_index_name in db.list_collection_names():
            print(f"删除现有索引表: {correct_index_name}")
            db[correct_index_name].drop()
        
        # 2. 检查源数据
        source_collection = db["dwd_yljgxx"]
        total_records = source_collection.count_documents({})
        address_records = source_collection.count_documents({"ZCDZ": {"$exists": True, "$ne": ""}})
        
        print(f"源表总记录数: {total_records}")
        print(f"有地址字段的记录数: {address_records}")
        print()
        
        # 3. 复制现有索引数据（如果旧索引表存在且有数据）
        if old_index_name in db.list_collection_names():
            old_collection = db[old_index_name]
            old_count = old_collection.count_documents({})
            
            if old_count > 0:
                print(f"复制现有索引数据: {old_count} 条记录")
                
                # 复制数据到新索引表
                new_collection = db[correct_index_name]
                
                # 批量复制
                batch_size = 1000
                for skip in range(0, old_count, batch_size):
                    batch = list(old_collection.find().skip(skip).limit(batch_size))
                    if batch:
                        new_collection.insert_many(batch)
                        print(f"  已复制: {min(skip + batch_size, old_count)}/{old_count}")
                
                print(f"✅ 索引数据复制完成")
                
                # 删除旧索引表
                print(f"删除旧索引表: {old_index_name}")
                db[old_index_name].drop()
            else:
                print(f"旧索引表为空，重新构建")
                # 重新构建索引
                index_builder = UniversalIndexBuilder(db_manager)
                index_builder.build_field_index("dwd_yljgxx", "ZCDZ", FieldType.ADDRESS)
        else:
            print(f"旧索引表不存在，重新构建")
            # 重新构建索引
            index_builder = UniversalIndexBuilder(db_manager)
            index_builder.build_field_index("dwd_yljgxx", "ZCDZ", FieldType.ADDRESS)
        
        # 4. 验证新索引表
        if correct_index_name in db.list_collection_names():
            new_count = db[correct_index_name].count_documents({})
            print(f"✅ 新索引表创建成功: {new_count} 条记录")
            
            # 检查天宝记录
            tianbao_records = list(db[correct_index_name].find({"keyword": "天宝路"}))
            print(f"天宝路关键词记录数: {len(tianbao_records)}")
            
            if tianbao_records:
                sample = tianbao_records[0]
                print(f"示例记录: {sample.get('original_value', 'N/A')}")
        else:
            print("❌ 新索引表创建失败")
        
        print()
        print("🎯 重建完成！")
        print("📝 建议:")
        print("  1. 重启匹配服务")
        print("  2. 运行新的匹配任务进行测试")
        
    except Exception as e:
        logger.error(f"重建失败: {e}")
        print(f"❌ 错误: {e}")

if __name__ == "__main__":
    main()
