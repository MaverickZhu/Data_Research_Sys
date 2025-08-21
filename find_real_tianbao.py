#!/usr/bin/env python3
"""
查找真正应该匹配的天宝记录
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
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
        
        print("🔍 查找真正应该匹配的天宝记录")
        print("=" * 60)
        
        # 1. 查找源数据中包含"天宝路"的记录
        print("📊 查找源数据中的天宝路记录:")
        print("-" * 40)
        
        source_collection = db["hztj_hzxx"]
        
        # 查找包含"天宝路"的记录
        tianbao_lu_source = list(source_collection.find({"起火地点": {"$regex": "天宝路"}}))
        print(f"源表中包含'天宝路'的记录数: {len(tianbao_lu_source)}")
        
        for i, record in enumerate(tianbao_lu_source):
            addr = record.get("起火地点", "")
            print(f"  {i+1}. {addr}")
        
        print()
        
        # 2. 查找包含"虹口区"的记录
        print("📊 查找源数据中的虹口区记录:")
        print("-" * 40)
        
        hongkou_source = list(source_collection.find({"起火地点": {"$regex": "虹口区"}}))
        print(f"源表中包含'虹口区'的记录数: {len(hongkou_source)}")
        
        for i, record in enumerate(hongkou_source[:5]):  # 只显示前5个
            addr = record.get("起火地点", "")
            print(f"  {i+1}. {addr}")
        
        if len(hongkou_source) > 5:
            print(f"  ... 还有 {len(hongkou_source) - 5} 条记录")
        
        print()
        
        # 3. 查找包含"881"的记录
        print("📊 查找源数据中包含881的记录:")
        print("-" * 40)
        
        addr_881_source = list(source_collection.find({"起火地点": {"$regex": "881"}}))
        print(f"源表中包含'881'的记录数: {len(addr_881_source)}")
        
        for i, record in enumerate(addr_881_source):
            addr = record.get("起火地点", "")
            print(f"  {i+1}. {addr}")
        
        print()
        
        # 4. 查找可能的匹配组合
        print("📊 查找可能的匹配组合:")
        print("-" * 40)
        
        # 查找同时包含"虹口"和"天宝"的记录
        combined_source = list(source_collection.find({
            "$and": [
                {"起火地点": {"$regex": "虹口"}},
                {"起火地点": {"$regex": "天宝"}}
            ]
        }))
        
        print(f"源表中同时包含'虹口'和'天宝'的记录数: {len(combined_source)}")
        
        for i, record in enumerate(combined_source):
            addr = record.get("起火地点", "")
            print(f"  {i+1}. {addr}")
        
        print()
        
        # 5. 检查目标记录
        print("📊 目标记录确认:")
        print("-" * 40)
        
        target_collection = db["dwd_yljgxx"]
        target_record = target_collection.find_one({"ZCDZ": {"$regex": "天宝路881号"}})
        
        if target_record:
            print(f"目标记录: {target_record.get('ZCDZ', 'N/A')}")
            print(f"记录ID: {target_record.get('_id', 'N/A')}")
        else:
            print("❌ 没有找到目标记录")
        
        print()
        
        # 6. 结论
        print("🎯 分析结论:")
        print("=" * 60)
        
        if not tianbao_lu_source and not combined_source:
            print("❌ 源数据中没有与目标天宝路记录匹配的数据")
            print("💡 这解释了为什么匹配数为0")
            print("📝 建议:")
            print("  1. 确认测试数据是否正确")
            print("  2. 检查是否有其他应该匹配的记录对")
            print("  3. 调整地址标准化规则，避免过度清洗")
        else:
            print("✅ 找到了潜在的匹配记录")
        
    except Exception as e:
        logger.error(f"查找失败: {e}")
        print(f"❌ 错误: {e}")

if __name__ == "__main__":
    main()
