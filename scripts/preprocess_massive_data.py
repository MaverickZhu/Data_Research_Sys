#!/usr/bin/env python3
"""
海量数据预处理器
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def preprocess_data():
    try:
        import yaml
        from src.database.connection import DatabaseManager
        print("🔄 开始数据预处理...")
        
        # 直接加载数据库配置
        with open("config/database.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        db_manager = DatabaseManager(config)
        supervision = db_manager.get_collection("xxj_shdwjbxx")
        inspection = db_manager.get_collection("xfaqpc_jzdwxx")
        
        sup_count = supervision.count_documents({})
        ins_count = inspection.count_documents({})
        
        print(f"📊 监督数据: {sup_count:,} 条")
        print(f"📊 排查数据: {ins_count:,} 条")
        
        # 创建索引
        indexes = [("dwmc", 1), ("tyshxydm", 1), ("dwdz", 1)]
        for field, direction in indexes:
            try:
                supervision.create_index([(field, direction)], background=True)
                inspection.create_index([(field, direction)], background=True)
                print(f"   ✅ 创建索引: {field}")
            except:
                pass
        
        return True
    except Exception as e:
        print(f"❌ 预处理失败: {e}")
        return False

if __name__ == "__main__":
    preprocess_data()
