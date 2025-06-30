import sys
from pathlib import Path
from pymongo import TEXT, ASCENDING

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def create_indexes():
    """为关键集合创建索引以优化查询性能。"""
    try:
        logger.info("正在初始化数据库管理器以创建索引...")
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config=config_manager.get_database_config())
        db = db_manager.get_db()

        # 1. 为源数据集合 (xxj_shdwjbxx) 创建索引
        logger.info("--- 正在处理源数据集合 (xxj_shdwjbxx) ---")
        target_collection_name = 'xxj_shdwjbxx'
        target_collection = db[target_collection_name]
        
        # 定义索引
        source_indexes = {
            "compound_text_index": ([("dwmc", TEXT), ("dwdz", TEXT)], {"default_language": "none", "name": "compound_text_index"}),
            "fddbr_asc": ([("fddbr", ASCENDING)], {"name": "fddbr_asc"})
        }

        existing_source_indexes = target_collection.index_information()
        logger.info(f"'{target_collection_name}' 上已存在的索引: {list(existing_source_indexes.keys())}")

        for index_name, (keys, options) in source_indexes.items():
            if options["name"] in existing_source_indexes:
                logger.info(f"ℹ️ 源集合索引 '{options['name']}' 已存在，跳过。")
            else:
                logger.info(f"正在创建源集合索引: {options['name']}...")
                target_collection.create_index(keys, **options)
                logger.info(f"✅ 源集合索引 {options['name']} 创建成功。")

        # 2. 为基础匹配结果集合 (unit_match_results) 创建索引
        logger.info("\n--- 正在处理基础匹配结果集合 (unit_match_results) ---")
        results_collection_name = 'unit_match_results'
        results_collection = db[results_collection_name]
        
        result_indexes = {
            "res_unit_name": ([("unit_name", ASCENDING)], {"name": "res_unit_name"}),
            "res_credit_code": ([("primary_credit_code", ASCENDING)], {"name": "res_credit_code"}),
            "res_building_id": ([("building_id", ASCENDING)], {"name": "res_building_id"})
        }

        existing_result_indexes = results_collection.index_information()
        logger.info(f"'{results_collection_name}' 上已存在的索引: {list(existing_result_indexes.keys())}")
        
        for index_name, (keys, options) in result_indexes.items():
            if options["name"] in existing_result_indexes:
                logger.info(f"ℹ️ 基础匹配结果索引 '{options['name']}' 已存在，跳过。")
            else:
                logger.info(f"正在创建基础匹配结果索引: {options['name']}...")
                results_collection.create_index(keys, **options)
                logger.info(f"✅ 基础匹配结果索引 {options['name']} 创建成功。")
        
        # 3. 为增强关联结果集合 (enhanced_association_results) 创建唯一索引
        logger.info("\n--- 正在处理增强关联结果集合 (enhanced_association_results) ---")
        final_collection_name = 'enhanced_association_results'
        final_collection = db[final_collection_name]

        final_index_name = "association_id_unique"
        existing_final_indexes = final_collection.index_information()
        logger.info(f"'{final_collection_name}' 上已存在的索引: {list(existing_final_indexes.keys())}")

        if final_index_name not in existing_final_indexes:
            logger.info(f"正在创建唯一索引: '{final_index_name}'...")
            final_collection.create_index([("association_id", ASCENDING)], name=final_index_name, unique=True)
            logger.info(f"✅ 唯一索引 '{final_index_name}' 创建成功。")
        else:
            logger.info(f"ℹ️ 唯一索引 '{final_index_name}' 已存在。")

        logger.info("\n🎉 所有索引创建流程完成。")

    except Exception as e:
        logger.error(f"创建索引失败: {e}", exc_info=True)

if __name__ == "__main__":
    create_indexes() 