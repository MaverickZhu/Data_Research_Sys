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
        # 初始化配置以获取数据库连接信息
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config=config_manager.get_database_config())
        db = db_manager.get_db()

        target_collection_name = 'xxj_shdwjbxx'
        target_collection = db[target_collection_name]
        logger.info(f"目标集合: {target_collection.name}")

        # 定义需要创建的索引
        indexes_to_create = {
            "dwmc_text": ([("dwmc", TEXT)], {"default_language": "none"}),
            "dwdz_asc": ([("dwdz", ASCENDING)], {}),
            "fddbr_asc": ([("fddbr", ASCENDING)], {})
        }

        # 获取已存在的索引信息，并提取出被索引的字段
        existing_indexes_info = target_collection.index_information()
        indexed_fields = set()
        for index in existing_indexes_info.values():
            if 'key' in index:
                indexed_fields.update(field[0] for field in index['key'])
        
        logger.info(f"已存在的索引: {list(existing_indexes_info.keys())}")
        logger.info(f"已被索引的字段: {indexed_fields}")

        for index_name, (keys, options) in indexes_to_create.items():
            # 检查该索引的第一个字段是否已经被索引
            field_to_check = keys[0][0]
            if field_to_check in indexed_fields:
                logger.info(f"ℹ️ 字段 '{field_to_check}' 已被其他索引覆盖，跳过创建 {index_name}。")
                continue

            logger.info(f"正在创建索引: {index_name} ... (这可能需要一些时间)")
            target_collection.create_index(keys, name=index_name, **options)
            logger.info(f"✅ 索引 {index_name} 创建成功。")
            # 更新已索引字段集合
            indexed_fields.add(field_to_check)
        
        logger.info("🎉 索引创建流程完成。")

    except Exception as e:
        logger.error(f"创建索引失败: {e}", exc_info=True)

if __name__ == "__main__":
    create_indexes() 