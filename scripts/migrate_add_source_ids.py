import sys
import os
from datetime import datetime

# Add project root to Python path for module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import DatabaseManager
from src.utils.config import ConfigManager
from src.utils.logger import setup_logger
from pymongo import UpdateOne
from tqdm import tqdm
from bson import ObjectId

# Setup logger
logger = setup_logger(__name__)

def migrate_add_source_ids():
    """
    One-time migration script to backfill `xfaqpc_jzdwxx_id` and `xxj_shdwjbxx_id`
    into the `unit_match_results` collection based on `primary_record_id` and `matched_record_id`.
    """
    logger.info("="*50)
    logger.info(f"启动历史数据迁移脚本... ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    logger.info("="*50)

    try:
        # Initialize database connection
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config=config_manager.get_database_config())
        
        # Get collections
        results_collection = db_manager.get_collection('unit_match_results')
        xfaqpc_collection = db_manager.get_collection('xfaqpc_jzdwxx')
        xxj_collection = db_manager.get_collection('xxj_shdwjbxx')

        # Find documents that need migration
        query = {
            "$or": [
                {"xfaqpc_jzdwxx_id": {"$exists": False}},
                {"xxj_shdwjbxx_id": {"$exists": False}}
            ]
        }
        total_docs_to_migrate = results_collection.count_documents(query)
        
        if total_docs_to_migrate == 0:
            logger.info("🎉 所有记录都已包含源ID，无需迁移。")
            return

        logger.info(f"发现 {total_docs_to_migrate} 条记录需要迁移。")

        # Use a cursor to iterate through documents
        cursor = results_collection.find(query, no_cursor_timeout=True)

        operations = []
        batch_size = 500
        updated_count = 0
        processed_count = 0
        error_count = 0

        with tqdm(total=total_docs_to_migrate, desc="迁移进度") as pbar:
            for doc in cursor:
                processed_count += 1
                update_payload = {}

                # 1. Get xfaqpc_jzdwxx_id
                primary_record_obj_id = doc.get('primary_record_id')
                if primary_record_obj_id and 'xfaqpc_jzdwxx_id' not in doc:
                    try:
                        # Ensure it's a valid ObjectId
                        if not isinstance(primary_record_obj_id, ObjectId):
                           primary_record_obj_id = ObjectId(primary_record_obj_id)
                        
                        source_doc = xfaqpc_collection.find_one(
                            {"_id": primary_record_obj_id},
                            {"ID": 1} # Projection
                        )
                        if source_doc and 'ID' in source_doc:
                            update_payload['xfaqpc_jzdwxx_id'] = source_doc['ID']
                    except Exception as e:
                        logger.warning(f"处理 primary_record_id {primary_record_obj_id} 失败: {e}")
                        error_count += 1
                
                # 2. Get xxj_shdwjbxx_id
                matched_record_obj_id = doc.get('matched_record_id')
                if matched_record_obj_id and 'xxj_shdwjbxx_id' not in doc:
                    try:
                        # Ensure it's a valid ObjectId
                        if not isinstance(matched_record_obj_id, ObjectId):
                           matched_record_obj_id = ObjectId(matched_record_obj_id)

                        target_doc = xxj_collection.find_one(
                            {"_id": matched_record_obj_id},
                            {"ID": 1} # Projection
                        )
                        if target_doc and 'ID' in target_doc:
                            update_payload['xxj_shdwjbxx_id'] = target_doc['ID']
                    except Exception as e:
                        logger.warning(f"处理 matched_record_id {matched_record_obj_id} 失败: {e}")
                        error_count += 1

                # If we found any ID, prepare the update operation
                if update_payload:
                    operations.append(
                        UpdateOne(
                            {"_id": doc["_id"]},
                            {"$set": update_payload}
                        )
                    )
                
                # Execute bulk write when batch is full
                if len(operations) >= batch_size:
                    try:
                        result = results_collection.bulk_write(operations)
                        updated_count += result.modified_count
                        operations = []
                    except Exception as e:
                        logger.error(f"批量写入失败: {e}")
                        error_count += len(operations)
                        operations = [] # Clear operations after error

                pbar.update(1)

            # Process the final batch
            if operations:
                try:
                    result = results_collection.bulk_write(operations)
                    updated_count += result.modified_count
                except Exception as e:
                    logger.error(f"最后批次写入失败: {e}")
                    error_count += len(operations)

        logger.info("\n" + "="*50)
        logger.info("迁移完成！")
        logger.info(f"总处理记录: {processed_count}")
        logger.info(f"成功更新记录: {updated_count}")
        logger.info(f"处理失败或跳过记录: {error_count}")
        logger.info("="*50)

    except Exception as e:
        logger.error(f"迁移脚本发生严重错误: {e}", exc_info=True)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
            logger.info("数据库游标已关闭。")


if __name__ == "__main__":
    migrate_add_source_ids() 