import os
import sys
import time
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
from src.matching.optimized_match_processor import OptimizedMatchProcessor, MatchingMode
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def run_test():
    """运行图匹配测试"""
    try:
        logger.info("初始化配置和数据库管理器...")
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config_manager.get_database_config())
        matching_config = config_manager.get_matching_config()

        logger.info("初始化 OptimizedMatchProcessor...")
        processor = OptimizedMatchProcessor(db_manager, matching_config)

        logger.info("启动全量匹配任务 (Full Mode)...")
        task_id = processor.start_optimized_matching_task(
            match_type="both",
            mode=MatchingMode.FULL,
            clear_existing=True
        )
        logger.info(f"任务已启动，任务ID: {task_id}")

        # 循环监控任务进度
        while True:
            progress = processor.get_optimized_task_progress(task_id)
            if 'error' in progress:
                logger.error(f"获取任务进度失败: {progress['error']}")
                break

            status = progress.get('status')
            progress_percent = progress.get('progress_percent', 0)
            processed_records = progress.get('processed_records', 0)
            total_records = progress.get('total_records', 0)

            logger.info(
                f"任务状态: {status} | "
                f"进度: {progress_percent:.2f}% ({processed_records}/{total_records})"
            )

            if status in ['completed', 'error', 'stopped']:
                logger.info(f"任务结束，最终状态: {status}")
                break
            
            time.sleep(10) # 每10秒查询一次进度

        logger.info("测试脚本执行完毕。")

    except Exception as e:
        logger.error(f"测试脚本执行失败: {e}", exc_info=True)

if __name__ == "__main__":
    run_test() 