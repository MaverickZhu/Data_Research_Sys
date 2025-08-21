#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能索引和图结构预建脚本
在系统启动时自动创建必要的优化结构
"""

import sys
import os
import time
import logging
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from src.database.connection import DatabaseManager
    from src.matching.graph_matcher import GraphMatcher
    from src.matching.slice_enhanced_matcher import SliceEnhancedMatcher
    from src.utils.logger import setup_logger
    from src.utils.config import ConfigManager
except ImportError as e:
    print(f"导入错误: {e}")
    sys.exit(1)

# 设置日志
logger = setup_logger(__name__)

class PerformancePrebuilder:
    """性能优化预建器"""
    
    def __init__(self):
        # 初始化配置管理器
        self.config_manager = ConfigManager()
        self.db_manager = DatabaseManager(config=self.config_manager.get_database_config())
        self.stats = {
            'indexes_created': 0,
            'graph_nodes': 0,
            'slice_indexes': 0,
            'total_time': 0
        }
    
    def prebuild_all(self):
        """预建所有性能优化结构"""
        start_time = time.time()
        logger.info("🚀 开始预建高性能优化结构...")
        
        try:
            # 1. 创建基础数据库索引
            self._create_basic_indexes()
            
            # 2. 预建图结构
            self._prebuild_graph_structure()
            
            # 3. 创建切片索引
            self._create_slice_indexes()
            
            # 4. 预热缓存
            self._warmup_caches()
            
            self.stats['total_time'] = time.time() - start_time
            self._print_summary()
            
            return True
            
        except Exception as e:
            logger.error(f"预建失败: {str(e)}")
            return False
    
    def _create_basic_indexes(self):
        """创建基础数据库索引"""
        logger.info("创建基础数据库索引...")
        
        db = self.db_manager.mongo_client.get_database()
        
        # 常用表的基础索引
        tables_and_fields = {
            'xfaqpc_jzdwxx': ['UNIT_NAME', 'UNIT_ADDRESS', 'LEGAL_PEOPLE'],
            'xxj_shdwjbxx': ['dwmc', 'dwdz', 'fddbr'],
            'dwd_yljgxx': ['医疗机构名称', '地址', '法定代表人']
        }
        
        for table, fields in tables_and_fields.items():
            try:
                collection = db[table]
                
                # 创建单字段索引
                for field in fields:
                    try:
                        collection.create_index([(field, 1)])
                        self.stats['indexes_created'] += 1
                        logger.debug(f"创建索引: {table}.{field}")
                    except Exception as e:
                        logger.debug(f"索引已存在或创建失败: {table}.{field}")
                
                # 创建复合索引
                if len(fields) >= 2:
                    try:
                        collection.create_index([(fields[0], 1), (fields[1], 1)])
                        self.stats['indexes_created'] += 1
                        logger.debug(f"创建复合索引: {table}.{fields[0]}+{fields[1]}")
                    except Exception as e:
                        logger.debug(f"复合索引已存在或创建失败: {table}")
                        
            except Exception as e:
                logger.warning(f"表 {table} 索引创建失败: {str(e)}")
        
        logger.info(f"基础索引创建完成，共创建 {self.stats['indexes_created']} 个索引")
    
    def _prebuild_graph_structure(self):
        """预建图结构"""
        logger.info("预建图结构...")
        
        try:
            db = self.db_manager.mongo_client.get_database()
            graph_matcher = GraphMatcher(db)
            
            # 【关键修复】预建适中规模图结构，避免连接超时
            graph_start = time.time()
            graph_matcher.build_graph(limit=10000)  # 预建1万节点的图，避免超时
            graph_time = time.time() - graph_start
            
            self.stats['graph_nodes'] = graph_matcher.graph.number_of_nodes()
            logger.info(f"图结构预建完成: {self.stats['graph_nodes']} 个节点, "
                       f"耗时: {graph_time:.2f}秒")
            
        except Exception as e:
            logger.error(f"图结构预建失败: {str(e)}")
    
    def _create_slice_indexes(self):
        """创建切片索引"""
        logger.info("创建切片索引...")
        
        try:
            slice_matcher = SliceEnhancedMatcher(self.db_manager)
            
            # 检查切片索引表是否存在
            db = self.db_manager.mongo_client.get_database()
            slice_collections = [
                'xfaqpc_jzdwxx_name_slices',
                'xfaqpc_jzdwxx_name_keywords', 
                'xxj_shdwjbxx_name_slices',
                'xxj_shdwjbxx_name_keywords'
            ]
            
            for collection_name in slice_collections:
                try:
                    collection = db[collection_name]
                    # 创建切片索引
                    collection.create_index([('slice', 1)])
                    collection.create_index([('keyword', 1)])
                    collection.create_index([('source_id', 1)])
                    self.stats['slice_indexes'] += 3
                    logger.debug(f"切片索引创建: {collection_name}")
                except Exception as e:
                    logger.debug(f"切片索引已存在: {collection_name}")
            
            logger.info(f"切片索引创建完成，共创建 {self.stats['slice_indexes']} 个索引")
            
        except Exception as e:
            logger.error(f"切片索引创建失败: {str(e)}")
    
    def _warmup_caches(self):
        """预热缓存"""
        logger.info("预热系统缓存...")
        
        try:
            db = self.db_manager.mongo_client.get_database()
            
            # 预热主要表的查询缓存
            tables = ['xfaqpc_jzdwxx', 'xxj_shdwjbxx', 'dwd_yljgxx']
            for table in tables:
                try:
                    collection = db[table]
                    # 执行一些预热查询
                    collection.find().limit(100).to_list(100)
                    collection.count_documents({})
                    logger.debug(f"缓存预热: {table}")
                except Exception as e:
                    logger.debug(f"缓存预热失败: {table}")
            
            logger.info("系统缓存预热完成")
            
        except Exception as e:
            logger.error(f"缓存预热失败: {str(e)}")
    
    def _print_summary(self):
        """打印预建摘要"""
        logger.info("=" * 60)
        logger.info("🎯 高性能优化预建完成摘要:")
        logger.info(f"  📊 创建基础索引: {self.stats['indexes_created']} 个")
        logger.info(f"  🕸️  图结构节点: {self.stats['graph_nodes']} 个")
        logger.info(f"  🔍 切片索引: {self.stats['slice_indexes']} 个")
        logger.info(f"  ⏱️  总耗时: {self.stats['total_time']:.2f} 秒")
        logger.info("=" * 60)

def main():
    """主函数"""
    prebuilder = PerformancePrebuilder()
    success = prebuilder.prebuild_all()
    
    if success:
        logger.info("✅ 高性能优化预建成功完成！")
        return 0
    else:
        logger.error("❌ 高性能优化预建失败！")
        return 1

if __name__ == "__main__":
    exit(main())
