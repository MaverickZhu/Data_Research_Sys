#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
清空匹配结果表脚本
"""
import sys
import os

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager

def clear_match_results():
    """清空匹配结果表"""
    try:
        # 初始化配置和数据库
        config_manager = ConfigManager()
        db_manager = DatabaseManager(config_manager.get_database_config())
        
        # 获取匹配结果集合
        match_results_collection = db_manager.get_collection('match_results')
        
        # 统计清空前的数据量
        before_count = match_results_collection.count_documents({})
        print(f"清空前匹配结果数量: {before_count}")
        
        if before_count > 0:
            # 清空集合
            result = match_results_collection.delete_many({})
            print(f"已清空 {result.deleted_count} 条匹配结果")
        else:
            print("匹配结果表已经是空的")
        
        # 验证清空结果
        after_count = match_results_collection.count_documents({})
        print(f"清空后匹配结果数量: {after_count}")
        
        if after_count == 0:
            print("✅ 匹配结果表清空成功！")
        else:
            print("❌ 匹配结果表清空失败！")
            
    except Exception as e:
        print(f"❌ 清空匹配结果表时发生错误: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    print("=" * 50)
    print("清空匹配结果表")
    print("=" * 50)
    
    confirm = input("确认要清空所有匹配结果吗？(输入 'yes' 确认): ")
    if confirm.lower() == 'yes':
        clear_match_results()
    else:
        print("操作已取消") 