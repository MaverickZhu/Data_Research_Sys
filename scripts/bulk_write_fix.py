#!/usr/bin/env python3
"""
批量写入操作修复脚本
修复MongoDB批量写入操作中的数据类型和格式问题
"""

import os
import sys
import time
import json
import yaml
from pathlib import Path
from datetime import datetime
from bson import ObjectId

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """主函数"""
    print("🔧 批量写入操作修复开始...")
    
    try:
        from src.database.connection import DatabaseManager
        
        # 加载数据库配置
        with open("config/database.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        db_manager = DatabaseManager(config)
        
        # 获取正确的集合
        match_collection = db_manager.get_collection("unit_match_results")
        
        # 修复数据类型问题的批量写入操作
        def safe_bulk_write(operations):
            """安全的批量写入操作"""
            try:
                # 数据类型转换
                safe_operations = []
                
                for op in operations:
                    if 'replaceOne' in op:
                        replace_op = op['replaceOne']
                        
                        # 确保所有datetime对象都是正确的格式
                        replacement = replace_op['replacement']
                        
                        # 转换datetime对象
                        for key, value in replacement.items():
                            if isinstance(value, datetime):
                                replacement[key] = value
                            elif key in ['created_time', 'updated_time', 'review_time'] and value is not None:
                                if isinstance(value, str):
                                    try:
                                        replacement[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                    except:
                                        replacement[key] = datetime.now()
                                else:
                                    replacement[key] = value
                        
                        # 创建安全的操作
                        safe_op = {
                            'replaceOne': {
                                'filter': replace_op['filter'],
                                'replacement': replacement,
                                'upsert': True
                            }
                        }
                        safe_operations.append(safe_op)
                
                # 执行批量操作
                if safe_operations:
                    result = match_collection.bulk_write(safe_operations)
                    return result
                else:
                    return None
                    
            except Exception as e:
                print(f"批量写入失败: {e}")
                return None
        
        # 测试批量写入
        test_operations = [
            {
                'replaceOne': {
                    'filter': {'source_record_id': 'test_fix_001'},
                    'replacement': {
                        'source_record_id': 'test_fix_001',
                        'source_system': 'supervision',
                        'match_type': 'none',
                        'match_status': 'unmatched',
                        'similarity_score': 0.0,
                        'match_confidence': 'none',
                        'created_time': datetime.now(),
                        'updated_time': datetime.now(),
                        'review_status': 'pending',
                        'review_time': None
                    },
                    'upsert': True
                }
            }
        ]
        
        # 执行测试
        result = safe_bulk_write(test_operations)
        
        if result:
            print(f"✅ 批量写入测试成功: 插入={result.upserted_count}, 修改={result.modified_count}")
            
            # 验证数据是否写入
            test_record = match_collection.find_one({'source_record_id': 'test_fix_001'})
            if test_record:
                print("✅ 数据写入验证成功")
                
                # 清理测试数据
                match_collection.delete_one({'source_record_id': 'test_fix_001'})
                print("✅ 测试数据清理完成")
            else:
                print("❌ 数据写入验证失败")
        else:
            print("❌ 批量写入测试失败")
        
        # 修复优化处理器中的批量写入方法
        processor_file = project_root / "src" / "matching" / "optimized_match_processor.py"
        
        if processor_file.exists():
            with open(processor_file, 'r', encoding='utf-8') as f:
                code = f.read()
            
            # 备份
            with open(f"{processor_file}.bulk_fix_backup", 'w', encoding='utf-8') as f:
                f.write(code)
            
            # 查找并替换批量写入方法
            old_method = '''    def _batch_save_optimized_results(self, results: List[Dict]) -> bool:
        """批量保存优化的匹配结果"""
        try:
            collection = self.db_manager.get_collection('unit_match_results')
            
            operations = []
            
            for result in results:
                if result.get('operation') == 'skipped':
                    continue
                
                source_id = result.get('source_record_id')
                if not source_id:
                    continue
                
                # 移除操作标识，不保存到数据库
                result_to_save = {k: v for k, v in result.items() if k not in ['operation', 'previous_result_id']}
                
                # 使用upsert操作，基于source_record_id进行去重
                operations.append({
                    'replaceOne': {
                        'filter': {'source_record_id': source_id},
                        'replacement': result_to_save,
                        'upsert': True
                    }
                })
            
            if operations:
                # 执行批量操作
                bulk_result = collection.bulk_write(operations)
                
                logger.info(f"批量保存结果: 匹配={bulk_result.matched_count}, "
                          f"修改={bulk_result.modified_count}, 插入={bulk_result.upserted_count}")
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"批量保存优化匹配结果失败: {str(e)}")
            return False'''
            
            new_method = '''    def _batch_save_optimized_results(self, results: List[Dict]) -> bool:
        """批量保存优化的匹配结果"""
        try:
            collection = self.db_manager.get_collection('unit_match_results')
            
            operations = []
            
            for result in results:
                if result.get('operation') == 'skipped':
                    continue
                
                source_id = result.get('source_record_id')
                if not source_id:
                    continue
                
                # 移除操作标识，不保存到数据库
                result_to_save = {k: v for k, v in result.items() if k not in ['operation', 'previous_result_id']}
                
                # 数据类型安全处理
                for key, value in result_to_save.items():
                    if key in ['created_time', 'updated_time', 'review_time'] and value is not None:
                        if not isinstance(value, datetime):
                            try:
                                if isinstance(value, str):
                                    result_to_save[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                else:
                                    result_to_save[key] = datetime.now()
                            except:
                                result_to_save[key] = datetime.now()
                    elif key == 'similarity_score' and value is not None:
                        result_to_save[key] = float(value) if value != '' else 0.0
                
                # 使用upsert操作，基于source_record_id进行去重
                operations.append({
                    'replaceOne': {
                        'filter': {'source_record_id': source_id},
                        'replacement': result_to_save,
                        'upsert': True
                    }
                })
            
            if operations:
                # 执行批量操作
                bulk_result = collection.bulk_write(operations)
                
                logger.info(f"批量保存结果: 匹配={bulk_result.matched_count}, "
                          f"修改={bulk_result.modified_count}, 插入={bulk_result.upserted_count}")
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"批量保存优化匹配结果失败: {str(e)}")
            return False'''
            
            if old_method in code:
                code = code.replace(old_method, new_method)
                
                with open(processor_file, 'w', encoding='utf-8') as f:
                    f.write(code)
                
                print("✅ 批量写入方法已修复")
            else:
                print("⚠️ 批量写入方法未找到，可能已经修复")
        
        print("🎉 批量写入操作修复完成！")
        
    except Exception as e:
        print(f"❌ 修复失败: {e}")

if __name__ == "__main__":
    main() 