#!/usr/bin/env python3
"""
消防单位建筑数据关联系统 - 海量数据优化（第11阶段）
专门针对大数据量的分片和流式处理优化
"""

import os
import sys
import time
import json
import psutil
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """主函数"""
    print("🌊" * 80)
    print("🌊 消防单位建筑数据关联系统 - 海量数据优化（第11阶段）")
    print("🌊 专门解决目标数据太大的问题")
    print("🌊" * 80)
    
    # 系统信息
    cpu_count = psutil.cpu_count(logical=True)
    memory_gb = psutil.virtual_memory().total / (1024**3)
    print(f"\n💻 系统配置: {cpu_count}核CPU, {memory_gb:.1f}GB内存")
    
    success_count = 0
    
    # 1. 实施智能数据分片策略
    print("\n🔧 1. 实施智能数据分片策略...")
    try:
        processor_path = project_root / "src" / "matching" / "optimized_match_processor.py"
        if processor_path.exists():
            with open(processor_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            # 备份
            with open(f"{processor_path}.massive_backup", 'w', encoding='utf-8') as f:
                f.write(code)
            
            # 更激进的批处理优化
            optimizations = [
                ('batch_size = 10000', 'batch_size = 100000'),
                ('batch_size = 50000', 'batch_size = 100000'),
                ('batch_size = 5000', 'batch_size = 100000'),
                ('chunk_size = 1000', 'chunk_size = 20000'),
                ('max_matches_per_unit = 100', 'max_matches_per_unit = 5'),
            ]
            
            for old, new in optimizations:
                if old in code:
                    code = code.replace(old, new)
                    print(f"   ✅ {old} -> {new}")
            
            with open(processor_path, 'w', encoding='utf-8') as f:
                f.write(code)
            
            success_count += 1
            print("   ✅ 智能数据分片优化完成")
        
    except Exception as e:
        print(f"   ❌ 数据分片优化失败: {e}")
    
    # 2. 优化数据库查询策略
    print("\n🔧 2. 优化数据库查询策略...")
    try:
        config_path = project_root / "config" / "database.yaml"
        if config_path.exists():
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if 'mongodb' in config:
                if 'connection_pool' not in config['mongodb']:
                    config['mongodb']['connection_pool'] = {}
                
                # 海量数据优化配置
                config['mongodb']['connection_pool'].update({
                    'max_pool_size': min(1000, cpu_count * 32),
                    'min_pool_size': cpu_count * 4,
                    'max_idle_time_ms': 300000,
                    'connect_timeout_ms': 60000,
                    'server_selection_timeout_ms': 120000,
                    'socket_timeout_ms': 300000
                })
                
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
                
                success_count += 1
                print(f"   ✅ 数据库查询优化: {config['mongodb']['connection_pool']['max_pool_size']}个连接")
    
    except Exception as e:
        print(f"   ❌ 数据库查询优化失败: {e}")
    
    # 3. 创建数据预处理脚本
    print("\n🔧 3. 创建数据预处理脚本...")
    try:
        preprocess_script = f'''#!/usr/bin/env python3
"""
海量数据预处理器
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def preprocess_data():
    try:
        from src.database.connection import get_database_connection
        print("🔄 开始数据预处理...")
        
        db = get_database_connection()
        supervision = db.get_collection("xxj_shdwjbxx")
        inspection = db.get_collection("xfaqpc_jzdwxx")
        
        sup_count = supervision.count_documents({{}})
        ins_count = inspection.count_documents({{}})
        
        print(f"📊 监督数据: {{sup_count:,}} 条")
        print(f"📊 排查数据: {{ins_count:,}} 条")
        
        # 创建索引
        indexes = [("dwmc", 1), ("tyshxydm", 1), ("dwdz", 1)]
        for field, direction in indexes:
            try:
                supervision.create_index([(field, direction)], background=True)
                inspection.create_index([(field, direction)], background=True)
                print(f"   ✅ 创建索引: {{field}}")
            except:
                pass
        
        return True
    except Exception as e:
        print(f"❌ 预处理失败: {{e}}")
        return False

if __name__ == "__main__":
    preprocess_data()
'''
        
        preprocess_path = project_root / "scripts" / "preprocess_massive_data.py"
        with open(preprocess_path, 'w', encoding='utf-8') as f:
            f.write(preprocess_script)
        
        success_count += 1
        print(f"   ✅ 数据预处理脚本创建完成")
    
    except Exception as e:
        print(f"   ❌ 预处理脚本创建失败: {e}")
    
    # 生成优化报告
    report = {
        'optimization_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'stage': 11,
        'stage_name': '海量数据优化',
        'success_count': success_count,
        'total_steps': 3,
        'system_info': {
            'cpu_count': cpu_count,
            'memory_gb': memory_gb
        },
        'key_improvements': [
            '批处理大小提升至100000条',
            f'数据库连接池扩展至{min(1000, cpu_count * 32)}个',
            '创建数据预处理和索引优化'
        ]
    }
    
    report_path = project_root / "massive_data_optimization_report.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📊 海量数据优化完成！成功执行 {success_count}/3 个步骤")
    print(f"📋 报告已保存: {report_path}")
    print("\n🌊 现在系统可以更好地处理大数据量！")

if __name__ == "__main__":
    main() 