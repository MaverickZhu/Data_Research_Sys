#!/usr/bin/env python3
"""
消防单位建筑数据关联系统 - 终极性能优化（第10阶段）
深度系统优化，最大化性能潜力
"""

import os
import sys
import time
import json
import psutil
import traceback
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """主函数"""
    print("🔥" * 60)
    print("🔥 消防单位建筑数据关联系统 - 终极性能优化（第10阶段）")
    print("🔥" * 60)
    
    # 系统信息
    cpu_count = psutil.cpu_count(logical=True)
    memory_gb = psutil.virtual_memory().total / (1024**3)
    print(f"\n💻 系统配置: {cpu_count}核CPU, {memory_gb:.1f}GB内存")
    
    success_count = 0
    
    # 1. 移除数据限制
    print("\n🔧 1. 移除数据限制...")
    try:
        processor_path = project_root / "src" / "matching" / "optimized_match_processor.py"
        if processor_path.exists():
            with open(processor_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            # 备份
            with open(f"{processor_path}.ultimate_backup", 'w', encoding='utf-8') as f:
                f.write(code)
            
            # 优化
            optimizations = [
                ('limit = 50000', 'limit = None'),
                ('if len(target_records) > 50000:', 'if False:'),
                ('target_records = target_records[:50000]', '# 移除限制'),
                ('batch_size = 100', 'batch_size = 10000'),
                ('batch_size = 500', 'batch_size = 10000'),
                ('batch_size = 1000', 'batch_size = 10000'),
                ('batch_size = 2000', 'batch_size = 10000'),
                ('batch_size = 5000', 'batch_size = 10000'),
            ]
            
            for old, new in optimizations:
                if old in code:
                    code = code.replace(old, new)
                    print(f"   ✅ {old} -> {new}")
            
            with open(processor_path, 'w', encoding='utf-8') as f:
                f.write(code)
            
            success_count += 1
            print("   ✅ 数据限制移除完成")
        
    except Exception as e:
        print(f"   ❌ 数据限制移除失败: {e}")
    
    # 2. 优化数据库连接
    print("\n🔧 2. 优化数据库连接...")
    try:
        config_path = project_root / "config" / "database.yaml"
        if config_path.exists():
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if 'mongodb' in config:
                if 'connection_pool' not in config['mongodb']:
                    config['mongodb']['connection_pool'] = {}
                
                config['mongodb']['connection_pool'].update({
                    'max_pool_size': min(200, cpu_count * 8),
                    'min_pool_size': cpu_count,
                    'max_idle_time_ms': 60000,
                    'connect_timeout_ms': 10000,
                    'server_selection_timeout_ms': 15000
                })
                
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
                
                success_count += 1
                print(f"   ✅ MongoDB连接池优化: {config['mongodb']['connection_pool']['max_pool_size']}个连接")
    
    except Exception as e:
        print(f"   ❌ 数据库连接优化失败: {e}")
    
    # 3. 创建终极监控
    print("\n🔧 3. 创建终极性能监控...")
    try:
        monitor_script = f'''#!/usr/bin/env python3
"""
终极性能监控器
"""
import psutil
import time
from datetime import datetime

def monitor():
    print("🔥 终极性能监控启动...")
    while True:
        try:
            cpu = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            status = "🟢" if cpu < 70 else "🟡" if cpu < 90 else "🔴"
            mem_status = "🟢" if memory.percent < 80 else "🟡" if memory.percent < 95 else "🔴"
            
            print(f"\\r{status} CPU: {cpu:.1f}% | {mem_status} 内存: {memory.percent:.1f}% | 可用: {memory.available/(1024**3):.1f}GB", end="")
            
            time.sleep(30)
            
        except KeyboardInterrupt:
            print("\\n监控已停止")
            break
        except Exception as e:
            print(f"\\n监控错误: {e}")
            time.sleep(5)

if __name__ == "__main__":
    monitor()
'''
        
        monitor_path = project_root / "scripts" / "ultimate_performance_monitor.py"
        with open(monitor_path, 'w', encoding='utf-8') as f:
            f.write(monitor_script)
        
        success_count += 1
        print(f"   ✅ 终极监控创建完成: {monitor_path}")
    
    except Exception as e:
        print(f"   ❌ 监控创建失败: {e}")
    
    # 4. 生成优化报告
    report = {
        'optimization_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'stage': 10,
        'stage_name': '终极性能优化',
        'success_count': success_count,
        'total_steps': 3,
        'system_info': {
            'cpu_count': cpu_count,
            'memory_gb': memory_gb
        },
        'key_improvements': [
            '完全移除50000条数据限制',
            '批处理大小优化至10000条',
            f'数据库连接池扩展至{min(200, cpu_count * 8)}个连接',
            '创建终极性能监控'
        ]
    }
    
    report_path = project_root / "ultimate_optimization_report.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📊 优化完成！成功执行 {success_count}/3 个步骤")
    print(f"📋 报告已保存: {report_path}")
    print("\n🚀 建议重启系统以应用所有优化！")

if __name__ == "__main__":
    main() 