#!/usr/bin/env python3
"""
实时监控当前知识图谱构建进度
"""

import requests
import time
import json

def monitor_build_progress():
    base_url = 'http://127.0.0.1:18888'
    
    print('🔄 实时监控知识图谱构建进度')
    print('=' * 60)
    print('项目: 排查消监养老机构')
    print('预计处理: 4个数据表')
    print('=' * 60)
    
    last_entities = 0
    last_relations = 0
    start_time = time.time()
    
    try:
        while True:
            # 获取当前数据状态
            response = requests.get(f'{base_url}/api/kg/falkor/stats')
            if response.status_code == 200:
                stats = response.json()
                entities = stats.get('total_entities', 0)
                relations = stats.get('total_relations', 0)
                triples = stats.get('total_triples', 0)
                
                # 计算增长速度
                entities_growth = entities - last_entities
                relations_growth = relations - last_relations
                elapsed_time = int(time.time() - start_time)
                
                # 显示当前状态
                print(f'\r⏰ {elapsed_time:03d}s | '
                      f'📊 实体: {entities:,} (+{entities_growth}) | '
                      f'🔗 关系: {relations:,} (+{relations_growth}) | '
                      f'🔺 三元组: {triples:,}', end='', flush=True)
                
                # 更新记录
                last_entities = entities
                last_relations = relations
                
                # 每10秒换行一次，便于阅读
                if elapsed_time % 10 == 0 and elapsed_time > 0:
                    print()  # 换行
                
                # 检查是否完成（简单判断）
                if entities > 50000 or (entities > 20000 and relations > 10000):
                    print('\n🎉 构建可能已完成或接近完成！')
                    break
                    
            else:
                print(f'\n❌ API错误: {response.status_code}')
                
            time.sleep(2)  # 每2秒检查一次
            
    except KeyboardInterrupt:
        print('\n\n⏹️ 监控已停止')
        elapsed_time = int(time.time() - start_time)
        print(f'📊 监控总时长: {elapsed_time}秒')
        
        # 显示最终状态
        try:
            response = requests.get(f'{base_url}/api/kg/falkor/stats')
            if response.status_code == 200:
                stats = response.json()
                print(f'📈 最终状态: 实体={stats.get("total_entities", 0)}, '
                      f'关系={stats.get("total_relations", 0)}, '
                      f'三元组={stats.get("total_triples", 0)}')
        except:
            pass
    
    except Exception as e:
        print(f'\n❌ 监控过程出错: {e}')

if __name__ == "__main__":
    print('💡 提示: 按 Ctrl+C 停止监控')
    print('🌐 同时可以访问: http://127.0.0.1:18888/kg_viewer_projects')
    print()
    monitor_build_progress()
