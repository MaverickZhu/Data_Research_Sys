#!/usr/bin/env python3
"""
分析知识图谱构建问题
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
import requests
import json

def analyze_kg_issues():
    print('🔍 分析知识图谱构建问题...')
    print('=' * 60)

    try:
        # 初始化配置和数据库连接
        config_manager = ConfigManager()
        db_config = config_manager.get_database_config()
        db_manager = DatabaseManager(config=db_config)
        
        # 检查的表
        tables = ['xp_jxjzdwxx', 'xxj_ssdwjbxx', 'dwd_yljgxx', 'dwd_zzzhzj']
        
        for table in tables:
            print(f'\n📊 分析表: {table}')
            try:
                collection = db_manager.get_collection(table)
                total_count = collection.count_documents({})
                print(f'   总记录数: {total_count}')
                
                if total_count == 0:
                    print('   ⚠️ 表为空，无法进行实体抽取')
                    continue
                
                # 获取多个样本记录查看结构
                samples = list(collection.find().limit(3))
                if samples:
                    sample = samples[0]
                    print(f'   字段数量: {len(sample.keys())}')
                    print(f'   所有字段: {list(sample.keys())}')
                    
                    # 检查文本字段内容质量
                    text_fields = []
                    for key, value in sample.items():
                        if isinstance(value, str) and len(value.strip()) > 0:
                            text_fields.append(key)
                    
                    print(f'   有效文本字段: {text_fields}')
                    
                    # 分析字段内容
                    print('   字段内容示例:')
                    for field in text_fields[:5]:  # 只显示前5个字段
                        values = []
                        for s in samples:
                            value = s.get(field, '')
                            if isinstance(value, str) and len(value.strip()) > 0:
                                preview = value.strip()[:30] + '...' if len(value.strip()) > 30 else value.strip()
                                values.append(preview)
                        
                        if values:
                            print(f'     {field}: {values[0]}')
                            if len(values) > 1:
                                print(f'              {values[1]}')
                    
                    # 检查是否有组织名称相关字段
                    org_fields = [f for f in text_fields if any(keyword in f.lower() for keyword in ['name', '名称', '单位', '机构', '公司', 'dwmc', 'jgmc'])]
                    print(f'   可能的组织名称字段: {org_fields}')
                    
                else:
                    print('   ❌ 无法获取样本数据')
                    
            except Exception as e:
                print(f'   ❌ 表访问失败: {e}')
        
        # 检查当前知识图谱状态
        print(f'\n🎯 当前知识图谱状态:')
        try:
            response = requests.get('http://127.0.0.1:18888/api/kg/falkor/stats')
            if response.status_code == 200:
                stats = response.json()
                print(f'   全局实体: {stats.get("total_entities", 0)}')
                print(f'   全局关系: {stats.get("total_relations", 0)}')
                print(f'   全局三元组: {stats.get("total_triples", 0)}')
            
            # 检查项目特定状态
            project_name = '排查消监养老机构'
            response = requests.get(f'http://127.0.0.1:18888/api/kg/projects/{project_name}/stats?global=false')
            if response.status_code == 200:
                stats = response.json()
                print(f'   项目实体: {stats.get("total_entities", 0)}')
                print(f'   项目关系: {stats.get("total_relations", 0)}')
                print(f'   项目三元组: {stats.get("total_triples", 0)}')
                
        except Exception as e:
            print(f'   API检查失败: {e}')
            
        # 分析问题原因
        print(f'\n🔍 问题分析:')
        print('1. dwd_yljgxx 和 dwd_zzzhzj 表实体抽取为0可能原因:')
        print('   - 数据格式不符合实体抽取规则')
        print('   - 字段名称不在预期范围内') 
        print('   - 数据内容质量问题（空值、格式异常等）')
        print('   - 实体抽取器的字段映射配置问题')
        
        print('\n2. 关系数量为0的可能原因:')
        print('   - 实体间缺乏明确的关联关系')
        print('   - 关系抽取规则过于严格')
        print('   - 数据表之间缺乏关联字段')
        print('   - 关系抽取器配置问题')

    except Exception as e:
        print(f'❌ 分析失败: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    analyze_kg_issues()
