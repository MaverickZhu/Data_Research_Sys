#!/usr/bin/env python3
"""
创建简化的知识图谱API
用于前端测试，提供基本的示例数据
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def create_simple_api_patch():
    """创建简化的API补丁"""
    
    patch_content = '''
# 简化的知识图谱API补丁
# 在app.py中添加这些路由来替换现有的问题API

@app.route('/api/kg/entities_simple')
def get_kg_entities_simple():
    """获取知识图谱实体API（简化版）"""
    try:
        entity_type = request.args.get('type', 'all')
        limit = min(int(request.args.get('limit', 10)), 100)
        
        sample_entities = [
            {'id': 'org_001', 'label': '北京科技有限公司', 'type': 'ORGANIZATION', 'confidence': 0.95},
            {'id': 'person_001', 'label': '张三', 'type': 'PERSON', 'confidence': 0.88},
            {'id': 'org_002', 'label': '上海教育集团', 'type': 'ORGANIZATION', 'confidence': 0.92},
            {'id': 'location_001', 'label': '北京市朝阳区', 'type': 'LOCATION', 'confidence': 0.90}
        ]
        
        if entity_type != 'all':
            sample_entities = [e for e in sample_entities if e['type'] == entity_type.upper()]
        
        return jsonify({
            'entities': sample_entities[:limit],
            'total': len(sample_entities),
            'status': 'success'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/relations_simple')  
def get_kg_relations_simple():
    """获取知识图谱关系API（简化版）"""
    try:
        limit = min(int(request.args.get('limit', 10)), 100)
        
        sample_relations = [
            {'id': 'rel_001', 'subject': '张三', 'predicate': '工作于', 'object': '北京科技有限公司', 'confidence': 0.95},
            {'id': 'rel_002', 'subject': '北京科技有限公司', 'predicate': '位于', 'object': '北京市朝阳区', 'confidence': 0.88},
            {'id': 'rel_003', 'subject': '上海教育集团', 'predicate': '类型为', 'object': '教育机构', 'confidence': 0.92}
        ]
        
        return jsonify({
            'triples': sample_relations[:limit],
            'total': len(sample_relations),
            'status': 'success'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/search_simple')
def search_kg_simple():
    """知识图谱搜索API（简化版）"""
    try:
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify({'error': '搜索关键词不能为空'}), 400
            
        # 模拟搜索结果
        results = {
            'entities': [
                {'id': 'search_001', 'label': f'搜索结果：{query}相关实体', 'type': 'ORGANIZATION', 'confidence': 0.85}
            ],
            'triples': [
                {'id': 'search_rel_001', 'subject': query, 'predicate': '相关于', 'object': '搜索结果', 'confidence': 0.80}
            ]
        }
        
        return jsonify({
            'query': query,
            'results': results,
            'total_entities': len(results['entities']),
            'total_triples': len(results['triples']),
            'status': 'success'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
'''
    
    print("简化API补丁内容：")
    print(patch_content)
    
    # 保存补丁文件
    with open('kg_api_patch.py', 'w', encoding='utf-8') as f:
        f.write(patch_content)
    
    print("\\n补丁文件已保存为: kg_api_patch.py")
    print("请手动将这些路由添加到src/web/app.py中，或者直接访问简化版API：")
    print("- /api/kg/entities_simple")
    print("- /api/kg/relations_simple") 
    print("- /api/kg/search_simple")

if __name__ == "__main__":
    create_simple_api_patch()
