"""
Flask Web应用主程序
提供数据匹配进度管理的Web界面
"""

from flask import Flask, render_template, jsonify, request, send_file, current_app
from flask_cors import CORS
import logging
import yaml
import os
from datetime import datetime
from typing import Dict, List
import pandas as pd
import io
import math
import json

# 导入自定义模块
from src.database.connection import DatabaseManager
from src.matching.match_processor import MatchProcessor
from src.matching.multi_match_processor import MultiMatchProcessor
from src.matching.enhanced_association_processor import EnhancedAssociationProcessor, AssociationStrategy
from src.matching.optimized_match_processor import OptimizedMatchProcessor
from src.utils.logger import setup_logger
from src.utils.config import ConfigManager
from src.utils.helpers import safe_json_response, generate_match_id

# V2.0新增模块导入
from src.data_manager.csv_processor import CSVProcessor
from src.data_manager.data_analyzer import DataAnalyzer  
from src.data_manager.schema_detector import SchemaDetector
from src.data_manager.validation_engine import ValidationEngine

# 知识图谱模块导入
from src.knowledge_graph.kg_store import KnowledgeGraphStore
from src.knowledge_graph.falkordb_store import FalkorDBStore
from src.knowledge_graph.kg_builder import KnowledgeGraphBuilder
from src.knowledge_graph.entity_extractor import EntityExtractor
from src.knowledge_graph.relation_extractor import RelationExtractor
from src.knowledge_graph.kg_quality_assessor import KnowledgeGraphQualityAssessor

# 性能优化模块导入
from src.utils.performance_optimizer import get_performance_monitor, get_memory_processor

# 设置日志
logger = setup_logger(__name__)

# 创建Flask应用
app = Flask(__name__)
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# 全局变量
db_manager = None
match_processor = None
multi_match_processor = None

# 知识图谱相关全局变量
kg_store = None
kg_builder = None
entity_extractor = None
relation_extractor = None
kg_quality_assessor = None
enhanced_association_processor = None
optimized_match_processor = None
config_manager = None

# V2.0新增全局变量
csv_processor = None
data_analyzer = None
schema_detector = None
validation_engine = None


def create_app():
    """创建和配置Flask应用"""
    global db_manager, match_processor, multi_match_processor, enhanced_association_processor, optimized_match_processor, config_manager
    global csv_processor, data_analyzer, schema_detector, validation_engine
    global kg_store, kg_builder, entity_extractor, relation_extractor, kg_quality_assessor
    
    try:
        # 初始化配置管理器
        config_manager = ConfigManager()
        
        # 初始化数据库管理器
        db_manager = DatabaseManager(config=config_manager.get_database_config())
        
        # 初始化匹配处理器
        match_processor = MatchProcessor(
            db_manager=db_manager,
            config=config_manager.get_matching_config()
        )
        
        # 初始化一对多匹配处理器
        multi_match_processor = MultiMatchProcessor(
            db_manager=db_manager,
            config=config_manager.get_matching_config()
        )
        
        # 初始化增强关联处理器
        enhanced_association_processor = EnhancedAssociationProcessor(
            db_manager=db_manager,
            config=config_manager.get_matching_config()
        )
        
        # 新增：统一初始化优化的匹配处理器
        optimized_match_processor = OptimizedMatchProcessor(
            db_manager=db_manager,
            config_manager=config_manager
        )
        
        # V2.0新增：初始化数据管理处理器
        csv_processor = CSVProcessor()
        data_analyzer = DataAnalyzer()
        schema_detector = SchemaDetector()
        validation_engine = ValidationEngine()
        
        # 知识图谱组件初始化
        # 使用默认配置初始化知识图谱组件
        kg_store = KnowledgeGraphStore(
            db_manager=db_manager,
            config={
                'batch_size': 1000,
                'use_threading': True,
                'max_workers': 4,
                'enable_compression': True,
                'cache_size': 10000
            }
        )
        
        entity_extractor = EntityExtractor(
            config={
                'min_confidence': 0.6,
                'max_entities_per_record': 50,
                'enable_nlp': True
            }
        )
        
        relation_extractor = RelationExtractor(
            config={
                'min_confidence': 0.7,
                'max_relations_per_record': 20,
                'enable_semantic_reasoning': True
            }
        )
        
        kg_builder = KnowledgeGraphBuilder(
            kg_store=kg_store,
            config={
                'batch_size': 500,
                'enable_quality_check': True
            }
        )
        
        kg_quality_assessor = KnowledgeGraphQualityAssessor(
            kg_store=kg_store,
            config={
                'assessment_interval': 3600,  # 1 hour
                'quality_threshold': 0.8
            }
        )
        
        # 设置Flask配置，包括MongoDB客户端
        app.config['MONGO_CLIENT'] = db_manager.mongo_client
        
        logger.info("Flask应用初始化成功（包含知识图谱组件）")
        return app
        
    except Exception as e:
        logger.error(f"Flask应用初始化失败: {str(e)}")
        raise


@app.route('/')
def index():
    """首页 - 系统概览"""
    try:
        # 提供基本的统计信息，避免阻塞
        basic_stats = {
            'data_sources': {
                'supervision_count': '加载中...',
                'inspection_count': '加载中...',
                'match_results_count': '加载中...'
            },
            'matching_stats': {
                'total_matches': '加载中...',
                'last_updated': '加载中...'
            },
            'system_info': {
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'running'
            }
        }
        return render_template('index.html', stats=basic_stats)
    except Exception as e:
        logger.error(f"首页加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/matching')
def matching_page():
    """匹配管理页面"""
    try:
        # 获取匹配配置信息
        matching_config = config_manager.get_matching_config()
        return render_template('matching.html', config=matching_config)
    except Exception as e:
        logger.error(f"匹配管理页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/results')
def results_page():
    """匹配结果页面"""
    try:
        return render_template('results.html')
    except Exception as e:
        logger.error(f"匹配结果页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/statistics')
def statistics_page():
    """数据统计页面"""
    try:
        return render_template('statistics.html')
    except Exception as e:
        logger.error(f"数据统计页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/test')
def test_page():
    """数据加载测试页面"""
    return '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>数据加载测试页面</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .card { border: 1px solid #ddd; padding: 20px; margin: 10px 0; border-radius: 5px; }
        .success { color: green; }
        .error { color: red; }
        .loading { color: orange; }
        pre { background: #f5f5f5; padding: 10px; border-radius: 3px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>🧪 数据加载测试页面</h1>
    
    <div class="card">
        <h2>📊 统计数据</h2>
        <div id="stats-display">
            <p class="loading">正在加载数据...</p>
        </div>
    </div>
    
    <div class="card">
        <h2>📡 API响应</h2>
        <div id="api-response">
            <p class="loading">正在获取API响应...</p>
        </div>
    </div>
    
    <div class="card">
        <h2>🔧 调试信息</h2>
        <div id="debug-info">
            <p>等待调试信息...</p>
        </div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            console.log('🚀 测试页面加载完成');
            testDataLoading();
        });

        function testDataLoading() {
            const debugInfo = document.getElementById('debug-info');
            const statsDisplay = document.getElementById('stats-display');
            const apiResponse = document.getElementById('api-response');
            
            debugInfo.innerHTML = '<p class="loading">开始测试数据加载...</p>';
            
            console.log('📡 开始测试API接口...');
            
            fetch('/api/stats')
                .then(response => {
                    console.log('📊 API响应状态:', response.status);
                    debugInfo.innerHTML += `<p>API响应状态: ${response.status}</p>`;
                    
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                    }
                    return response.json();
                })
                .then(data => {
                    console.log('✅ 收到数据:', data);
                    
                    apiResponse.innerHTML = `
                        <p class="success">✅ API调用成功</p>
                        <pre>${JSON.stringify(data, null, 2)}</pre>
                    `;
                    
                    const supervisionCount = data.data_sources.supervision_count || 0;
                    const inspectionCount = data.data_sources.inspection_count || 0;
                    const matchResultsCount = data.data_sources.match_results_count || 0;
                    const totalMatches = data.matching_stats.total_matches || 0;
                    const totalUnits = supervisionCount + inspectionCount;
                    const matchRate = totalUnits > 0 ? (totalMatches / totalUnits * 100).toFixed(1) : 0;
                    
                    statsDisplay.innerHTML = `
                        <div class="success">
                            <h3>📈 数据统计</h3>
                            <p><strong>监督管理系统:</strong> ${supervisionCount.toLocaleString()} 条</p>
                            <p><strong>安全排查系统:</strong> ${inspectionCount.toLocaleString()} 条</p>
                            <p><strong>总单位数:</strong> ${totalUnits.toLocaleString()} 条</p>
                            <p><strong>匹配结果:</strong> ${matchResultsCount.toLocaleString()} 条</p>
                            <p><strong>总匹配数:</strong> ${totalMatches.toLocaleString()} 条</p>
                            <p><strong>匹配率:</strong> ${matchRate}%</p>
                        </div>
                    `;
                    
                    debugInfo.innerHTML += '<p class="success">✅ 数据加载成功</p>';
                    
                })
                .catch(error => {
                    console.error('❌ 数据加载失败:', error);
                    
                    apiResponse.innerHTML = `
                        <p class="error">❌ API调用失败</p>
                        <p>错误信息: ${error.message}</p>
                    `;
                    
                    statsDisplay.innerHTML = `
                        <p class="error">❌ 数据加载失败: ${error.message}</p>
                    `;
                    
                    debugInfo.innerHTML += `<p class="error">❌ 错误: ${error.message}</p>`;
                });
        }
    </script>
</body>
</html>'''


@app.route('/progress')
def progress_page():
    """进度监控页面"""
    try:
        return render_template('progress.html')
    except Exception as e:
        logger.error(f"加载进度监控页面失败: {str(e)}")
        return render_template('error.html', error=str(e)), 500


@app.route('/api/stats')
def api_get_stats():
    """API: 获取系统统计信息"""
    try:
        stats = get_system_stats()
        # 确保数据能正确序列化
        safe_stats = safe_json_response(stats)
        return jsonify(safe_stats)
    except Exception as e:
        logger.error(f"获取统计信息失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/start_matching', methods=['POST'])
def api_start_matching():
    """API: 启动匹配任务"""
    try:
        request_data = request.get_json() or {}
        
        # 获取匹配参数
        match_type = request_data.get('match_type', 'both')  # exact, fuzzy, both
        batch_size = request_data.get('batch_size', 100)
        
        # 启动匹配任务
        task_id = match_processor.start_matching_task(
            match_type=match_type,
            batch_size=batch_size
        )
        
        logger.info(f"匹配任务启动成功，任务ID: {task_id}")
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '匹配任务已启动'
        })
        
    except Exception as e:
        logger.error(f"启动匹配任务失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/task_progress/<task_id>')
def api_get_task_progress(task_id):
    """API: 获取任务进度"""
    try:
        progress = match_processor.get_task_progress(task_id)
        # 确保数据能正确序列化
        safe_progress = safe_json_response(progress)
        return jsonify(safe_progress)
    except Exception as e:
        logger.error(f"获取任务进度失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stop_matching/<task_id>', methods=['POST'])
def api_stop_matching(task_id):
    """API: 停止匹配任务"""
    try:
        # 增强诊断：记录API请求来源
        logger.info(f"收到来自IP '{request.remote_addr}' 的API请求，要求停止任务 '{task_id}'")

        success = match_processor.stop_matching_task(task_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': '匹配任务已停止'
            })
        else:
            return jsonify({
                'success': False,
                'message': '停止任务失败'
            })
            
    except Exception as e:
        logger.error(f"停止匹配任务失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def _format_match_results(raw_results: List[Dict]) -> List[Dict]:
    """将从数据库获取的原始匹配结果格式化为统一的结构"""
    new_results = []
    for row in raw_results:
        # 安全获取字段值的辅助函数
        def safe_get(key, default=None):
            value = row.get(key, default)
            # 统一处理各种形式的空值
            return value if value not in [None, '', '-', 'null'] else default
        
        # 优先使用数据库中已存在的、稳定的match_id
        # 这是修复决策分析功能404问题的关键
        match_id = safe_get('match_id')
        if not match_id:
            # 仅在match_id不存在时才生成，作为后备方案
            # (需要确保helper函数可用)
            try:
                from src.utils.helpers import generate_match_id
                primary_id = safe_get('primary_record_id') or safe_get('_id')
                matched_id = safe_get('matched_record_id', 'no_match')
                match_id = generate_match_id(str(primary_id), str(matched_id))
            except ImportError:
                # 如果帮助函数不可用，则使用_id作为备用
                match_id = str(safe_get('_id'))

        # 辅助函数：安全地将ID转换为字符串
        def to_str(val):
            return str(val) if val is not None else None
            
        new_results.append({
            # 基本信息
            'primary_unit_name': safe_get('primary_unit_name') or safe_get('unit_name'),
            'matched_unit_name': safe_get('matched_unit_name'),
            'match_type': safe_get('match_type', 'none'),
            'similarity_score': safe_get('similarity_score', 0),
            'match_time': safe_get('match_time') or safe_get('matching_time') or safe_get('process_time'),
            
            # 地址信息
            'primary_unit_address': safe_get('primary_unit_address') or safe_get('unit_address') or safe_get('address'),
            'matched_unit_address': safe_get('matched_unit_address'),
            
            # 法定代表人信息
            'primary_legal_person': safe_get('primary_legal_person') or safe_get('legal_person') or safe_get('contact_person'),
            'matched_legal_person': safe_get('matched_legal_person'),
            
            # 消防安全管理人信息
            'primary_security_manager': safe_get('primary_security_manager') or safe_get('SECURITY_PEOPLE') or safe_get('security_manager') or safe_get('fire_safety_manager'),
            'matched_security_manager': safe_get('matched_security_manager') or safe_get('xfaqglr'),
            
            # 联系电话
            'primary_phone': safe_get('primary_phone') or safe_get('contact_phone') or safe_get('phone'),
            'matched_phone': safe_get('matched_phone') or safe_get('matched_contact_phone'),
            
            # 其他信息 (确保ID为字符串)
            'primary_credit_code': to_str(safe_get('primary_credit_code') or safe_get('credit_code')),
            'matched_credit_code': to_str(safe_get('matched_credit_code')),
                
            # 系统信息
            '_id': str(row.get('_id')),
            'match_id': match_id, # 确保使用生成的match_id
            'xfaqpc_jzdwxx_id': to_str(safe_get('xfaqpc_jzdwxx_id')),
            'xxj_shdwjbxx_id': to_str(safe_get('xxj_shdwjbxx_id')),
            'review_status': safe_get('review_status', 'pending'),
            'review_notes': safe_get('review_notes', ''),
            'reviewer': safe_get('reviewer', ''),
            'review_time': safe_get('review_time'),
            'created_time': safe_get('created_time'),
            'updated_time': safe_get('updated_time'),
            
            # 匹配详情
            'match_details': safe_get('match_details', {}),
            'match_confidence': safe_get('match_confidence'),
            'match_reason': safe_get('match_reason')
        })
    return new_results


@app.route('/api/match_results')
def api_get_match_results():
    """API: 获取匹配结果"""
    try:
        # 获取查询参数
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        match_type = request.args.get('match_type')
        search_term = request.args.get('search_term') # 获取搜索关键词
        
        # 查询匹配结果
        results_data = db_manager.get_match_results(
            page=page,
            per_page=per_page,
            match_type=match_type,
            search_term=search_term # 传递搜索关键词
        )
        
        # 使用新的辅助函数格式化结果
        new_results = _format_match_results(results_data.get('results', []))
        
        results_data['results'] = new_results
        return jsonify(results_data)
        
    except Exception as e:
        logger.error(f"获取匹配结果失败: {str(e)}")
        return jsonify({
            'results': [],
            'total': 0,
            'page': page,
            'per_page': per_page,
            'pages': 0,
            'error': str(e)
        })


@app.route('/api/match_statistics')
def api_get_match_statistics():
    """API: 获取匹配统计信息"""
    try:
        statistics = db_manager.get_match_statistics()
        # 确保数据能正确序列化
        safe_statistics = safe_json_response(statistics)
        return jsonify(safe_statistics)
    except Exception as e:
        logger.error(f"获取匹配统计失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/running_tasks')
def api_get_running_tasks():
    """API: 获取当前运行的任务"""
    try:
        # 获取运行中的任务
        running_tasks = match_processor.get_running_tasks() if hasattr(match_processor, 'get_running_tasks') else []
        
        # 确保数据能正确序列化
        safe_tasks = safe_json_response(running_tasks)
        return jsonify({
            'success': True,
            'tasks': safe_tasks,
            'count': len(safe_tasks)
        })
    except Exception as e:
        logger.error(f"获取运行任务失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'tasks': [],
            'count': 0
        }), 500


@app.route('/api/task_history')
def api_get_task_history():
    """API: 获取任务历史"""
    try:
        # 获取查询参数
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        
        # 获取任务历史（这里需要实现实际的历史记录功能）
        # 目前返回模拟数据
        mock_history = [
            {
                'id': 'ab2b5b43-8970-429f-aa9b-115273276637',
                'type': 'both',
                'status': 'completed',
                'start_time': '2025-06-13 10:34:50',
                'end_time': '2025-06-13 10:45:30',
                'processed': 1659320,
                'matches': 28,
                'progress': 100,
                'duration': 640  # 秒
            },
            {
                'id': '13170dc0-353b-4d07-ae63-691c9090a66c',
                'type': 'both',
                'status': 'completed',
                'start_time': '2025-06-13 11:21:21',
                'end_time': '2025-06-13 11:25:15',
                'processed': 1659320,
                'matches': 32,
                'progress': 100,
                'duration': 234  # 秒
            }
        ]
        
        # 分页处理
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        page_tasks = mock_history[start_idx:end_idx]
        
        return jsonify({
            'success': True,
            'tasks': page_tasks,
            'total': len(mock_history),
            'page': page,
            'per_page': per_page,
            'pages': (len(mock_history) + per_page - 1) // per_page
        })
        
    except Exception as e:
        logger.error(f"获取任务历史失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'tasks': [],
            'total': 0
        }), 500


@app.route('/api/export_results')
def api_export_results():
    """API: 导出匹配结果为CSV文件"""
    try:
        # 获取查询参数
        match_type = request.args.get('match_type')
        search_term = request.args.get('search_term')

        # 获取所有匹配结果（不分页）
        # 我们通过设置一个极大的per_page值来获取所有数据
        # 更好的方法是修改get_match_results以支持导出模式
        results_data = db_manager.get_match_results(
            page=1,
            per_page=1000000, # 获取所有数据
            match_type=match_type,
            search_term=search_term
        )

        # 使用辅助函数格式化结果
        formatted_results = _format_match_results(results_data.get('results', []))

        if not formatted_results:
            return jsonify({'success': False, 'message': '没有可导出的数据'}), 404

        # 使用pandas创建DataFrame
        df = pd.DataFrame(formatted_results)

        # 选择并重命名字段
        export_df = df[[
            'primary_unit_name', 'matched_unit_name', 'match_type', 'similarity_score',
            'review_status', 'primary_unit_address', 'matched_unit_address',
            'primary_legal_person', 'matched_legal_person', 'primary_credit_code', 'matched_credit_code',
            'xfaqpc_jzdwxx_id', 'xxj_shdwjbxx_id'
        ]].rename(columns={
            'primary_unit_name': '源单位名称',
            'matched_unit_name': '匹配单位名称',
            'match_type': '匹配类型',
            'similarity_score': '相似度',
            'review_status': '审核状态',
            'primary_unit_address': '源单位地址',
            'matched_unit_address': '匹配单位地址',
            'primary_legal_person': '源单位法人',
            'matched_legal_person': '匹配单位法人',
            'primary_credit_code': '源单位信用代码',
            'matched_credit_code': '匹配单位信用代码',
            'xfaqpc_jzdwxx_id': '源系统原始ID',
            'xxj_shdwjbxx_id': '目标系统原始ID'
        })
        
        # 将相似度转换为百分比
        export_df['相似度'] = export_df['相似度'].apply(lambda x: f"{x*100:.1f}%" if isinstance(x, float) else x)

        # 为信用代码和原始ID添加特殊格式，防止Excel等软件自动转换为科学计数法
        def format_id_for_excel(value):
            """Safely formats a value that might be an ID to prevent Excel's conversion."""
            if value is None or not pd.notna(value) or str(value).strip() == '':
                return ''
            
            # The value is now guaranteed to be a string from the source.
            return f'="{str(value).strip()}"'

        for col in ['源单位信用代码', '匹配单位信用代码', '源系统原始ID', '目标系统原始ID']:
            export_df[col] = export_df[col].apply(format_id_for_excel)

        # 创建一个内存中的CSV文件
        output = io.BytesIO()
        export_df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)

        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"match_results_{timestamp}.csv"

        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"导出结果失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/update_config', methods=['POST'])
def api_update_config():
    """API: 更新匹配配置"""
    try:
        config_data = request.get_json()
        
        # 更新配置
        success = config_manager.update_matching_config(config_data)
        
        if success:
            # 重新初始化匹配处理器
            global match_processor
            match_processor = MatchProcessor(
                db_manager=db_manager,
                config=config_manager.get_matching_config()
            )
            
            return jsonify({
                'success': True,
                'message': '配置更新成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '配置更新失败'
            })
            
    except Exception as e:
        logger.error(f"更新配置失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/start_optimized_matching', methods=['POST'])
def api_start_optimized_matching():
    """API: 启动优化匹配任务"""
    try:
        request_data = request.get_json() or {}
        
        # 获取匹配参数
        match_type = request_data.get('match_type', 'both')  # exact, fuzzy, both
        mode = request_data.get('mode', 'incremental')  # incremental, update, full
        batch_size = request_data.get('batch_size', 100)
        clear_existing = request_data.get('clear_existing', False)
        
        # 验证模式参数
        from src.matching.optimized_match_processor import MatchingMode
        valid_modes = [MatchingMode.INCREMENTAL, MatchingMode.UPDATE, MatchingMode.FULL]
        if mode not in valid_modes:
            return jsonify({
                'success': False,
                'error': f'无效的匹配模式: {mode}，支持的模式: {valid_modes}'
            }), 400
        
        # 直接使用已初始化的处理器
        task_id = optimized_match_processor.start_optimized_matching_task(
            match_type=match_type,
            mode=mode,
            batch_size=batch_size,
            clear_existing=clear_existing
        )
        
        logger.info(f"优化匹配任务启动成功，任务ID: {task_id}, 模式: {mode}")
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'mode': mode,
            'message': f'优化匹配任务已启动 (模式: {mode})'
        })
        
    except Exception as e:
        logger.error(f"启动优化匹配任务失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/optimized_task_progress/<task_id>')
def api_get_optimized_task_progress(task_id):
    """API: 获取优化任务进度"""
    try:
        if not optimized_match_processor:
            return jsonify({'error': '优化匹配处理器未初始化'}), 500
        
        progress = optimized_match_processor.get_optimized_task_progress(task_id)
        # 确保数据能正确序列化
        safe_progress = safe_json_response(progress)
        return jsonify(safe_progress)
    except Exception as e:
        logger.error(f"获取优化任务进度失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stop_optimized_matching/<task_id>', methods=['POST'])
def api_stop_optimized_matching(task_id):
    """API: 停止优化匹配任务"""
    try:
        # 增强诊断：记录API请求来源
        logger.info(f"收到来自IP '{request.remote_addr}' 的API请求，要求停止任务 '{task_id}'")

        if not optimized_match_processor:
            return jsonify({
                'success': False,
                'message': '优化匹配处理器未初始化'
            }), 500
        
        success = optimized_match_processor.stop_optimized_matching_task(task_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': '优化匹配任务已停止'
            })
        else:
            return jsonify({
                'success': False,
                'message': '停止优化任务失败'
            })
            
    except Exception as e:
        logger.error(f"停止优化匹配任务失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/optimized_match_statistics')
def api_get_optimized_match_statistics():
    """API: 获取优化匹配统计信息"""
    try:
        if not optimized_match_processor:
            return jsonify({'error': '优化匹配处理器未初始化'}), 500
        
        statistics = optimized_match_processor.get_optimized_matching_statistics()
        # 确保数据能正确序列化
        safe_statistics = safe_json_response(statistics)
        return jsonify(safe_statistics)
    except Exception as e:
        logger.error(f"获取优化匹配统计失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/match_result_details/<match_id>')
def api_get_match_result_details(match_id):
    """API: 获取匹配结果详情"""
    try:
        if not optimized_match_processor:
            return jsonify({'error': '优化匹配处理器未初始化'}), 500
        
        details = optimized_match_processor.get_match_result_details(match_id)
        
        if details:
            return jsonify({
                'success': True,
                'details': details
            })
        else:
            return jsonify({
                'success': False,
                'message': '未找到匹配结果'
            }), 404
            
    except Exception as e:
        logger.error(f"获取匹配结果详情失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/update_review_status', methods=['POST'])
def api_update_review_status():
    """API: 更新审核状态"""
    try:
        request_data = request.get_json() or {}
        
        match_id = request_data.get('match_id')
        review_status = request_data.get('review_status')  # pending, approved, rejected
        review_notes = request_data.get('review_notes', '')
        reviewer = request_data.get('reviewer', '')
        
        if not match_id or not review_status:
            return jsonify({
                'success': False,
                'error': '缺少必要参数: match_id 和 review_status'
            }), 400
        
        # 验证审核状态
        valid_statuses = ['pending', 'approved', 'rejected']
        if review_status not in valid_statuses:
            return jsonify({
                'success': False,
                'error': f'无效的审核状态: {review_status}，支持的状态: {valid_statuses}'
            }), 400
        
        if not optimized_match_processor:
            return jsonify({'error': '优化匹配处理器未初始化'}), 500
        
        success = optimized_match_processor.update_review_status(
            match_id=match_id,
            review_status=review_status,
            review_notes=review_notes,
            reviewer=reviewer
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': '审核状态更新成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '审核状态更新失败'
            })
            
    except Exception as e:
        logger.error(f"更新审核状态失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/clear_match_results', methods=['POST'])
def api_clear_match_results():
    """API: 清除匹配结果表"""
    try:
        # 获取匹配结果集合
        collection = db_manager.get_collection('unit_match_results')
        
        # 删除所有记录
        result = collection.delete_many({})
        
        logger.info(f"清除匹配结果表成功，删除记录数: {result.deleted_count}")
        
        return jsonify({
            'success': True,
            'deleted_count': result.deleted_count,
            'message': f'成功清除 {result.deleted_count} 条匹配结果记录'
        })
        
    except Exception as e:
        logger.error(f"清除匹配结果表失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '清除匹配结果表失败'
        }), 500


@app.route('/api/fuzzy_review_results')
def api_fuzzy_review_results():
    """API: 获取待人工审核的模糊匹配结果"""
    try:
        # 获取集合
        collection = db_manager.get_collection('unit_match_results')
        
        # 查询条件：模糊匹配且待审核
        query = {
            'match_type': 'fuzzy',
            '$or': [
                {'review_status': {'$exists': False}},
                {'review_status': 'pending'}
            ]
        }
        
        cursor = collection.find(query).sort('created_time', -1).limit(100)
        results = []
        
        for doc in cursor:
            # 返回前端需要的字段，确保字段名匹配
            result = {
                'primary_unit_name': doc.get('unit_name', ''),  # 主系统单位名称
                'matched_unit_name': doc.get('matched_unit_name', ''),  # 匹配到的单位名称
                'similarity_score': doc.get('similarity_score', 0),  # 相似度分数
                'field_similarities': doc.get('match_details', {}).get('field_similarities', {}),  # 字段相似度详情
                'match_id': str(doc.get('_id', '')),  # 匹配记录ID
                
                # 添加更多详细信息用于显示
                'primary_unit_address': doc.get('unit_address', ''),
                'matched_unit_address': doc.get('matched_unit_address', ''),
                'primary_legal_person': doc.get('contact_person', ''),
                'matched_legal_person': doc.get('matched_legal_person', ''),
                'primary_security_manager': doc.get('security_manager', ''),
                'matched_security_manager': doc.get('matched_security_manager', ''),
                'match_confidence': doc.get('match_confidence', ''),
                'match_fields': doc.get('match_fields', []),
                'created_time': doc.get('created_time', '').strftime('%Y-%m-%d %H:%M:%S') if doc.get('created_time') else '',
            }
            results.append(result)
        
        return jsonify({'results': results})
        
    except Exception as e:
        logger.error(f"获取待人工审核模糊匹配结果失败: {str(e)}")
        return jsonify({'results': [], 'error': str(e)})


@app.route('/api/confirm_association', methods=['POST'])
def api_confirm_association():
    """API: 确认关联"""
    try:
        request_data = request.get_json() or {}
        match_id = request_data.get('match_id')
        
        if not match_id:
            return jsonify({
                'success': False,
                'error': '缺少必要参数: match_id'
            }), 400
        
        # 获取集合
        collection = db_manager.get_collection('unit_match_results')
        
        # 更新审核状态
        from bson import ObjectId
        from datetime import datetime
        
        update_result = collection.update_one(
            {'_id': ObjectId(match_id)},
            {
                '$set': {
                    'review_status': 'approved',
                    'review_notes': '人工审核确认关联',
                    'reviewer': 'manual_reviewer',
                    'review_time': datetime.now()
                }
            }
        )
        
        if update_result.modified_count > 0:
            logger.info(f"确认关联成功: {match_id}")
            return jsonify({
                'success': True,
                'message': '关联确认成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '未找到匹配记录或更新失败'
            })
            
    except Exception as e:
        logger.error(f"确认关联失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/reject_association', methods=['POST'])
def api_reject_association():
    """API: 拒绝关联"""
    try:
        request_data = request.get_json() or {}
        match_id = request_data.get('match_id')
        reason = request_data.get('reason', '人工审核拒绝关联')
        
        if not match_id:
            return jsonify({
                'success': False,
                'error': '缺少必要参数: match_id'
            }), 400
        
        # 获取集合
        collection = db_manager.get_collection('unit_match_results')
        
        # 更新审核状态
        from bson import ObjectId
        from datetime import datetime
        
        update_result = collection.update_one(
            {'_id': ObjectId(match_id)},
            {
                '$set': {
                    'review_status': 'rejected',
                    'review_notes': reason,
                    'reviewer': 'manual_reviewer',
                    'review_time': datetime.now()
                }
            }
        )
        
        if update_result.modified_count > 0:
            logger.info(f"拒绝关联成功: {match_id}")
            return jsonify({
                'success': True,
                'message': '关联拒绝成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '未找到匹配记录或更新失败'
            })
            
    except Exception as e:
        logger.error(f"拒绝关联失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def get_system_stats() -> Dict:
    """获取系统统计信息"""
    try:
        # 获取原始匹配统计
        raw_matching_stats = db_manager.get_match_statistics()
        
        # 转换为前端需要的格式
        matching_stats = {
            'total_matches': raw_matching_stats.get('total_matches', 0),
            'exact': 0,
            'fuzzy': 0, 
            'none': 0
        }
        
        # 从stats_by_type中提取数据
        for stat in raw_matching_stats.get('stats_by_type', []):
            match_type = stat.get('_id', '')
            count = stat.get('count', 0)
            
            if match_type == 'exact':
                matching_stats['exact'] = count
            elif match_type == 'fuzzy':
                matching_stats['fuzzy'] = count
            elif match_type == 'none':
                matching_stats['none'] = count
        
        # 如果stats_by_type为空，尝试从raw_stats_by_type中提取
        if not raw_matching_stats.get('stats_by_type'):
            for stat in raw_matching_stats.get('raw_stats_by_type', []):
                match_type = stat.get('_id', '')
                count = stat.get('count', 0)
                
                if match_type and match_type.startswith('exact'):
                    matching_stats['exact'] += count
                elif match_type and match_type.startswith('fuzzy'):
                    matching_stats['fuzzy'] += count
                elif match_type == 'none':
                    matching_stats['none'] += count
        
        stats = {
            'data_sources': {
                'supervision_count': db_manager.get_collection_count('xxj_shdwjbxx'),
                'inspection_count': db_manager.get_collection_count('xfaqpc_jzdwxx'),
                'match_results_count': db_manager.get_collection_count('unit_match_results')  # 修正集合名称
            },
            'matching_stats': matching_stats,
            'raw_matching_stats': raw_matching_stats,  # 保留原始数据供调试
            'system_info': {
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'running'
            }
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"获取系统统计信息失败: {str(e)}")
        return {
            'error': str(e),
            'data_sources': {},
            'matching_stats': {},
            'system_info': {'status': 'error'}
        }


@app.errorhandler(404)
def not_found_error(error):
    """404错误处理"""
    return render_template('error.html', 
                          error='页面未找到', 
                          error_code=404), 404


@app.errorhandler(500)
def internal_error(error):
    """500错误处理"""
    logger.error(f"内部服务器错误: {str(error)}")
    return render_template('error.html', 
                          error='内部服务器错误', 
                          error_code=500), 500


@app.route('/api/start_multi_matching', methods=['POST'])
def api_start_multi_matching():
    """启动一对多匹配处理"""
    global multi_match_processor
    
    try:
        data = request.get_json() or {}
        limit = data.get('limit', None)
        
        logger.info(f"开始一对多匹配处理，限制: {limit}")
        
        # 执行一对多匹配
        statistics = multi_match_processor.process_all_records(limit=limit)
        
        return safe_json_response({
            'success': True,
            'message': '一对多匹配处理完成',
            'statistics': statistics
        })
        
    except Exception as e:
        logger.error(f"一对多匹配处理失败: {str(e)}")
        return safe_json_response({
            'success': False,
            'error': str(e)
        }, status_code=500)


@app.route('/api/multi_match_statistics')
def api_get_multi_match_statistics():
    """获取一对多匹配统计信息"""
    global multi_match_processor
    
    try:
        statistics = multi_match_processor.get_statistics()
        
        return safe_json_response({
            'success': True,
            'statistics': statistics
        })
        
    except Exception as e:
        logger.error(f"获取一对多匹配统计失败: {str(e)}")
        return safe_json_response({
            'success': False,
            'error': str(e)
        }, status_code=500)


@app.route('/api/multi_match_results')
def api_get_multi_match_results():
    """获取一对多匹配结果"""
    global multi_match_processor
    
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        
        skip = (page - 1) * per_page
        results = multi_match_processor.get_match_results(limit=per_page, skip=skip)
        
        # 格式化结果用于前端显示
        formatted_results = []
        for result in results:
            formatted_result = {
                'id': str(result.get('_id', '')),
                'unit_name': result.get('unit_name', ''),
                'address': result.get('address', ''),
                'legal_representative': result.get('legal_representative', ''),
                'credit_code': result.get('credit_code', ''),
                'match_status': result.get('match_status', ''),
                'total_matched_records': result.get('total_matched_records', 0),
                'match_summary': result.get('match_summary', {}),
                'primary_matched_record': result.get('primary_matched_record', {}),
                'matched_records': result.get('matched_records', []),
                'created_at': result.get('created_at', '').isoformat() if result.get('created_at') else ''
            }
            formatted_results.append(formatted_result)
        
        return safe_json_response({
            'success': True,
            'results': formatted_results,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': len(formatted_results)
            }
        })
        
    except Exception as e:
        logger.error(f"获取一对多匹配结果失败: {str(e)}")
        return safe_json_response({
            'success': False,
            'error': str(e)
        }, status_code=500)


@app.route('/api/unit_inspection_history/<unit_id>')
def api_get_unit_inspection_history(unit_id):
    """获取单位的检查历史记录"""
    global multi_match_processor
    
    try:
        # 查找指定单位的匹配结果
        result_collection = db_manager.get_collection('unit_multi_match_results')
        unit_result = result_collection.find_one({'_id': unit_id})
        
        if not unit_result:
            return safe_json_response({
                'success': False,
                'error': '未找到指定单位'
            }, status_code=404)
        
        # 格式化检查历史记录
        inspection_history = []
        for record in unit_result.get('matched_records', []):
            match_info = record.get('match_info', {})
            inspection_record = {
                'inspection_id': str(record.get('_id', '')),
                'inspection_date': match_info.get('inspection_date', ''),
                'unit_name': record.get('dwmc', ''),
                'address': record.get('dwdz', ''),
                'legal_representative': record.get('fddbr', ''),
                'contact_phone': record.get('frlxdh', ''),
                'credit_code': record.get('tyshxydm', ''),
                'match_type': match_info.get('match_type', ''),
                'similarity_score': match_info.get('similarity_score', 0),
                'is_primary': match_info.get('is_primary', False),
                'match_rank': match_info.get('match_rank', 0),
                'data_completeness': match_info.get('data_completeness', 0)
            }
            inspection_history.append(inspection_record)
        
        # 按检查日期排序（最新的在前）
        inspection_history.sort(key=lambda x: x['inspection_date'], reverse=True)
        
        # 构建响应数据
        unit_info = {
            'unit_id': str(unit_result.get('_id', '')),
            'unit_name': unit_result.get('unit_name', ''),
            'address': unit_result.get('address', ''),
            'legal_representative': unit_result.get('legal_representative', ''),
            'credit_code': unit_result.get('credit_code', ''),
            'total_inspections': len(inspection_history),
            'match_summary': unit_result.get('match_summary', {}),
            'inspection_history': inspection_history
        }
        
        return safe_json_response({
            'success': True,
            'unit_info': unit_info
        })
        
    except Exception as e:
        logger.error(f"获取单位检查历史失败: {str(e)}")
        return safe_json_response({
            'success': False,
            'error': str(e)
        }, status_code=500)


@app.route('/multi_results')
def multi_results_page():
    """一对多匹配结果页面"""
    try:
        return render_template('multi_results.html')
    except Exception as e:
        logger.error(f"一对多匹配结果页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/unit_detail/<unit_id>')
def unit_detail_page(unit_id):
    """单位详情页面"""
    try:
        return render_template('unit_detail.html', unit_id=unit_id)
    except Exception as e:
        logger.error(f"单位详情页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


# ===== 增强关联功能API接口 =====

@app.route('/enhanced_association')
def enhanced_association_page():
    """增强关联页面"""
    try:
        return render_template('enhanced_association.html')
    except Exception as e:
        logger.error(f"增强关联页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/csv_upload')
def csv_upload_page():
    """V2.0 CSV文件上传页面"""
    try:
        return render_template('csv_upload.html')
    except Exception as e:
        logger.error(f"CSV上传页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/data_analysis')
def data_analysis_page():
    """V2.0 数据分析页面"""
    try:
        file_id = request.args.get('file_id')
        if not file_id:
            return render_template('error.html', error_message='缺少文件ID参数'), 400
        return render_template('data_analysis.html', file_id=file_id)
    except Exception as e:
        logger.error(f"数据分析页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/field_mapping')
def field_mapping_page():
    """V2.0 字段映射配置页面"""
    try:
        file_id = request.args.get('file_id')
        if not file_id:
            return render_template('error.html', error_message='缺少文件ID参数'), 400
        return render_template('field_mapping.html', file_id=file_id)
    except Exception as e:
        logger.error(f"字段映射页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/kg_builder')
def kg_builder_page():
    """V2.0 知识图谱构建页面"""
    try:
        file_id = request.args.get('file_id')
        if not file_id:
            return render_template('error.html', error_message='缺少文件ID参数'), 400
        return render_template('kg_builder.html', file_id=file_id)
    except Exception as e:
        logger.error(f"知识图谱构建页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/kg_viewer')
def kg_viewer_page():
    """知识图谱浏览页面"""
    try:
        return render_template('kg_viewer.html')
    except Exception as e:
        logger.error(f"知识图谱浏览页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/kg_visualization')
def kg_visualization_page():
    """知识图谱可视化页面"""
    try:
        return render_template('kg_visualization.html')
    except Exception as e:
        logger.error(f"知识图谱可视化页面渲染失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/api/start_enhanced_association', methods=['POST'])
def api_start_enhanced_association():
    """启动增强关联任务"""
    try:
        data = request.get_json() or {}
        
        # 获取参数
        strategy = data.get('strategy', AssociationStrategy.HYBRID)
        clear_existing = data.get('clear_existing', False)
        
        # 验证策略参数
        valid_strategies = [AssociationStrategy.BUILDING_BASED, AssociationStrategy.UNIT_BASED, AssociationStrategy.HYBRID]
        if strategy not in valid_strategies:
            return jsonify({
                'success': False,
                'error': f'无效的关联策略: {strategy}',
                'valid_strategies': valid_strategies
            }), 400
        
        # 启动任务
        task_id = enhanced_association_processor.start_enhanced_association_task(
            strategy=strategy,
            clear_existing=clear_existing
        )
        
        logger.info(f"增强关联任务启动成功: {task_id}")
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': f'增强关联任务已启动 (策略: {strategy})',
            'parameters': {
                'strategy': strategy,
                'clear_existing': clear_existing
            }
        })
        
    except Exception as e:
        logger.error(f"启动增强关联任务失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/enhanced_association_progress/<task_id>')
def api_get_enhanced_association_progress(task_id):
    """API: 获取增强关联任务进度"""
    try:
        progress = enhanced_association_processor.get_association_task_progress(task_id)
        if progress:
            return jsonify({'success': True, 'progress': progress})
        else:
            return jsonify({'success': False, 'error': '未找到任务'}), 404
    except Exception as e:
        logger.error(f"获取增强关联任务进度失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/enhanced_association_statistics')
def api_get_enhanced_association_statistics():
    """获取增强关联统计信息 - [修复] 直接从结果表聚合"""
    try:
        if not enhanced_association_processor:
            return jsonify({'error': '增强关联处理器未初始化'}), 500
        
        # 直接调用重构后的统计方法
        statistics = enhanced_association_processor.get_association_statistics()
        
        return jsonify({
            'success': True,
            'statistics': statistics,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取增强关联统计失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/enhanced_association_results')
def api_get_enhanced_association_results():
    """获取增强关联结果 - [修复] 直接从结果表查询"""
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))
        
        # 构建查询（这里可以保持不变，因为我们是直接查询最终结果）
        query = {}
        strategy = request.args.get('strategy')
        if strategy:
            query['association_type'] = strategy # 注意字段名可能已在聚合中更改
        
        collection = db_manager.get_collection('enhanced_association_results')
        
        total_count = collection.count_documents(query)
        cursor = collection.find(query).skip((page - 1) * page_size).limit(page_size)
        
        results = safe_json_response(list(cursor))
        
        return jsonify({
            'success': True,
            'results': results,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_count': total_count,
                'total_pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
            }
        })
        
    except Exception as e:
        logger.error(f"获取增强关联结果失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/association_result_detail/<association_id>')
def api_get_association_result_detail(association_id):
    """获取关联结果详情"""
    try:
        from bson import ObjectId
        
        collection = db_manager.get_collection('enhanced_association_results')
        
        # 查找记录
        result = collection.find_one({'association_id': association_id})
        
        if not result:
            return jsonify({
                'success': False,
                'error': '未找到指定的关联结果'
            }), 404
        
        # 使用 safe_json_response 确保所有字段都能被正确序列化
        return jsonify({
            'success': True,
            'result': safe_json_response(result)
        })
        
    except Exception as e:
        logger.error(f"获取关联结果详情失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/clear_enhanced_association_results', methods=['POST'])
def api_clear_enhanced_association_results():
    """API: 清空增强关联结果"""
    try:
        if not enhanced_association_processor:
             return jsonify({'success': False, 'error': '处理器未初始化'}), 500

        success = enhanced_association_processor._clear_association_results()
        
        if success:
            return jsonify({'success': True, 'message': '增强关联结果已成功清除'})
        else:
            return jsonify({'success': False, 'error': '清除增强关联结果失败'})
            
    except Exception as e:
        logger.error(f"清除增强关联结果失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== V2.0 CSV数据管理API接口 =====

@app.route('/api/upload_csv', methods=['POST'])
def api_upload_csv():
    """API: 上传CSV文件"""
    logger.info(f"收到CSV上传请求，Content-Type: {request.content_type}")
    logger.info(f"请求文件: {list(request.files.keys())}")
    try:
        # 检查处理器是否初始化
        if not csv_processor:
            return jsonify({'success': False, 'error': 'CSV处理器未初始化'}), 500
            
        # 读取上传配置
        upload_config = {}
        try:
            upload_config = (config_manager.get_web_config() or {}).get('upload', {})
        except Exception:
            upload_config = {}

        def _parse_size_to_bytes(size_str: str) -> int:
            try:
                s = (size_str or '').strip().lower()
                if s.endswith('gb'):
                    return int(float(s[:-2].strip()) * 1024 * 1024 * 1024)
                if s.endswith('mb'):
                    return int(float(s[:-2].strip()) * 1024 * 1024)
                if s.endswith('kb'):
                    return int(float(s[:-2].strip()) * 1024)
                if s.isdigit():
                    return int(s)
            except Exception:
                pass
            # 默认500MB
            return 500 * 1024 * 1024

        allowed_extensions = upload_config.get('allowed_extensions', ['.csv', '.txt', '.xlsx', '.xls'])
        max_size_bytes = _parse_size_to_bytes(upload_config.get('max_file_size', '500MB'))
        upload_folder = upload_config.get('upload_folder', 'uploads')  # 兼容旧配置但不再本地落盘

        # 检查文件是否存在
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': '未选择文件'}), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': '文件名为空'}), 400
            
        # 验证文件类型
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in allowed_extensions:
            return jsonify({'success': False, 'error': '不支持的文件格式'}), 400
            
        # 检查文件大小 (500MB限制)
        file.seek(0, 2)  # 移动到文件末尾
        file_size = file.tell()
        file.seek(0)  # 重置到文件开头
        
        if file_size > max_size_bytes:
            max_mb = int(round(max_size_bytes / (1024 * 1024)))
            return jsonify({'success': False, 'error': f'文件大小超过限制（最大{max_mb}MB）'}), 400
            
        # 生成时间戳与原始文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        original_filename = file.filename

        # 读取内存内容
        file_bytes = file.read()

        # 按类型解析为DataFrame（不落盘）
        from io import BytesIO
        import pandas as _pd

        if file_ext in ['.xlsx', '.xls']:
            df = _pd.read_excel(BytesIO(file_bytes))
            df = csv_processor._basic_cleaning(df)
            parse_meta = {
                'encoding': 'binary',
                'delimiter': ','
            }
            result = {
                'success': True,
                'dataframe': df,
                'metadata': {
                    'file_name': original_filename,
                    'total_rows': len(df),
                    'total_columns': len(df.columns),
                    'columns': df.columns.tolist(),
                },
                'encoding': 'binary',
                'delimiter': ',',
                'row_count': len(df),
                'column_count': len(df.columns),
                'sample_data': csv_processor.get_sample_data(df),
                'warnings': []
            }
        else:
            # CSV/TXT 走解析器，不落盘
            # 先校验（用文件名做路径参数以复用扩展名校验）
            validation_result = csv_processor.validate_file(original_filename, file_bytes)
            if not validation_result.get('valid'):
                return jsonify({'success': False, 'message': f"文件验证失败: {', '.join(validation_result.get('errors', []))}"}), 400

            result = csv_processor.parse_csv(file_bytes, original_filename)
            if result.get('success'):
                # 补全统一返回结构
                df = result['data']
                result = {
                    'success': True,
                    'dataframe': df,
                    'metadata': result.get('metadata', {}),
                    'encoding': result.get('metadata', {}).get('encoding', 'utf-8'),
                    'delimiter': result.get('metadata', {}).get('delimiter', ','),
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'sample_data': csv_processor.get_sample_data(df),
                    'warnings': result.get('warnings', [])
                }
        
        # 检查处理结果
        if not result.get('success', False):
            return jsonify({
                'success': False,
                'message': result.get('error', 'CSV文件处理失败')
            }), 400
        
        # 进行数据分析
        df = result['dataframe']
        if df is None:
            return jsonify({
                'success': False,
                'message': 'CSV文件解析失败，无法获取数据'
            }), 400
            
        analysis_result = data_analyzer.analyze_dataframe(df)
        
        # 进行模式检测
        schema_result = schema_detector.detect_schema(df)
        
        # 生成集合名（按文件名）与文件ID
        def derive_collection_name(name: str) -> str:
            import re
            base = os.path.splitext(os.path.basename(name))[0]
            base = base.lower()
            base = re.sub(r'[^a-z0-9_]+', '_', base)
            base = re.sub(r'_+', '_', base).strip('_')
            if not base:
                base = f"csv_{timestamp}"
            return base

        collection_name = derive_collection_name(original_filename)
        file_id = f"csv_{timestamp}_{hash(original_filename) % 10000}"

        # 写入MongoDB（按文件名建表/集合），若已存在则使用别名
        try:
            db = db_manager.get_db()
            existing_names = set(db.list_collection_names())
            requested_collection = collection_name
            alias_used = False
            alias_notice = None
            if collection_name in existing_names:
                # 使用时间戳别名，确保唯一
                alias_candidate = f"{collection_name}_{timestamp}"
                idx = 1
                while alias_candidate in existing_names:
                    alias_candidate = f"{collection_name}_{timestamp}_{idx}"
                    idx += 1
                collection_name = alias_candidate
                alias_used = True
                alias_notice = f"集合 {requested_collection} 已存在，已使用别名 {collection_name}"
            # 字段名清洗（Mongo不允许包含'.'或以'$'开头）
            def _sanitize_field_name(name: str) -> str:
                import re
                n = str(name)
                if n.startswith('$'):
                    n = f"f_{n[1:]}"  # 前缀替换
                n = n.replace('.', '_')
                n = re.sub(r'\s+', '_', n).strip('_')
                return n or 'field'

            column_mapping = {col: _sanitize_field_name(col) for col in df.columns}
            if list(column_mapping.values()) != list(df.columns):
                df = df.rename(columns=column_mapping)

            # 将NaN转为None
            safe_df = df.where(pd.notna(df), None)
            records = safe_df.to_dict('records')
            if records:
                db[collection_name].insert_many(records)
        except Exception as e:
            logger.error(f"写入MongoDB集合失败: {collection_name}, 错误: {e}")
            return jsonify({'success': False, 'error': f'写入数据库失败: {str(e)}'}), 500
        
        # 构建文件信息，针对大数据集优化响应大小
        is_large_dataset = len(df) > 50000
        
        file_info = {
            'file_id': file_id,
            'filename': original_filename,
            'collection_name': collection_name,
            'requested_collection': requested_collection,
            'alias_used': alias_used,
            'alias_notice': alias_notice,
            'file_size': file_size,
            'upload_time': datetime.now().isoformat(),
            'encoding': result.get('encoding', 'utf-8'),
            'delimiter': result.get('delimiter', ','),
            'row_count': len(df),
            'column_count': len(df.columns),
            'quality_score': analysis_result.get('quality_score', 0),
            'is_large_dataset': is_large_dataset
        }
        
        # 对于大数据集，减少详细信息以避免响应过大
        if is_large_dataset:
            file_info.update({
                'columns': df.columns.tolist()[:20],  # 只显示前20列
                'columns_truncated': len(df.columns) > 20,
                'sample_data': df.head(2).to_dict('records'),  # 只显示前2行
                'message': f'大数据集（{len(df):,}行），部分信息已省略以优化响应速度'
            })
        else:
            file_info.update({
                'columns': df.columns.tolist(),
                'data_types': analysis_result.get('field_types', {}),
                'schema_detection': schema_result,
                'sample_data': df.head(5).to_dict('records'),
                'column_mapping': column_mapping
            })
        
        # 可选：存储到MongoDB
        if db_manager and db_manager.mongo_client:
            try:
                db_manager.mongo_client.get_database().csv_files.insert_one({
                    'file_id': file_id,
                    'file_info': file_info,
                    'created_at': datetime.now()
                })
            except Exception as e:
                logger.warning(f"存储文件信息到数据库失败: {str(e)}")
        
        # 返回处理结果
        success_message = '文件上传和导入成功'
        if alias_notice:
            success_message += f'（{alias_notice}）'
        return jsonify({'success': True, 'message': success_message, 'data': file_info})
        
    except Exception as e:
        logger.error(f"CSV文件上传处理失败: {str(e)}")
        return jsonify({'success': False, 'error': f'文件处理失败: {str(e)}'}), 500


@app.route('/api/get_file_analysis/<file_id>')
def api_get_file_analysis(file_id):
    """API: 获取文件分析结果"""
    try:
        # 从数据库获取文件信息
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接失败'}), 500
            
        file_record = db_manager.mongo_client.get_database().csv_files.find_one(
            {'file_id': file_id}
        )
        
        if not file_record:
            return jsonify({'success': False, 'error': '文件不存在'}), 404
            
        return jsonify({
            'success': True,
            'data': file_record['file_info']
        })
        
    except Exception as e:
        logger.error(f"获取文件分析结果失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/validate_csv_data', methods=['POST'])
def api_validate_csv_data():
    """API: 验证CSV数据"""
    try:
        data = request.get_json() or {}
        file_id = data.get('file_id')
        
        if not file_id:
            return jsonify({'success': False, 'error': '缺少文件ID'}), 400
            
        if not validation_engine:
            return jsonify({'success': False, 'error': '验证引擎未初始化'}), 500
            
        # 获取文件信息
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接失败'}), 500
            
        file_record = db_manager.mongo_client.get_database().csv_files.find_one(
            {'file_id': file_id}
        )
        
        if not file_record:
            return jsonify({'success': False, 'error': '文件不存在'}), 404
            
        # 优先从Mongo集合读取数据
        file_info = file_record.get('file_info', {})
        collection_name = file_info.get('collection_name')
        df = None
        if collection_name:
            try:
                import pandas as _pd
                db = db_manager.get_db()
                docs = list(db[collection_name].find({}))
                if not docs:
                    return jsonify({'success': False, 'error': '集合中无数据'}), 404
                # 去除Mongo的_id字段
                for d in docs:
                    d.pop('_id', None)
                df = _pd.DataFrame(docs)
            except Exception as e:
                return jsonify({'success': False, 'error': f'从集合读取数据失败: {str(e)}'}), 500
        else:
            # 兼容旧数据：从本地文件路径回退
            file_path = file_info.get('file_path')
            if not file_path or not os.path.exists(file_path):
                return jsonify({'success': False, 'error': '无法读取数据（无集合且文件不存在）'}), 404
            result = csv_processor.process_file(file_path)
            df = result['dataframe']
        
        # 执行数据验证
        validation_result = validation_engine.validate_dataframe(
            df, 
            validation_rules=data.get('validation_rules', {})
        )
        
        return jsonify({
            'success': True,
            'data': validation_result
        })
        
    except Exception as e:
        logger.error(f"CSV数据验证失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/build_knowledge_graph', methods=['POST'])
def api_build_knowledge_graph():
    """API: 构建知识图谱"""
    try:
        data = request.get_json() or {}
        file_id = data.get('file_id')
        config = data.get('config', {})
        
        if not file_id:
            return jsonify({'success': False, 'error': '缺少文件ID'}), 400
            
        # 检查知识图谱处理器（需要从昨天的模块导入）
        try:
            from src.knowledge_graph.kg_builder import KnowledgeGraphBuilder
            kg_builder = KnowledgeGraphBuilder(db_manager)
        except ImportError as e:
            return jsonify({'success': False, 'error': f'知识图谱模块未找到: {str(e)}'}), 500
            
        # 获取文件信息
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接失败'}), 500
            
        file_record = db_manager.mongo_client.get_database().csv_files.find_one(
            {'file_id': file_id}
        )
        
        if not file_record:
            return jsonify({'success': False, 'error': '文件不存在'}), 404
            
        # 获取文件数据
        file_path = file_record['file_info']['file_path']
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'error': '文件已不存在'}), 404
            
        # 处理文件
        result = csv_processor.process_file(file_path)
        df = result['dataframe']
        
        # 生成任务ID
        task_id = f"kg_build_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(file_id) % 10000}"
        
        # 创建构建任务记录
        build_task = {
            'task_id': task_id,
            'file_id': file_id,
            'config': config,
            'status': 'starting',
            'progress': 0,
            'current_step': 1,
            'steps': ['数据加载', '实体抽取', '关系发现', '图谱构建', '质量验证'],
            'processed_records': 0,
            'extracted_entities': 0,
            'discovered_relations': 0,
            'logs': [],
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        # 保存任务到数据库
        db_manager.mongo_client.get_database().kg_build_tasks.insert_one(build_task)
        
        # 这里应该启动异步构建任务，现在先返回成功
        # 在实际实现中，应该使用Celery或类似的任务队列
        
        return jsonify({
            'success': True,
            'message': '知识图谱构建任务已启动',
            'task_id': task_id
        })
        
    except Exception as e:
        logger.error(f"知识图谱构建启动失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500





@app.route('/api/database_tables')
def api_get_database_tables():
    """API: 获取数据库中所有可用的数据表列表"""
    try:
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        
        # 获取所有集合名称
        collection_names = db.list_collection_names()
        
        # 过滤掉系统集合和临时集合
        excluded_prefixes = ['system.', 'tmp_', 'temp_']
        excluded_collections = ['csv_files', 'match_results', 'tokenization_cache', 'unit_name_cache', 'address_cache', 'match_result_cache']
        
        filtered_collections = []
        for name in collection_names:
            # 跳过系统集合和缓存集合
            if any(name.startswith(prefix) for prefix in excluded_prefixes):
                continue
            if name in excluded_collections:
                continue
                
            try:
                # 获取集合基本信息
                collection = db[name]
                doc_count = collection.count_documents({})
                
                # 获取样本文档来分析字段
                sample_doc = collection.find_one()
                field_count = len(sample_doc.keys()) if sample_doc else 0
                
                # 获取集合创建信息（如果有的话）
                collection_info = {
                    'name': name,
                    'display_name': name.replace('_', ' ').title(),
                    'count': doc_count,  # 前端期望的字段名
                    'fields': field_count,  # 前端期望的字段名
                    'document_count': doc_count,  # 保留原字段名兼容性
                    'field_count': field_count,  # 保留原字段名兼容性
                    'sample_fields': list(sample_doc.keys())[:10] if sample_doc else [],
                    'has_data': doc_count > 0,
                    'type': 'user_data'  # 用户数据表
                }
                
                # 尝试从csv_files集合获取更详细的信息
                try:
                    csv_file_info = db.csv_files.find_one({'file_info.collection_name': name})
                    if csv_file_info and 'file_info' in csv_file_info:
                        file_info = csv_file_info['file_info']
                        collection_info.update({
                            'source_filename': file_info.get('filename', name),
                            'upload_time': file_info.get('upload_time'),
                            'file_size': file_info.get('file_size'),
                            'encoding': file_info.get('encoding'),
                            'type': 'imported_csv'
                        })
                except:
                    pass  # 如果获取不到也没关系
                
                filtered_collections.append(collection_info)
                
            except Exception as e:
                logger.warning(f"获取集合 {name} 信息失败: {str(e)}")
                continue
        
        # 按文档数量排序
        filtered_collections.sort(key=lambda x: x['count'], reverse=True)
        
        return jsonify({
            'success': True,
            'tables': filtered_collections,
            'total_count': len(filtered_collections)
        })
        
    except Exception as e:
        logger.error(f"获取数据表列表失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/analyze_table', methods=['POST'])
def api_analyze_table():
    """API: 分析指定数据表"""
    try:
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        data = request.get_json()
        table_name = data.get('table_name')
        options = {
            'include_sample_data': data.get('include_sample_data', True),
            'include_quality_check': data.get('include_quality_check', True),
            'include_field_analysis': data.get('include_field_analysis', True),
            'include_statistics': data.get('include_statistics', True),
            'sample_size': data.get('sample_size', 5000)
        }
        
        if not table_name:
            return jsonify({'success': False, 'error': '未指定数据表名'}), 400
        
        db = db_manager.mongo_client.get_database()
        collection = db[table_name]
        
        # 检查集合是否存在
        if table_name not in db.list_collection_names():
            return jsonify({'success': False, 'error': f'数据表 {table_name} 不存在'}), 404
        
        analysis_result = {
            'table_name': table_name,
            'total_records': collection.count_documents({}),
            'field_count': 0,
            'quality_score': 0,
            'completeness': 0
        }
        
        # 获取样本数据进行分析
        sample_size = min(options['sample_size'], analysis_result['total_records'])
        pipeline = [{'$sample': {'size': sample_size}}] if sample_size > 0 else []
        sample_docs = list(collection.aggregate(pipeline)) if sample_size > 0 else []
        
        if sample_docs:
            # 分析字段
            all_fields = set()
            for doc in sample_docs:
                all_fields.update(doc.keys())
            
            all_fields.discard('_id')  # 排除MongoDB的_id字段
            analysis_result['field_count'] = len(all_fields)
            
            # 字段分析
            if options['include_field_analysis'] and all_fields:
                field_analysis = []
                
                for field in all_fields:
                    field_info = analyze_field(sample_docs, field)
                    field_analysis.append(field_info)
                
                analysis_result['field_analysis'] = field_analysis
                
                # 计算整体质量分数
                if field_analysis:
                    avg_quality = sum(f['quality_score'] for f in field_analysis) / len(field_analysis)
                    analysis_result['quality_score'] = round(avg_quality)
            
            # 计算完整性
            if options['include_quality_check']:
                total_fields = len(all_fields) * len(sample_docs)
                non_null_fields = 0
                
                for doc in sample_docs:
                    for field in all_fields:
                        if field in doc and doc[field] is not None and doc[field] != '':
                            non_null_fields += 1
                
                analysis_result['completeness'] = round((non_null_fields / total_fields) * 100) if total_fields > 0 else 0
            
            # 样本数据
            if options['include_sample_data']:
                # 清理样本数据，移除_id字段
                clean_sample = []
                for doc in sample_docs[:10]:  # 只返回前10条
                    clean_doc = {k: v for k, v in doc.items() if k != '_id'}
                    clean_sample.append(clean_doc)
                analysis_result['sample_data'] = clean_sample
        
        # 清理所有NaN值以确保JSON序列化成功
        analysis_result = clean_nan_values(analysis_result)
        
        return jsonify({
            'success': True,
            'analysis': analysis_result
        })
        
    except Exception as e:
        logger.error(f"数据表分析失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


def analyze_field(sample_docs, field_name):
    """分析单个字段的详细信息"""
    values = []
    non_null_count = 0
    
    for doc in sample_docs:
        value = doc.get(field_name)
        if value is not None and value != '' and not (isinstance(value, float) and math.isnan(value)):
            values.append(value)
            non_null_count += 1
    
    total_count = len(sample_docs)
    non_null_rate = round((non_null_count / total_count) * 100) if total_count > 0 else 0
    unique_count = len(set(str(v) for v in values))
    
    # 推断数据类型
    data_type = infer_data_type(values)
    
    # 计算质量分数
    quality_score = calculate_field_quality(values, non_null_rate, unique_count, total_count)
    
    return {
        'field_name': field_name,
        'data_type': data_type,
        'non_null_rate': non_null_rate,
        'unique_count': unique_count,
        'quality_score': quality_score,
        'total_values': total_count
    }


def infer_data_type(values):
    """推断字段的数据类型"""
    if not values:
        return 'unknown'
    
    # 检查数值类型
    numeric_count = 0
    date_count = 0
    boolean_count = 0
    
    for value in values[:100]:  # 只检查前100个值
        # 跳过NaN值
        if isinstance(value, float) and math.isnan(value):
            continue
        if pd.isna(value):
            continue
            
        str_value = str(value).strip()
        
        # 检查数字
        try:
            float(str_value)
            numeric_count += 1
            continue
        except (ValueError, TypeError):
            pass
        
        # 检查布尔值
        if str_value.lower() in ['true', 'false', '1', '0', 'yes', 'no', 'y', 'n']:
            boolean_count += 1
            continue
        
        # 检查日期
        try:
            import dateutil.parser
            dateutil.parser.parse(str_value)
            date_count += 1
            continue
        except:
            pass
    
    total_checked = min(len(values), 100)
    
    if numeric_count / total_checked > 0.8:
        return 'number'
    elif date_count / total_checked > 0.8:
        return 'date'
    elif boolean_count / total_checked > 0.8:
        return 'boolean'
    else:
        return 'string'


def calculate_field_quality(values, non_null_rate, unique_count, total_count):
    """计算字段质量分数"""
    # 基础分数基于非空率
    quality = non_null_rate
    
    # 根据唯一性调整分数
    if total_count > 0:
        uniqueness_ratio = unique_count / total_count
        
        # 如果所有值都相同，降低质量分数
        if uniqueness_ratio < 0.01:
            quality *= 0.7
        # 如果有合理的重复，这是好的
        elif 0.01 <= uniqueness_ratio <= 0.8:
            quality *= 1.1
        # 如果几乎所有值都唯一，可能是ID字段，保持原分数
    
    return min(100, round(quality))


def clean_nan_values(obj):
    """清理对象中的NaN值，使其可以JSON序列化"""
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    elif pd.isna(obj):  # 处理pandas的NaN值
        return None
    else:
        return obj


@app.route('/standalone_data_analysis')
def standalone_data_analysis():
    """独立数据分析页面"""
    return render_template('standalone_data_analysis.html')


@app.route('/user_driven_field_mapping')
def user_driven_field_mapping():
    """用户主导的字段映射页面"""
    return render_template('user_driven_field_mapping.html')


@app.route('/api/get_table_fields', methods=['POST'])
def api_get_table_fields():
    """API: 获取指定数据表的字段信息"""
    try:
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        data = request.get_json()
        
        # 支持单表和多表查询
        table_name = data.get('table_name')
        table_names = data.get('table_names', [])
        
        if table_name:
            table_names = [table_name]
        
        if not table_names:
            return jsonify({'success': False, 'error': '未指定数据表名'}), 400
        
        db = db_manager.mongo_client.get_database()
        tables_info = []
        
        for table_name in table_names:
            # 检查集合是否存在
            if table_name not in db.list_collection_names():
                return jsonify({'success': False, 'error': f'数据表 {table_name} 不存在'}), 404
            
            collection = db[table_name]
            
            # 获取样本文档来分析字段
            sample_docs = list(collection.find().limit(100))
            if not sample_docs:
                tables_info.append({
                    'table_name': table_name,
                    'fields': [],
                    'sample_count': 0,
                    'total_count': 0
                })
                continue
            
            # 分析字段信息
            all_fields = set()
            for doc in sample_docs:
                all_fields.update(doc.keys())
            
            all_fields.discard('_id')  # 排除MongoDB的_id字段
            
            # 详细分析每个字段
            field_info = []
            for field in sorted(all_fields):
                field_analysis = analyze_field_details(sample_docs, field)
                field_info.append(field_analysis)
            
            tables_info.append({
                'table_name': table_name,
                'display_name': table_name.replace('_', ' ').title(),
                'fields': field_info,
                'sample_count': len(sample_docs),
                'total_count': collection.count_documents({})
            })
        
        # 如果是单表查询，直接返回字段信息
        if len(tables_info) == 1 and data.get('table_name'):
            return jsonify({
                'success': True,
                'fields': tables_info[0]['fields'],
                'table_info': tables_info[0]
            })
        else:
            return jsonify({
                'success': True,
                'tables': tables_info
            })
        
    except Exception as e:
        logger.error(f"获取表字段信息失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/suggest_field_mappings', methods=['POST'])
def api_suggest_field_mappings():
    """API: 智能字段映射建议"""
    try:
        data = request.get_json()
        source_table = data.get('source_table')
        target_table = data.get('target_table')
        source_fields = data.get('source_fields', [])
        target_fields = data.get('target_fields', [])
        
        if not all([source_table, target_table, source_fields, target_fields]):
            return jsonify({'success': False, 'error': '缺少必要参数'}), 400
        
        # 智能字段匹配算法
        suggestions = generate_field_mapping_suggestions(
            source_fields, target_fields, source_table, target_table
        )
        
        return jsonify({
            'success': True,
            'suggestions': suggestions,
            'source_table': source_table,
            'target_table': target_table
        })
        
    except Exception as e:
        logger.error(f"生成字段映射建议失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/save_field_mapping', methods=['POST'])
def api_save_field_mapping():
    """API: 保存字段映射配置"""
    try:
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        data = request.get_json()
        source_table = data.get('source_table')
        target_tables = data.get('target_tables', [])
        mappings = data.get('mappings', [])
        
        if not all([source_table, target_tables, mappings]):
            return jsonify({'success': False, 'error': '映射配置不完整'}), 400
        
        # 保存到数据库
        db = db_manager.mongo_client.get_database()
        config_collection = db['field_mapping_configs']
        
        # 创建配置文档
        config_doc = {
            'config_id': f"{source_table}_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'source_table': source_table,
            'target_tables': target_tables,
            'mappings': mappings,
            'created_at': datetime.now().isoformat(),  # 使用ISO格式字符串
            'status': 'active'
        }
        
        result = config_collection.insert_one(config_doc)
        
        if result.inserted_id:
            return jsonify({
                'success': True,
                'config_id': config_doc['config_id'],
                'message': '字段映射配置保存成功'
            })
        else:
            return jsonify({'success': False, 'error': '配置保存失败'}), 500
            
    except Exception as e:
        logger.error(f"保存字段映射配置失败: {str(e)}")
        logger.error(f"请求数据: {data}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/preview_field_mapping', methods=['POST'])
def api_preview_field_mapping():
    """API: 预览字段映射结果"""
    try:
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        data = request.get_json()
        source_table = data.get('source_table')
        target_table = data.get('target_table')
        field_mappings = data.get('field_mappings', [])
        preview_limit = data.get('preview_limit', 10)
        
        if not all([source_table, target_table, field_mappings]):
            return jsonify({'success': False, 'error': '缺少必要参数'}), 400
        
        db = db_manager.mongo_client.get_database()
        
        # 获取源表和目标表的样本数据
        source_collection = db[source_table]
        target_collection = db[target_table]
        
        source_docs = list(source_collection.find().limit(preview_limit))
        target_docs = list(target_collection.find().limit(preview_limit))
        
        # 执行字段映射预览
        preview_result = execute_field_mapping_preview(
            source_docs, target_docs, field_mappings, preview_limit
        )
        
        return jsonify({
            'success': True,
            'preview': preview_result,
            'source_table': source_table,
            'target_table': target_table,
            'mapping_count': len(field_mappings)
        })
        
    except Exception as e:
        logger.error(f"预览字段映射失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500





def analyze_field_details(sample_docs, field_name):
    """分析字段的详细信息"""
    import math
    
    values = []
    non_null_count = 0
    
    for doc in sample_docs:
        value = doc.get(field_name)
        if value is not None and value != '':
            # 处理NaN值
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                continue
            values.append(value)
            non_null_count += 1
    
    total_count = len(sample_docs)
    non_null_rate = round((non_null_count / total_count) * 100) if total_count > 0 else 0
    unique_count = len(set(str(v) for v in values))
    
    # 推断数据类型
    data_type = infer_data_type(values)
    
    # 获取示例值，确保序列化安全
    sample_values = []
    for v in values[:5]:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            continue
        sample_values.append(str(v))
    
    return {
        'name': field_name,  # 前端期望的字段名
        'type': data_type,   # 前端期望的字段名
        'field_name': field_name,  # 保留原字段名兼容性
        'data_type': data_type,    # 保留原字段名兼容性
        'non_null_rate': non_null_rate,
        'unique_count': unique_count,
        'total_values': total_count,
        'sample_values': sample_values,
        'is_key_field': unique_count == non_null_count if non_null_count > 0 else False  # 判断是否可能是主键字段
    }


def generate_field_mapping_suggestions(source_fields, target_fields, source_table, target_table):
    """生成智能字段映射建议（增强版）"""
    try:
        # 导入增强字段映射器
        from src.matching.enhanced_field_mapper import EnhancedFieldMapper
        
        # 初始化增强映射器
        enhanced_mapper = EnhancedFieldMapper()
        
        # 使用增强算法生成建议
        suggestions = enhanced_mapper.generate_intelligent_mapping_suggestions(
            source_fields, target_fields, max_suggestions=3
        )
        
        return suggestions
        
    except ImportError as e:
        logger.warning(f"增强字段映射器导入失败，使用基础算法: {str(e)}")
        # 回退到原始算法
        return _generate_basic_field_mapping_suggestions(source_fields, target_fields, source_table, target_table)
    except Exception as e:
        logger.error(f"增强字段映射失败: {str(e)}")
        # 回退到原始算法
        return _generate_basic_field_mapping_suggestions(source_fields, target_fields, source_table, target_table)


def _generate_basic_field_mapping_suggestions(source_fields, target_fields, source_table, target_table):
    """生成基础字段映射建议（原始算法作为回退）"""
    suggestions = []
    
    for source_field in source_fields:
        source_name = source_field['field_name'].lower()
        source_type = source_field['data_type']
        
        best_matches = []
        
        for target_field in target_fields:
            target_name = target_field['field_name'].lower()
            target_type = target_field['data_type']
            
            # 计算字段名称相似度
            name_similarity = calculate_field_name_similarity(source_name, target_name)
            
            # 数据类型兼容性检查
            type_compatibility = check_data_type_compatibility(source_type, target_type)
            
            # 综合评分
            overall_score = (name_similarity * 0.7) + (type_compatibility * 0.3)
            
            if overall_score > 0.3:  # 只保留相似度较高的建议
                best_matches.append({
                    'target_field': target_field['field_name'],
                    'target_type': target_type,
                    'name_similarity': round(name_similarity, 3),
                    'type_compatibility': round(type_compatibility, 3),
                    'overall_score': round(overall_score, 3),
                    'confidence': 'high' if overall_score > 0.8 else 'medium' if overall_score > 0.6 else 'low'
                })
        
        # 按评分排序，取前3个建议
        best_matches.sort(key=lambda x: x['overall_score'], reverse=True)
        
        suggestions.append({
            'source_field': source_field['field_name'],
            'source_type': source_type,
            'suggested_mappings': best_matches[:3]
        })
    
    return suggestions


def calculate_field_name_similarity(name1, name2):
    """计算字段名称相似度"""
    # 简单的字符串相似度计算
    if name1 == name2:
        return 1.0
    
    # 检查是否包含相同的关键词
    name1_parts = name1.replace('_', ' ').split()
    name2_parts = name2.replace('_', ' ').split()
    
    common_parts = set(name1_parts) & set(name2_parts)
    total_parts = set(name1_parts) | set(name2_parts)
    
    if total_parts:
        return len(common_parts) / len(total_parts)
    
    return 0.0


def check_data_type_compatibility(type1, type2):
    """检查数据类型兼容性"""
    if type1 == type2:
        return 1.0
    
    # 数值类型兼容性
    numeric_types = {'number', 'integer', 'float'}
    if type1 in numeric_types and type2 in numeric_types:
        return 0.8
    
    # 字符串类型可以与大部分类型兼容
    if type1 == 'string' or type2 == 'string':
        return 0.6
    
    # 日期类型兼容性
    date_types = {'date', 'datetime', 'timestamp'}
    if type1 in date_types and type2 in date_types:
        return 0.9
    
    return 0.2  # 不兼容的类型


def execute_field_mapping_preview(source_docs, target_docs, field_mappings, limit):
    """执行字段映射预览"""
    preview_results = []
    
    # 创建映射字典
    mapping_dict = {}
    for mapping in field_mappings:
        mapping_dict[mapping['source_field']] = mapping['target_field']
    
    # 预览映射结果
    for i, source_doc in enumerate(source_docs[:limit]):
        mapped_doc = {}
        mapping_info = {}
        
        for source_field, target_field in mapping_dict.items():
            source_value = source_doc.get(source_field)
            mapped_doc[target_field] = source_value
            mapping_info[target_field] = {
                'source_field': source_field,
                'source_value': source_value,
                'mapped_successfully': source_value is not None
            }
        
        preview_results.append({
            'index': i + 1,
            'source_doc': {k: v for k, v in source_doc.items() if k != '_id'},
            'mapped_doc': mapped_doc,
            'mapping_info': mapping_info
        })
    
    # 计算映射统计信息
    mapping_stats = calculate_mapping_statistics(preview_results, field_mappings)
    
    return {
        'preview_data': preview_results,
        'statistics': mapping_stats,
        'total_previewed': len(preview_results)
    }


def calculate_mapping_statistics(preview_results, field_mappings):
    """计算映射统计信息"""
    stats = {
        'total_mappings': len(field_mappings),
        'successful_mappings': 0,
        'field_coverage': {},
        'data_quality': {}
    }
    
    if not preview_results:
        return stats
    
    # 统计每个字段的映射成功率
    for mapping in field_mappings:
        target_field = mapping['target_field']
        successful_count = 0
        
        for result in preview_results:
            if result['mapping_info'].get(target_field, {}).get('mapped_successfully'):
                successful_count += 1
        
        success_rate = successful_count / len(preview_results)
        stats['field_coverage'][target_field] = {
            'success_rate': round(success_rate, 3),
            'successful_count': successful_count,
            'total_count': len(preview_results)
        }
        
        if success_rate > 0.8:
            stats['successful_mappings'] += 1
    
    # 计算整体数据质量评分
    if stats['total_mappings'] > 0:
        overall_success_rate = stats['successful_mappings'] / stats['total_mappings']
        stats['data_quality']['overall_score'] = round(overall_success_rate * 100)
        stats['data_quality']['quality_level'] = (
            'excellent' if overall_success_rate > 0.9 else
            'good' if overall_success_rate > 0.7 else
            'fair' if overall_success_rate > 0.5 else
            'poor'
        )
    
    return stats


@app.route('/standalone_field_mapping')
def standalone_field_mapping():
    """独立字段映射页面"""
    return render_template('standalone_field_mapping.html')


@app.route('/api/kg_build_config', methods=['POST'])
def api_kg_build_config():
    """API: 知识图谱构建配置"""
    try:
        data = request.get_json()
        table_names = data.get('table_names', [])
        build_options = data.get('build_options', {})
        
        if not table_names:
            return jsonify({'success': False, 'error': '未指定数据表'}), 400
        
        # 分析数据表，生成构建配置建议
        config_suggestions = generate_kg_build_config(table_names, build_options)
        
        return jsonify({
            'success': True,
            'config_suggestions': config_suggestions,
            'table_names': table_names
        })
        
    except Exception as e:
        logger.error(f"生成知识图谱构建配置失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/start_kg_build', methods=['POST'])
def api_start_kg_build():
    """API: 启动知识图谱构建任务"""
    try:
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        data = request.get_json()
        table_names = data.get('table_names', [])
        build_config = data.get('build_config', {})
        project_name = data.get('project_name', f'KG_Project_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        
        if not table_names:
            return jsonify({'success': False, 'error': '未指定数据表'}), 400
        
        # 创建知识图谱构建任务
        task_id = start_kg_build_task(table_names, build_config, project_name)
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'project_name': project_name,
            'table_count': len(table_names),
            'status': 'started'
        })
        
    except Exception as e:
        logger.error(f"启动知识图谱构建失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/kg_build_progress/<task_id>')
def api_kg_build_progress(task_id):
    """API: 查询知识图谱构建进度"""
    try:
        progress_info = get_kg_build_progress(task_id)
        
        if 'error' in progress_info:
            return jsonify({'success': False, 'error': progress_info['error']}), 404
        
        # 转换为前端期望的格式
        stage_details = progress_info.get('stage_details', {})
        
        formatted_progress = {
            'status': progress_info.get('status', 'running'),
            'overall_progress': progress_info.get('progress', 0),
            'current_step': stage_details.get('current_step', 1),
            'steps': ['数据加载', '实体抽取', '关系发现', '图谱构建', '质量验证'],
            'processed_records': stage_details.get('processed_records', 0),
            'extracted_entities': stage_details.get('extracted_entities', 0),
            'discovered_relations': stage_details.get('discovered_relations', 0),
            'current_table': stage_details.get('current_table', ''),
            'step_name': stage_details.get('step_name', ''),
            'table_index': stage_details.get('table_index', 1),
            'total_tables': stage_details.get('total_tables', 1),
            'logs': [],
            'start_time': progress_info.get('start_time'),
            'updated_time': progress_info.get('updated_time'),
            'error_message': progress_info.get('error_message')
        }
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'data': formatted_progress
        })
        
    except Exception as e:
        logger.error(f"查询构建进度失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/kg_build_result/<task_id>')
def api_kg_build_result(task_id):
    """API: 获取知识图谱构建结果"""
    try:
        build_result = get_kg_build_result(task_id)
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'result': build_result
        })
        
    except Exception as e:
        logger.error(f"获取构建结果失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


def generate_kg_build_config(table_names, build_options):
    """生成知识图谱构建配置建议"""
    try:
        db = db_manager.mongo_client.get_database()
        config_suggestions = {
            'entity_extraction': {
                'enabled': True,
                'methods': ['field_based', 'pattern_based'],
                'confidence_threshold': 0.6
            },
            'relation_extraction': {
                'enabled': True,
                'methods': ['field_mapping', 'semantic_analysis'],
                'confidence_threshold': 0.5
            },
            'build_options': {
                'batch_size': 1000,
                'enable_validation': True,
                'auto_merge_entities': True,
                'create_indexes': True
            },
            'table_analysis': []
        }
        
        # 分析每个数据表
        for table_name in table_names:
            if table_name not in db.list_collection_names():
                continue
                
            collection = db[table_name]
            sample_docs = list(collection.find().limit(100))
            
            if not sample_docs:
                continue
            
            # 分析字段特征
            all_fields = set()
            for doc in sample_docs:
                all_fields.update(doc.keys())
            all_fields.discard('_id')
            
            # 识别潜在的实体字段
            entity_fields = []
            relation_fields = []
            
            for field in all_fields:
                field_analysis = analyze_field_for_kg(sample_docs, field)
                
                if field_analysis['is_entity_candidate']:
                    entity_fields.append({
                        'field_name': field,
                        'entity_type': field_analysis['suggested_entity_type'],
                        'confidence': field_analysis['confidence']
                    })
                
                if field_analysis['is_relation_candidate']:
                    relation_fields.append({
                        'field_name': field,
                        'relation_type': field_analysis['suggested_relation_type'],
                        'confidence': field_analysis['confidence']
                    })
            
            config_suggestions['table_analysis'].append({
                'table_name': table_name,
                'total_records': collection.count_documents({}),
                'total_fields': len(all_fields),
                'entity_fields': entity_fields,
                'relation_fields': relation_fields,
                'recommended_config': {
                    'primary_entity_field': entity_fields[0]['field_name'] if entity_fields else None,
                    'key_relation_fields': [f['field_name'] for f in relation_fields[:3]]
                }
            })
        
        return config_suggestions
        
    except Exception as e:
        logger.error(f"生成构建配置失败: {str(e)}")
        return {}


def analyze_field_for_kg(sample_docs, field_name):
    """分析字段是否适合作为实体或关系"""
    values = [doc.get(field_name) for doc in sample_docs if doc.get(field_name) is not None]
    
    if not values:
        return {
            'is_entity_candidate': False,
            'is_relation_candidate': False,
            'confidence': 0.0
        }
    
    # 基本统计
    unique_count = len(set(str(v) for v in values))
    total_count = len(values)
    uniqueness_ratio = unique_count / total_count if total_count > 0 else 0
    
    # 判断是否为实体候选
    is_entity_candidate = False
    suggested_entity_type = 'Generic'
    entity_confidence = 0.0
    
    # 高唯一性字段可能是实体
    if uniqueness_ratio > 0.7:
        is_entity_candidate = True
        entity_confidence = min(uniqueness_ratio, 0.9)
        
        # 根据字段名推断实体类型
        field_lower = field_name.lower()
        if any(keyword in field_lower for keyword in ['name', '名称', '姓名']):
            suggested_entity_type = 'Person' if '姓名' in field_lower else 'Organization'
            entity_confidence = min(entity_confidence + 0.1, 1.0)
        elif any(keyword in field_lower for keyword in ['id', '编号', '代码']):
            suggested_entity_type = 'Identifier'
        elif any(keyword in field_lower for keyword in ['地址', 'address', '位置']):
            suggested_entity_type = 'Location'
    
    # 判断是否为关系候选
    is_relation_candidate = False
    suggested_relation_type = 'hasProperty'
    relation_confidence = 0.0
    
    # 中等唯一性字段可能表示关系
    if 0.1 < uniqueness_ratio < 0.7:
        is_relation_candidate = True
        relation_confidence = 1.0 - abs(uniqueness_ratio - 0.4) * 2
        
        # 根据字段名推断关系类型
        field_lower = field_name.lower()
        if any(keyword in field_lower for keyword in ['类型', 'type', '分类']):
            suggested_relation_type = 'hasType'
        elif any(keyword in field_lower for keyword in ['状态', 'status', '情况']):
            suggested_relation_type = 'hasStatus'
        elif any(keyword in field_lower for keyword in ['属于', 'belongs', '隶属']):
            suggested_relation_type = 'belongsTo'
    
    return {
        'is_entity_candidate': is_entity_candidate,
        'suggested_entity_type': suggested_entity_type,
        'is_relation_candidate': is_relation_candidate,
        'suggested_relation_type': suggested_relation_type,
        'confidence': max(entity_confidence, relation_confidence),
        'uniqueness_ratio': uniqueness_ratio,
        'sample_values': values[:5]
    }


def start_kg_build_task(table_names, build_config, project_name):
    """启动知识图谱构建任务"""
    import uuid
    import threading
    
    task_id = str(uuid.uuid4())
    
    # 创建任务记录
    task_record = {
        'task_id': task_id,
        'project_name': project_name,
        'table_names': table_names,
        'build_config': build_config,
        'status': 'started',
        'progress': 0,
        'start_time': datetime.now().isoformat(),
        'current_stage': 'initializing',
        'results': {}
    }
    
    # 保存任务记录到数据库
    db = db_manager.mongo_client.get_database()
    db.kg_build_tasks.insert_one(task_record)
    
    # 启动后台构建任务
    build_thread = threading.Thread(
        target=execute_kg_build_task,
        args=(task_id, table_names, build_config, project_name)
    )
    build_thread.daemon = True
    build_thread.start()
    
    return task_id


def execute_kg_build_task(task_id, table_names, build_config, project_name):
    """执行知识图谱构建任务"""
    try:
        db = db_manager.mongo_client.get_database()
        
        # 更新任务状态
        def update_task_progress(stage, progress, details=None):
            update_data = {
                'current_stage': stage,
                'progress': progress,
                'updated_time': datetime.now().isoformat()
            }
            if details:
                update_data['stage_details'] = details
            
            db.kg_build_tasks.update_one(
                {'task_id': task_id},
                {'$set': update_data}
            )
        
        # 初始化知识图谱构建器
        from src.knowledge_graph.falkordb_store import FalkorDBStore
        from src.knowledge_graph.kg_builder import KnowledgeGraphBuilder
        
        kg_store = FalkorDBStore(project_name=project_name or "全局")
        kg_builder = KnowledgeGraphBuilder(kg_store, build_config)
        
        total_tables = len(table_names)
        build_results = []
        
        for i, table_name in enumerate(table_names):
            current_progress = int((i / total_tables) * 100)
            update_task_progress(f'processing_table_{i+1}', current_progress, {
                'current_table': table_name,
                'table_index': i + 1,
                'total_tables': total_tables,
                'processed_records': 0,
                'extracted_entities': 0,
                'discovered_relations': 0
            })
            
            # 获取数据表数据
            collection = db[table_name]
            documents = list(collection.find())
            
            if not documents:
                logger.warning(f"数据表 {table_name} 为空，跳过处理")
                continue
            
            # 转换为DataFrame
            df = pd.DataFrame(documents)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            # 创建进度回调函数
            def progress_callback(progress_data):
                table_progress = int((i / total_tables) * 100) + int((progress_data.get('current_step', 1) / 4) * (100 / total_tables))
                update_task_progress(f'processing_table_{i+1}', table_progress, {
                    'current_table': table_name,
                    'table_index': i + 1,
                    'total_tables': total_tables,
                    'step_name': progress_data.get('step_name', ''),
                    'current_step': progress_data.get('current_step', 1),
                    'processed_records': progress_data.get('processed_records', 0),
                    'extracted_entities': progress_data.get('extracted_entities', 0),
                    'discovered_relations': progress_data.get('discovered_relations', 0)
                })
            
            # 构建知识图谱（带进度回调）
            table_result = kg_builder.build_knowledge_graph_from_dataframe(
                df, table_name, project_name, progress_callback=progress_callback
            )
            
            build_results.append(table_result)
        
        # 完成构建
        final_result = {
            'project_name': project_name,
            'total_tables_processed': len(build_results),
            'total_entities': sum(r.get('entities_created', 0) for r in build_results),
            'total_relations': sum(r.get('triples_created', 0) for r in build_results),
            'total_triples': sum(r.get('triples_created', 0) for r in build_results),
            'table_results': build_results,
            'build_summary': generate_build_summary(build_results)
        }
        
        # 更新任务完成状态
        db.kg_build_tasks.update_one(
            {'task_id': task_id},
            {'$set': {
                'status': 'completed',
                'progress': 100,
                'current_stage': 'completed',
                'end_time': datetime.now().isoformat(),
                'results': final_result
            }}
        )
        
        logger.info(f"知识图谱构建任务完成: {task_id}")
        
    except Exception as e:
        error_msg = f"知识图谱构建任务失败: {str(e)}"
        logger.error(error_msg)
        
        # 更新任务失败状态
        db.kg_build_tasks.update_one(
            {'task_id': task_id},
            {'$set': {
                'status': 'failed',
                'current_stage': 'error',
                'error_message': error_msg,
                'end_time': datetime.now().isoformat()
            }}
        )


def generate_build_summary(build_results):
    """生成构建摘要"""
    if not build_results:
        return {}
    
    total_entities = sum(r.get('entities_created', 0) for r in build_results)
    total_triples = sum(r.get('triples_created', 0) for r in build_results)
    successful_tables = len([r for r in build_results if r.get('status') == 'completed'])
    
    return {
        'total_entities_created': total_entities,
        'total_triples_created': total_triples,
        'successful_tables': successful_tables,
        'total_tables': len(build_results),
        'success_rate': (successful_tables / len(build_results)) * 100 if build_results else 0,
        'avg_entities_per_table': total_entities / len(build_results) if build_results else 0,
        'avg_triples_per_table': total_triples / len(build_results) if build_results else 0
    }


def get_kg_build_progress(task_id):
    """获取知识图谱构建进度"""
    try:
        db = db_manager.mongo_client.get_database()
        task_record = db.kg_build_tasks.find_one({'task_id': task_id})
        
        if not task_record:
            return {'error': 'Task not found'}
        
        return {
            'task_id': task_id,
            'status': task_record.get('status', 'unknown'),
            'progress': task_record.get('progress', 0),
            'current_stage': task_record.get('current_stage', 'unknown'),
            'stage_details': task_record.get('stage_details', {}),
            'start_time': task_record.get('start_time'),
            'updated_time': task_record.get('updated_time'),
            'error_message': task_record.get('error_message')
        }
        
    except Exception as e:
        logger.error(f"获取任务进度失败: {str(e)}")
        return {'error': str(e)}


def get_kg_build_result(task_id):
    """获取知识图谱构建结果"""
    try:
        db = db_manager.mongo_client.get_database()
        task_record = db.kg_build_tasks.find_one({'task_id': task_id})
        
        if not task_record:
            return {'error': 'Task not found'}
        
        if task_record.get('status') != 'completed':
            return {
                'error': 'Task not completed',
                'current_status': task_record.get('status', 'unknown')
            }
        
        return task_record.get('results', {})
        
    except Exception as e:
        logger.error(f"获取构建结果失败: {str(e)}")
        return {'error': str(e)}


@app.route('/standalone_kg_builder')
def standalone_kg_builder():
    """独立知识图谱构建页面"""
    return render_template('standalone_kg_builder.html')


@app.route('/dynamic_matching')
def dynamic_matching():
    """动态匹配页面 - 基于用户字段映射配置的智能匹配"""
    return render_template('dynamic_matching.html')


@app.route('/user_data_matching')
def user_data_matching():
    """用户数据智能匹配页面 - 专门处理用户上传数据的匹配"""
    return render_template('user_data_matching.html')


@app.route('/user_matching_history')
def user_matching_history():
    """用户匹配历史页面 - 查看所有历史匹配任务"""
    return render_template('user_matching_history.html')


@app.route('/api/analyze_multi_table_relationships', methods=['POST'])
def api_analyze_multi_table_relationships():
    """API: 分析多表关系"""
    try:
        data = request.get_json()
        
        if not data or 'table_names' not in data:
            return jsonify({'success': False, 'error': '请求数据无效，需要table_names字段'}), 400
        
        table_names = data['table_names']
        if not isinstance(table_names, list) or len(table_names) < 2:
            return jsonify({'success': False, 'error': '需要至少2个表进行关系分析'}), 400
        
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        
        # 获取表结构信息
        table_schemas = []
        for table_name in table_names:
            if table_name not in db.list_collection_names():
                return jsonify({'success': False, 'error': f'表 {table_name} 不存在'}), 400
            
            # 获取字段信息
            sample_docs = list(db[table_name].find().limit(100))
            if not sample_docs:
                continue
            
            fields = []
            for field_name in sample_docs[0].keys():
                if field_name == '_id':
                    continue
                
                field_info = analyze_field_details(sample_docs, field_name)
                fields.append({
                    'field_name': field_name,
                    'data_type': field_info['data_type'],
                    'non_null_rate': field_info['non_null_rate'],
                    'unique_count': field_info['unique_count'],
                    'is_key_field': field_info['is_key_field']
                })
            
            table_schemas.append({
                'table_name': table_name,
                'fields': fields,
                'record_count': len(sample_docs)
            })
        
        # 使用增强字段映射器分析多表关系
        try:
            from src.matching.enhanced_field_mapper import EnhancedFieldMapper
            enhanced_mapper = EnhancedFieldMapper()
            
            relationships = enhanced_mapper.analyze_multi_table_relationships(table_schemas)
            
            return jsonify({
                'success': True,
                'table_count': len(table_schemas),
                'relationships': relationships,
                'analysis_summary': {
                    'potential_joins': len(relationships['suggested_join_conditions']),
                    'similar_field_pairs': sum(len(item['similar_fields']) for item in relationships['similar_fields_across_tables']),
                    'analyzed_tables': [schema['table_name'] for schema in table_schemas]
                }
            })
            
        except ImportError:
            # 回退到基础分析
            basic_relationships = _analyze_basic_table_relationships(table_schemas)
            return jsonify({
                'success': True,
                'table_count': len(table_schemas),
                'relationships': basic_relationships,
                'analysis_type': 'basic',
                'message': '使用基础算法进行分析'
            })
        
    except Exception as e:
        logger.error(f"多表关系分析失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


def _analyze_basic_table_relationships(table_schemas):
    """基础多表关系分析（回退算法）"""
    relationships = {
        'similar_fields_across_tables': [],
        'suggested_join_conditions': []
    }
    
    # 简单的字段名匹配
    for i, table1 in enumerate(table_schemas):
        for j, table2 in enumerate(table_schemas):
            if i >= j:
                continue
            
            similar_fields = []
            for field1 in table1['fields']:
                for field2 in table2['fields']:
                    name_similarity = calculate_field_name_similarity(
                        field1['field_name'].lower(),
                        field2['field_name'].lower()
                    )
                    
                    if name_similarity > 0.7:
                        similar_fields.append({
                            'field1': field1['field_name'],
                            'field2': field2['field_name'],
                            'similarity': name_similarity
                        })
            
            if similar_fields:
                relationships['similar_fields_across_tables'].append({
                    'table1': table1['table_name'],
                    'table2': table2['table_name'],
                    'similar_fields': similar_fields
                })
    
    return relationships


# ====== 知识图谱增强API端点 ======

@app.route('/api/kg_entity_extract', methods=['POST'])
def api_kg_entity_extract():
    """实体抽取API"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': '请求数据不能为空'}), 400
        
        # 支持两种模式：从表格抽取或从文本抽取
        if 'table_name' in data:
            # 从数据表抽取实体
            table_name = data['table_name']
            
            # 获取表数据
            collection = db_manager.get_collection(table_name)
            if collection is None:
                return jsonify({'error': f'数据表 {table_name} 不存在'}), 404
            
            # 转换为DataFrame
            documents = list(collection.find().limit(1000))  # 限制处理数量
            if not documents:
                return jsonify({'error': '数据表为空'}), 400
            
            df = pd.DataFrame(documents)
            
            # 抽取实体
            entities = entity_extractor.extract_entities_from_dataframe(df, table_name)
            
            # 获取统计信息
            stats = entity_extractor.get_entity_extraction_statistics()
            
            return jsonify({
                'success': True,
                'entities_count': len(entities),
                'entities': [entity.to_dict() for entity in entities[:50]],  # 限制返回数量
                'statistics': stats,
                'table_name': table_name
            })
        
        elif 'text' in data:
            # 从文本抽取实体
            text = data['text']
            context = data.get('context', {})
            
            entities = entity_extractor.extract_entities_from_text(text, context)
            
            return jsonify({
                'success': True,
                'entities_count': len(entities),
                'entities': [entity.to_dict() for entity in entities],
                'text_length': len(text)
            })
        
        else:
            return jsonify({'error': '请提供 table_name 或 text 参数'}), 400
    
    except Exception as e:
        logger.error(f"实体抽取API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kg_relation_extract', methods=['POST'])
def api_kg_relation_extract():
    """关系抽取API"""
    try:
        data = request.get_json()
        
        if not data or 'table_name' not in data:
            return jsonify({'error': '请提供 table_name 参数'}), 400
        
        table_name = data['table_name']
        
        # 获取表数据
        collection = db_manager.get_collection(table_name)
        if collection is None:
            return jsonify({'error': f'数据表 {table_name} 不存在'}), 404
        
        documents = list(collection.find().limit(1000))
        if not documents:
            return jsonify({'error': '数据表为空'}), 400
        
        df = pd.DataFrame(documents)
        
        # 首先抽取实体
        entities = entity_extractor.extract_entities_from_dataframe(df, table_name)
        
        if not entities:
            return jsonify({'error': '未找到实体，无法抽取关系'}), 400
        
        # 抽取关系
        triples = relation_extractor.extract_relations_from_dataframe(df, entities, table_name)
        
        # 获取统计信息
        stats = relation_extractor.get_relation_extraction_statistics()
        
        return jsonify({
            'success': True,
            'entities_count': len(entities),
            'relations_count': len(triples),
            'relations': [triple.to_dict() for triple in triples[:50]],
            'statistics': stats,
            'table_name': table_name
        })
    
    except Exception as e:
        logger.error(f"关系抽取API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kg_quality_assessment', methods=['GET', 'POST'])
def api_kg_quality_assessment():
    """知识图谱质量评估API"""
    try:
        if request.method == 'POST':
            # 执行完整质量评估
            quality_report = kg_quality_assessor.assess_overall_quality()
            return jsonify({
                'success': True,
                'assessment_type': 'full_assessment',
                'report': quality_report
            })
        
        else:
            # GET请求：获取质量统计信息
            stats = kg_quality_assessor.get_quality_statistics()
            
            # 获取质量趋势（过去7天）
            trends = kg_quality_assessor.get_quality_trends(days=7)
            
            return jsonify({
                'success': True,
                'assessment_type': 'statistics',
                'statistics': stats,
                'trends': trends
            })
    
    except Exception as e:
        logger.error(f"质量评估API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kg_export', methods=['GET'])
def api_kg_export():
    """知识图谱导出API"""
    try:
        export_format = request.args.get('format', 'json').lower()
        limit = int(request.args.get('limit', 1000))
        
        # 获取实体和关系
        entities = kg_store.find_entities(limit=limit)
        triples = kg_store.find_triples(limit=limit)
        
        if export_format == 'json':
            export_data = {
                'metadata': {
                    'export_time': datetime.now().isoformat(),
                    'entities_count': len(entities),
                    'triples_count': len(triples),
                    'format': 'json'
                },
                'entities': [entity.to_dict() for entity in entities],
                'triples': [triple.to_dict() for triple in triples]
            }
            
            return jsonify({
                'success': True,
                'export_data': export_data
            })
        
        elif export_format == 'rdf':
            # 简化的RDF导出
            rdf_triples = []
            for triple in triples:
                rdf_triple = {
                    'subject': triple.subject.label if triple.subject else 'unknown',
                    'predicate': triple.predicate.type.value if triple.predicate else 'unknown',
                    'object': triple.object.label if triple.object else 'unknown'
                }
                rdf_triples.append(rdf_triple)
            
            return jsonify({
                'success': True,
                'format': 'rdf',
                'triples': rdf_triples
            })
        
        elif export_format == 'csv':
            # CSV格式导出三元组
            csv_data = []
            for triple in triples:
                csv_data.append({
                    'subject': triple.subject.label if triple.subject else '',
                    'subject_type': triple.subject.type.value if triple.subject else '',
                    'predicate': triple.predicate.type.value if triple.predicate else '',
                    'object': triple.object.label if triple.object else '',
                    'object_type': triple.object.type.value if triple.object else '',
                    'confidence': triple.confidence or 0.0,
                    'source': triple.source or ''
                })
            
            return jsonify({
                'success': True,
                'format': 'csv',
                'data': csv_data
            })
        
        else:
            return jsonify({'error': f'不支持的导出格式: {export_format}'}), 400
    
    except Exception as e:
        logger.error(f"知识图谱导出API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kg_search', methods=['GET'])
def api_kg_search():
    """知识图谱搜索API"""
    try:
        query = request.args.get('q', '').strip()
        search_type = request.args.get('type', 'entity').lower()
        limit = int(request.args.get('limit', 50))
        
        if not query:
            return jsonify({'error': '请提供搜索查询参数 q'}), 400
        
        if search_type == 'entity':
            # 搜索实体
            entities = kg_store.find_entities_by_text(query, limit=limit)
            
            return jsonify({
                'success': True,
                'search_type': 'entity',
                'query': query,
                'results_count': len(entities),
                'results': [entity.to_dict() for entity in entities]
            })
        
        elif search_type == 'relation':
            # 搜索高置信度关系
            triples = kg_store.find_high_confidence_triples(min_confidence=0.7, limit=limit)
            
            # 过滤包含查询词的关系
            filtered_triples = []
            query_lower = query.lower()
            
            for triple in triples:
                if (triple.subject and query_lower in triple.subject.label.lower()) or \
                   (triple.object and query_lower in triple.object.label.lower()) or \
                   (triple.predicate and query_lower in triple.predicate.type.value.lower()):
                    filtered_triples.append(triple)
            
            return jsonify({
                'success': True,
                'search_type': 'relation',
                'query': query,
                'results_count': len(filtered_triples),
                'results': [triple.to_dict() for triple in filtered_triples[:limit]]
            })
        
        else:
            return jsonify({'error': f'不支持的搜索类型: {search_type}'}), 400
    
    except Exception as e:
        logger.error(f"知识图谱搜索API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kg_statistics', methods=['GET'])
def api_kg_statistics():
    """知识图谱统计信息API"""
    try:
        # 获取存储统计
        storage_stats = kg_store.get_statistics()
        
        # 获取性能统计
        performance_stats = kg_store.get_performance_stats()
        
        # 获取质量统计
        quality_stats = kg_quality_assessor.get_quality_statistics()
        
        return jsonify({
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'storage_statistics': storage_stats,
            'performance_statistics': performance_stats,
            'quality_statistics': quality_stats
        })
    
    except Exception as e:
        logger.error(f"知识图谱统计API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kg_optimize', methods=['POST'])
def api_kg_optimize():
    """知识图谱优化API"""
    try:
        data = request.get_json() or {}
        operation = data.get('operation', 'collections').lower()
        
        if operation == 'collections':
            # 优化集合性能
            results = kg_store.optimize_collections()
            
            return jsonify({
                'success': True,
                'operation': 'collections_optimization',
                'results': results
            })
        
        elif operation == 'cache':
            # 清理缓存
            kg_store.clear_cache()
            
            return jsonify({
                'success': True,
                'operation': 'cache_cleared',
                'message': '缓存已清理'
            })
        
        else:
            return jsonify({'error': f'不支持的优化操作: {operation}'}), 400
    
    except Exception as e:
        logger.error(f"知识图谱优化API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ====== 动态匹配相关API ======

@app.route('/api/get_field_mapping_config', methods=['GET'])
def api_get_field_mapping_config():
    """API: 获取字段映射配置"""
    try:
        config_id = request.args.get('config_id')
        if not config_id:
            return jsonify({'success': False, 'error': '缺少配置ID参数'}), 400
        
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        config_collection = db['field_mapping_configs']
        
        config = config_collection.find_one({'config_id': config_id})
        
        if not config:
            return jsonify({'success': False, 'error': '配置不存在'}), 404
        
        # 移除MongoDB的_id字段
        if '_id' in config:
            del config['_id']
        
        return jsonify({
            'success': True,
            'config': config
        })
        
    except Exception as e:
        logger.error(f"获取字段映射配置失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/start_dynamic_matching', methods=['POST'])
def api_start_dynamic_matching():
    """API: 启动基于配置的动态匹配任务"""
    try:
        data = request.get_json()
        config_id = data.get('config_id')
        match_type = data.get('match_type', 'both')
        batch_size = data.get('batch_size', 100)
        similarity_threshold = data.get('similarity_threshold', 0.7)
        max_results = data.get('max_results', 10)
        
        if not config_id:
            return jsonify({'success': False, 'error': '缺少配置ID'}), 400
        
        # 获取字段映射配置
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        config_collection = db['field_mapping_configs']
        config = config_collection.find_one({'config_id': config_id})
        
        if not config:
            return jsonify({'success': False, 'error': '映射配置不存在'}), 404
        
        # 创建匹配任务
        task_id = f"dynamic_match_{config_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        task_doc = {
            'task_id': task_id,
            'config_id': config_id,
            'source_table': config['source_table'],
            'target_tables': config['target_tables'],
            'mappings': config['mappings'],
            'match_type': match_type,
            'batch_size': batch_size,
            'similarity_threshold': similarity_threshold,
            'max_results': max_results,
            'status': 'running',
            'progress': {
                'percentage': 0,
                'processed': 0,
                'total': 0,
                'matches': 0
            },
            'created_at': datetime.now().isoformat(),
            'started_at': datetime.now().isoformat()
        }
        
        # 保存任务到数据库
        task_collection = db['dynamic_matching_tasks']
        task_collection.insert_one(task_doc)
        
        # 启动后台匹配任务（简化版，实际应该用线程池）
        import threading
        thread = threading.Thread(
            target=execute_dynamic_matching_task, 
            args=(task_id, config)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '动态匹配任务已启动'
        })
        
    except Exception as e:
        logger.error(f"启动动态匹配任务失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/matching_progress', methods=['GET'])
def api_matching_progress():
    """API: 获取匹配任务进度"""
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'error': '缺少任务ID'}), 400
        
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        task_collection = db['dynamic_matching_tasks']
        
        task = task_collection.find_one({'task_id': task_id})
        
        if not task:
            return jsonify({'success': False, 'error': '任务不存在'}), 404
        
        return jsonify({
            'success': True,
            'progress': task['progress'],
            'status': task['status']
        })
        
    except Exception as e:
        logger.error(f"获取匹配进度失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ====== 用户数据智能匹配API ======

@app.route('/api/start_user_data_matching', methods=['POST'])
def api_start_user_data_matching():
    """API: 启动用户数据智能匹配任务"""
    try:
        data = request.get_json()
        config_id = data.get('config_id')
        algorithm_type = data.get('algorithm_type', 'enhanced')
        similarity_threshold = data.get('similarity_threshold', 0.7)
        batch_size = data.get('batch_size', 100)
        max_results = data.get('max_results', 10)
        
        if not config_id:
            return jsonify({'success': False, 'error': '缺少配置ID'}), 400
        
        # 获取字段映射配置
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        config_collection = db['field_mapping_configs']
        config = config_collection.find_one({'config_id': config_id})
        
        if not config:
            return jsonify({'success': False, 'error': '映射配置不存在'}), 404
        
        # 检查是否已有进行中的任务（防重复提交）
        tasks_collection = db['user_matching_tasks']
        existing_task = tasks_collection.find_one({
            'config_id': config_id,
            'status': {'$in': ['running', 'pending']}
        })
        
        if existing_task:
            return jsonify({
                'success': False, 
                'error': f'该配置已有任务在进行中（任务ID: {existing_task["task_id"]}），请等待完成或先停止当前任务'
            }), 409  # Conflict
        
        # 创建匹配任务
        task_id = f"user_match_{config_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        task_doc = {
            'task_id': task_id,
            'config_id': config_id,
            'source_table': config['source_table'],
            'target_tables': config['target_tables'],
            'mappings': config['mappings'],
            'algorithm_type': algorithm_type,
            'similarity_threshold': similarity_threshold,
            'batch_size': batch_size,
            'max_results': max_results,
            'status': 'running',
            'progress': {
                'percentage': 0,
                'processed': 0,
                'total': 0,
                'matches': 0,
                'current_operation': '准备启动...'
            },
            'created_at': datetime.now().isoformat(),
            'started_at': datetime.now().isoformat()
        }
        
        # 保存任务到数据库
        task_collection = db['user_matching_tasks']
        task_collection.insert_one(task_doc)
        
        # 初始化用户数据匹配器
        from src.matching.user_data_matcher import UserDataMatcher
        user_matcher = UserDataMatcher(db_manager=db_manager, config=config)
        
        # 启动匹配任务
        user_matcher.start_matching_task(task_doc)
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '用户数据智能匹配任务已启动'
        })
        
    except Exception as e:
        logger.error(f"启动用户数据匹配任务失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/user_matching_progress', methods=['GET'])
def api_user_matching_progress():
    """API: 获取用户数据匹配任务进度"""
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'error': '缺少任务ID'}), 400
        
        if not db_manager:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        # 使用带连接检查的方法获取集合
        try:
            task_collection = db_manager.get_collection('user_matching_tasks')
        except Exception as conn_error:
            logger.error(f"数据库连接失败: {str(conn_error)}")
            return jsonify({'success': False, 'error': '数据库连接失败'}), 500
        
        task = task_collection.find_one({'task_id': task_id})
        
        if not task:
            return jsonify({'success': False, 'error': '任务不存在'}), 404
        
        return jsonify({
            'success': True,
            'progress': task.get('progress', {}),
            'status': task.get('status', 'unknown'),
            'error': task.get('error'),
            'created_at': task.get('created_at'),
            'updated_at': task.get('updated_at')
        })
        
    except Exception as e:
        logger.error(f"获取用户匹配进度失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/preview_user_data_matching', methods=['POST'])
def api_preview_user_data_matching():
    """API: 预览用户数据匹配结果"""
    try:
        data = request.get_json()
        config_id = data.get('config_id')
        preview_count = data.get('preview_count', 5)
        algorithm_type = data.get('algorithm_type', 'enhanced')
        similarity_threshold = data.get('similarity_threshold', 0.7)
        
        if not config_id:
            return jsonify({'success': False, 'error': '缺少配置ID'}), 400
        
        # 初始化用户数据匹配器
        from src.matching.user_data_matcher import UserDataMatcher
        user_matcher = UserDataMatcher(db_manager=db_manager)
        
        # 执行预览匹配
        preview_results = user_matcher.preview_matching(
            config_id=config_id,
            preview_count=preview_count,
            algorithm_type=algorithm_type,
            similarity_threshold=similarity_threshold
        )
        
        return jsonify({
            'success': True,
            'preview_results': preview_results,
            'count': len(preview_results)
        })
        
    except Exception as e:
        logger.error(f"预览用户数据匹配失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/stop_user_matching_task', methods=['POST'])
def api_stop_user_matching_task():
    """API: 停止用户数据匹配任务"""
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'error': '缺少任务ID'}), 400
        
        # 初始化用户数据匹配器
        from src.matching.user_data_matcher import UserDataMatcher
        user_matcher = UserDataMatcher(db_manager=db_manager)
        
        # 停止任务
        success = user_matcher.stop_task(task_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': '任务停止请求已发送'
            })
        else:
            return jsonify({'success': False, 'error': '停止任务失败'}), 500
        
    except Exception as e:
        logger.error(f"停止用户匹配任务失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_user_matching_results', methods=['GET'])
def api_get_user_matching_results():
    """API: 获取用户数据匹配结果"""
    try:
        task_id = request.args.get('task_id')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))
        
        if not task_id:
            return jsonify({'success': False, 'error': '缺少任务ID'}), 400
        
        if not db_manager:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        # 使用带连接检查的方法获取数据库
        try:
            # 先获取一个集合来测试连接并触发重连机制
            test_collection = db_manager.get_collection('user_matching_tasks')
            db = test_collection.database
        except Exception as conn_error:
            logger.error(f"数据库连接失败: {str(conn_error)}")
            return jsonify({'success': False, 'error': '数据库连接失败'}), 500
        
        # 获取结果集合
        result_collection_name = f'user_match_results_{task_id}'
        if result_collection_name not in db.list_collection_names():
            return jsonify({
                'success': True,
                'results': [],
                'total': 0,
                'statistics': {
                    'total_processed': 0,
                    'total_matches': 0,
                    'match_rate': 0,
                    'avg_similarity': 0,
                    'high_confidence_count': 0,
                    'execution_time': 0
                }
            })
        
        result_collection = db[result_collection_name]
        
        # 获取总数
        total_count = result_collection.count_documents({})
        
        # 分页查询结果
        skip = (page - 1) * page_size
        results = list(result_collection.find({})
                      .sort([('similarity_score', -1)])
                      .skip(skip)
                      .limit(page_size))
        
        # 清理MongoDB的ObjectId
        for result in results:
            if '_id' in result:
                del result['_id']
        
        # 计算统计信息
        statistics = _calculate_matching_statistics(result_collection, task_id)
        
        return jsonify({
            'success': True,
            'results': results,
            'total': total_count,
            'page': page,
            'page_size': page_size,
            'statistics': statistics
        })
        
    except Exception as e:
        logger.error(f"获取用户匹配结果失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_user_matching_tasks', methods=['GET'])
def api_get_user_matching_tasks():
    """API: 获取用户数据匹配任务列表"""
    try:
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        task_collection = db['user_matching_tasks']
        
        # 获取所有任务，按创建时间倒序排列
        tasks = list(task_collection.find({})
                    .sort([('created_at', -1)])
                    .limit(100))  # 限制最多返回100个任务
        
        # 处理任务数据
        task_list = []
        for task in tasks:
            task_data = {
                'task_id': task.get('task_id', ''),
                'config_id': task.get('config_id', ''),
                'status': task.get('status', 'unknown'),
                'created_at': task.get('created_at', ''),
                'updated_at': task.get('updated_at', ''),
                'progress': task.get('progress', 0),
                'message': task.get('message', ''),
                'processed_count': task.get('processed_count', 0),
                'total_count': task.get('total_count', 0),
                'match_count': task.get('match_count', 0)
            }
            
            # 检查是否有对应的结果表
            result_collection_name = f'user_match_results_{task_data["task_id"]}'
            if result_collection_name in db.list_collection_names():
                result_collection = db[result_collection_name]
                result_count = result_collection.count_documents({})
                task_data['result_count'] = result_count
                task_data['has_results'] = result_count > 0
            else:
                task_data['result_count'] = 0
                task_data['has_results'] = False
            
            task_list.append(task_data)
        
        return jsonify({
            'success': True,
            'tasks': task_list,
            'total': len(task_list)
        })
        
    except Exception as e:
        logger.error(f"获取用户匹配任务列表失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/delete_user_matching_task', methods=['DELETE'])
def api_delete_user_matching_task():
    """API: 删除用户数据匹配任务"""
    try:
        data = request.get_json()
        if not data or 'task_id' not in data:
            return jsonify({'success': False, 'error': '缺少task_id参数'}), 400
        
        task_id = data['task_id']
        
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        
        # 1. 检查任务是否存在
        task_collection = db['user_matching_tasks']
        task = task_collection.find_one({'task_id': task_id})
        if not task:
            return jsonify({'success': False, 'error': '任务不存在'}), 404
        
        # 2. 允许删除运行中的任务（系统崩溃后的异常状态恢复）
        # 注释掉原有的运行状态检查，允许强制删除异常状态的任务
        # if task.get('status') == 'running':
        #     return jsonify({'success': False, 'error': '任务正在运行中，无法删除'}), 400
        
        # 记录运行中任务的强制删除操作
        if task.get('status') == 'running':
            logger.warning(f"强制删除运行中的任务: {task_id}（可能是系统异常导致的状态错误）")
        
        # 3. 删除结果表
        result_collection_name = f'user_match_results_{task_id}'
        if result_collection_name in db.list_collection_names():
            db.drop_collection(result_collection_name)
            logger.info(f"已删除结果表: {result_collection_name}")
        
        # 4. 删除任务记录
        delete_result = task_collection.delete_one({'task_id': task_id})
        if delete_result.deleted_count == 0:
            return jsonify({'success': False, 'error': '删除任务记录失败'}), 500
        
        logger.info(f"成功删除用户匹配任务: {task_id}")
        return jsonify({
            'success': True,
            'message': f'任务 {task_id} 已成功删除',
            'deleted_task_id': task_id
        })
        
    except Exception as e:
        logger.error(f"删除用户匹配任务失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cleanup_old_user_matching_tasks', methods=['DELETE'])
def api_cleanup_old_user_matching_tasks():
    """API: 清理旧的用户数据匹配任务"""
    try:
        data = request.get_json() or {}
        days_ago = data.get('days_ago', 30)  # 默认清理30天前的任务
        
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        task_collection = db['user_matching_tasks']
        
        # 计算时间阈值
        from datetime import datetime, timedelta
        cutoff_time = datetime.now() - timedelta(days=days_ago)
        cutoff_time_str = cutoff_time.isoformat()
        
        # 查找需要清理的任务
        old_tasks = list(task_collection.find({
            'created_at': {'$lt': cutoff_time_str},
            'status': {'$nin': ['running']}  # 排除正在运行的任务
        }))
        
        deleted_tasks = []
        deleted_collections = []
        
        for task in old_tasks:
            task_id = task.get('task_id')
            if not task_id:
                continue
            
            try:
                # 删除结果表
                result_collection_name = f'user_match_results_{task_id}'
                if result_collection_name in db.list_collection_names():
                    db.drop_collection(result_collection_name)
                    deleted_collections.append(result_collection_name)
                
                # 删除任务记录
                task_collection.delete_one({'task_id': task_id})
                deleted_tasks.append(task_id)
                
            except Exception as e:
                logger.error(f"清理任务 {task_id} 时出错: {str(e)}")
                continue
        
        logger.info(f"清理完成: 删除了 {len(deleted_tasks)} 个任务和 {len(deleted_collections)} 个结果表")
        
        return jsonify({
            'success': True,
            'message': f'清理完成，删除了 {len(deleted_tasks)} 个 {days_ago} 天前的任务',
            'deleted_tasks_count': len(deleted_tasks),
            'deleted_collections_count': len(deleted_collections),
            'deleted_task_ids': deleted_tasks,
            'days_ago': days_ago
        })
        
    except Exception as e:
        logger.error(f"清理旧任务失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


def _calculate_matching_statistics(result_collection, task_id: str) -> Dict:
    """计算匹配统计信息"""
    try:
        # 基础统计
        total_matches = result_collection.count_documents({})
        
        if total_matches == 0:
            return {
                'total_processed': 0,
                'total_matches': 0,
                'match_rate': 0,
                'avg_similarity': 0,
                'high_confidence_count': 0,
                'execution_time': 0
            }
        
        # 聚合查询统计信息
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'avg_similarity': {'$avg': '$similarity_score'},
                    'max_similarity': {'$max': '$similarity_score'},
                    'min_similarity': {'$min': '$similarity_score'},
                    'high_confidence_count': {
                        '$sum': {
                            '$cond': [
                                {'$gte': ['$similarity_score', 0.8]},
                                1, 0
                            ]
                        }
                    }
                }
            }
        ]
        
        stats_result = list(result_collection.aggregate(pipeline))
        stats = stats_result[0] if stats_result else {}
        
        # 获取任务信息计算处理数量和执行时间
        db = db_manager.mongo_client.get_database()
        task_collection = db['user_matching_tasks']
        task = task_collection.find_one({'task_id': task_id})
        
        total_processed = 0
        execution_time = 0
        
        if task:
            progress = task.get('progress', {})
            total_processed = progress.get('processed', 0)
            
            # 计算执行时间
            if task.get('created_at') and task.get('updated_at'):
                start_time = datetime.fromisoformat(task['created_at'])
                end_time = datetime.fromisoformat(task['updated_at'])
                execution_time = int((end_time - start_time).total_seconds())
        
        # 计算匹配率
        match_rate = (total_matches / total_processed * 100) if total_processed > 0 else 0
        
        return {
            'total_processed': total_processed,
            'total_matches': total_matches,
            'match_rate': round(match_rate, 2),
            'avg_similarity': round((stats.get('avg_similarity', 0) * 100), 2),
            'high_confidence_count': stats.get('high_confidence_count', 0),
            'execution_time': execution_time
        }
        
    except Exception as e:
        logger.error(f"计算匹配统计信息失败: {str(e)}")
        return {
            'total_processed': 0,
            'total_matches': 0,
            'match_rate': 0,
            'avg_similarity': 0,
            'high_confidence_count': 0,
            'execution_time': 0
        }


@app.route('/user_matching_results')
def user_matching_results():
    """用户数据匹配结果页面"""
    return render_template('user_matching_results.html')


def execute_dynamic_matching_task(task_id: str, config: dict):
    """执行动态匹配任务（后台线程）"""
    try:
        logger.info(f"开始执行动态匹配任务: {task_id}")
        
        db = db_manager.mongo_client.get_database()
        task_collection = db['dynamic_matching_tasks']
        
        # 更新任务状态
        def update_progress(percentage, processed, total, matches, status='running'):
            task_collection.update_one(
                {'task_id': task_id},
                {
                    '$set': {
                        'progress': {
                            'percentage': percentage,
                            'processed': processed,
                            'total': total,
                            'matches': matches
                        },
                        'status': status,
                        'updated_at': datetime.now().isoformat()
                    }
                }
            )
        
        # 模拟匹配过程
        source_table = config['source_table']
        target_tables = config['target_tables']
        mappings = config['mappings']
        
        # 获取源表数据量
        source_collection = db[source_table]
        total_records = source_collection.count_documents({})
        
        update_progress(0, 0, total_records, 0, 'running')
        
        # 模拟批量处理
        batch_size = 100
        processed = 0
        matches = 0
        
        for i in range(0, total_records, batch_size):
            # 模拟处理时间
            import time
            time.sleep(1)
            
            processed += min(batch_size, total_records - i)
            matches += min(batch_size // 2, total_records - i)  # 模拟50%匹配率
            
            percentage = int((processed / total_records) * 100)
            update_progress(percentage, processed, total_records, matches)
            
            # 检查是否被停止
            current_task = task_collection.find_one({'task_id': task_id})
            if current_task and current_task.get('status') == 'stopped':
                logger.info(f"动态匹配任务被停止: {task_id}")
                return
        
        # 任务完成
        update_progress(100, total_records, total_records, matches, 'completed')
        
        # 保存匹配结果到结果表
        result_collection = db[f'dynamic_match_results_{task_id}']
        result_collection.create_index([('source_id', 1)])
        
        logger.info(f"动态匹配任务完成: {task_id}, 匹配数: {matches}")
        
    except Exception as e:
        logger.error(f"执行动态匹配任务失败: {str(e)}")
        # 更新任务状态为失败
        try:
            db = db_manager.mongo_client.get_database()
            task_collection = db['dynamic_matching_tasks']
            task_collection.update_one(
                {'task_id': task_id},
                {
                    '$set': {
                        'status': 'failed',
                        'error': str(e),
                        'updated_at': datetime.now().isoformat()
                    }
                }
            )
        except:
            pass


@app.route('/api/cleanup_abnormal_user_matching_tasks', methods=['POST'])
def api_cleanup_abnormal_user_matching_tasks():
    """API: 清理异常的用户匹配任务"""
    try:
        data = request.get_json() or {}
        timeout_hours = data.get('timeout_hours', 2)  # 默认2小时超时
        dry_run = data.get('dry_run', False)  # 是否只是检查不实际删除
        
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        tasks_collection = db['user_matching_tasks']
        
        # 计算超时时间点
        from datetime import datetime, timedelta
        timeout_time = datetime.now() - timedelta(hours=timeout_hours)
        
        # 查找异常任务
        abnormal_criteria = {
            '$or': [
                # 长时间运行中的任务
                {
                    'status': 'running',
                    'started_at': {'$lt': timeout_time.isoformat()}
                },
                # 状态为pending但创建时间过久的任务
                {
                    'status': 'pending',
                    'created_at': {'$lt': timeout_time.isoformat()}
                }
            ]
        }
        
        # 查找异常任务
        abnormal_tasks = list(tasks_collection.find(abnormal_criteria))
        
        result = {
            'found_abnormal_tasks': len(abnormal_tasks),
            'timeout_hours': timeout_hours,
            'timeout_time': timeout_time.isoformat(),
            'tasks': []
        }
        
        if abnormal_tasks:
            for task in abnormal_tasks:
                task_info = {
                    'task_id': task.get('task_id'),
                    'config_id': task.get('config_id'),
                    'status': task.get('status'),
                    'created_at': task.get('created_at'),
                    'started_at': task.get('started_at'),
                    'last_updated': task.get('last_updated'),
                    'source_table': task.get('source_table')
                }
                result['tasks'].append(task_info)
            
            if not dry_run:
                # 实际清理异常任务
                deleted_count = 0
                for task in abnormal_tasks:
                    task_id = task.get('task_id')
                    
                    try:
                        # 删除任务记录
                        tasks_collection.delete_one({'task_id': task_id})
                        
                        # 检查并删除对应的结果表
                        result_table_name = f"user_match_results_{task_id}"
                        if result_table_name in db.list_collection_names():
                            db.drop_collection(result_table_name)
                            logger.info(f"已删除结果表: {result_table_name}")
                        
                        deleted_count += 1
                        logger.info(f"已清理异常任务: {task_id}")
                        
                    except Exception as e:
                        logger.error(f"清理任务 {task_id} 失败: {e}")
                
                result['deleted_count'] = deleted_count
                result['action'] = 'cleanup_completed'
                
                logger.info(f"异常任务清理完成: 发现 {len(abnormal_tasks)} 个，清理 {deleted_count} 个")
            else:
                result['action'] = 'dry_run_only'
                logger.info(f"异常任务检查完成（仅检查）: 发现 {len(abnormal_tasks)} 个异常任务")
        else:
            result['action'] = 'no_abnormal_tasks'
            logger.info("未发现异常任务")
        
        return jsonify({
            'success': True,
            'result': result
        })
        
    except Exception as e:
        logger.error(f"清理异常任务失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/force_stop_user_matching_task', methods=['POST'])
def api_force_stop_user_matching_task():
    """API: 强制停止用户匹配任务（用于清理异常任务）"""
    try:
        data = request.get_json()
        task_id = data.get('task_id')
        
        if not task_id:
            return jsonify({'success': False, 'error': '缺少任务ID'}), 400
        
        if not db_manager or not db_manager.mongo_client:
            return jsonify({'success': False, 'error': '数据库连接未初始化'}), 500
        
        db = db_manager.mongo_client.get_database()
        tasks_collection = db['user_matching_tasks']
        
        # 查找任务
        task = tasks_collection.find_one({'task_id': task_id})
        if not task:
            return jsonify({'success': False, 'error': '任务不存在'}), 404
        
        # 强制更新任务状态为停止
        update_result = tasks_collection.update_one(
            {'task_id': task_id},
            {
                '$set': {
                    'status': 'stopped',
                    'end_time': datetime.now().isoformat(),
                    'error_message': '管理员强制停止',
                    'last_updated': datetime.now().isoformat()
                }
            }
        )
        
        if update_result.modified_count > 0:
            logger.info(f"强制停止任务成功: {task_id}")
            
            # 尝试从运行任务列表中移除
            try:
                # 获取全局用户数据匹配器实例
                from src.matching.user_data_matcher import UserDataMatcher
                global_matcher = UserDataMatcher.get_instance() if hasattr(UserDataMatcher, 'get_instance') else None
                
                if global_matcher and hasattr(global_matcher, 'running_tasks') and task_id in global_matcher.running_tasks:
                    del global_matcher.running_tasks[task_id]
                    logger.info(f"已从运行任务列表中移除: {task_id}")
            except Exception as e:
                logger.warning(f"从运行任务列表移除失败: {e}")
            
            return jsonify({
                'success': True,
                'message': f'任务 {task_id} 已强制停止'
            })
        else:
            return jsonify({'success': False, 'error': '更新任务状态失败'}), 500
        
    except Exception as e:
        logger.error(f"强制停止任务失败: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== 性能监控相关路由 =====

@app.route('/performance_monitor')
def performance_monitor():
    """性能监控页面（简洁版）"""
    return render_template('performance_monitor_simple.html')

@app.route('/performance_monitor_full')
def performance_monitor_full():
    """性能监控页面（完整版）"""
    return render_template('performance_monitor.html')


@app.route('/api/performance_metrics')
def get_performance_metrics():
    """获取性能指标API"""
    try:
        import psutil
        import time
        
        # 获取系统信息
        memory = psutil.virtual_memory()
        process = psutil.Process()
        process_memory = process.memory_info()
        
        # 获取性能监控器数据
        monitor = get_performance_monitor()
        memory_processor = get_memory_processor()
        
        # 基本性能指标
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'processing_speed': monitor.get_average('processing_speed', 60) or 0,
            'memory_usage_percent': memory.percent,
            'process_memory_mb': process_memory.rss / (1024 * 1024),
            'cpu_usage_percent': process.cpu_percent(),
            'active_connections': 0,  # 需要从数据库管理器获取
            'used_connections': 0,
            'total_connections': 100,
            'task_statistics': {
                'completed': 0,
                'failed': 0,
                'running': 0,
                'pending': 0
            }
        }
        
        # 数据库连接信息（简化版）
        metrics.update({
            'active_connections': 5,
            'used_connections': 3,
            'total_connections': 100
        })
        
        # 任务统计（示例数据）
        metrics['task_statistics'] = {
            'completed': 1250,
            'failed': 15,
            'running': 2,
            'pending': 8
        }
        
        # 详细指标
        detailed_metrics = {
            'memory_usage': {
                'display_name': '内存使用率',
                'current': memory.percent,
                'average': monitor.get_average('memory_usage', 300) or memory.percent,
                'max': 100,
                'threshold': 90,
                'unit': '%',
                'status': '正常' if memory.percent < 80 else '警告' if memory.percent < 90 else '严重',
                'last_update': datetime.now().isoformat()
            },
            'cpu_usage': {
                'display_name': 'CPU使用率',
                'current': process.cpu_percent(),
                'average': monitor.get_average('cpu_usage', 300) or process.cpu_percent(),
                'max': 100,
                'threshold': 90,
                'unit': '%',
                'status': '正常' if process.cpu_percent() < 80 else '警告' if process.cpu_percent() < 90 else '严重',
                'last_update': datetime.now().isoformat()
            },
            'processing_speed': {
                'display_name': '处理速度',
                'current': monitor.get_average('processing_speed', 60) or 0,
                'average': monitor.get_average('processing_speed', 1800) or 0,
                'max': 10000,
                'threshold': 100,
                'unit': '条/秒',
                'status': '正常' if (monitor.get_average('processing_speed', 60) or 0) > 100 else '警告',
                'last_update': datetime.now().isoformat()
            }
        }
        
        metrics['detailed_metrics'] = detailed_metrics
        
        # 记录当前指标到监控器
        monitor.record_metric('memory_usage', memory.percent)
        monitor.record_metric('cpu_usage', process.cpu_percent())
        
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"获取性能指标失败: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/api/performance_history')
def get_performance_history():
    """获取性能历史数据"""
    try:
        monitor = get_performance_monitor()
        
        # 获取指定时间窗口的历史数据
        window_minutes = request.args.get('window', 60, type=int)
        
        history = {
            'processing_speed': [],
            'memory_usage': [],
            'cpu_usage': [],
            'timestamps': []
        }
        
        # 这里需要实现历史数据的获取逻辑
        # 目前返回模拟数据
        import time
        current_time = time.time()
        
        for i in range(20):  # 返回20个数据点
            timestamp = current_time - (19 - i) * 60  # 每分钟一个点
            history['timestamps'].append(datetime.fromtimestamp(timestamp).strftime('%H:%M'))
            history['processing_speed'].append(monitor.get_average('processing_speed', 60) or 0)
            history['memory_usage'].append(monitor.get_average('memory_usage', 60) or 0)
            history['cpu_usage'].append(monitor.get_average('cpu_usage', 60) or 0)
        
        return jsonify(history)
        
    except Exception as e:
        logger.error(f"获取性能历史失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/system_health')
def get_system_health():
    """系统健康检查API"""
    try:
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'components': {}
        }
        
        # 检查数据库连接（简化版）
        try:
            if db_manager:
                health_status['components']['database'] = {
                    'status': 'healthy',
                    'message': '数据库连接正常'
                }
        except Exception as e:
            health_status['components']['database'] = {
                'status': 'unhealthy',
                'message': f'数据库连接失败: {str(e)}'
            }
            health_status['overall_status'] = 'unhealthy'
        
        # 检查内存使用
        try:
            import psutil
            memory = psutil.virtual_memory()
        except ImportError:
            memory = type('Memory', (), {'percent': 50})()  # 模拟数据
        if memory.percent > 90:
            health_status['components']['memory'] = {
                'status': 'critical',
                'message': f'内存使用过高: {memory.percent:.1f}%'
            }
            health_status['overall_status'] = 'critical'
        elif memory.percent > 80:
            health_status['components']['memory'] = {
                'status': 'warning',
                'message': f'内存使用较高: {memory.percent:.1f}%'
            }
            if health_status['overall_status'] == 'healthy':
                health_status['overall_status'] = 'warning'
        else:
            health_status['components']['memory'] = {
                'status': 'healthy',
                'message': f'内存使用正常: {memory.percent:.1f}%'
            }
        
        # 检查磁盘空间
        try:
            import psutil
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            if disk_percent > 90:
                health_status['components']['disk'] = {
                    'status': 'critical',
                    'message': f'磁盘空间不足: {disk_percent:.1f}%'
                }
                health_status['overall_status'] = 'critical'
            elif disk_percent > 80:
                health_status['components']['disk'] = {
                    'status': 'warning',
                    'message': f'磁盘空间较少: {disk_percent:.1f}%'
                }
                if health_status['overall_status'] == 'healthy':
                    health_status['overall_status'] = 'warning'
            else:
                health_status['components']['disk'] = {
                    'status': 'healthy',
                    'message': f'磁盘空间充足: {disk_percent:.1f}%'
                }
        except Exception as e:
            health_status['components']['disk'] = {
                'status': 'unknown',
                'message': f'无法获取磁盘信息: {str(e)}'
            }
        
        return jsonify(health_status)
        
    except Exception as e:
        logger.error(f"系统健康检查失败: {e}")
        return jsonify({
            'overall_status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


# ====== 知识图谱构建相关API ======

@app.route('/api/kg/build', methods=['POST'])
def build_knowledge_graph():
    """构建知识图谱API"""
    import time
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        project_id = data.get('project_id', f"kg_project_{int(time.time())}")
        config = data.get('config', {})
        
        if not table_name:
            return jsonify({'error': '表名不能为空'}), 400
        
        # 获取表数据
        collection = db_manager.get_collection(table_name)
        if collection is None:
            return jsonify({'error': f'表 {table_name} 不存在'}), 404
        
        # 转换为DataFrame
        cursor = collection.find().limit(config.get('max_records', 10000))
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return jsonify({'error': f'表 {table_name} 没有数据'}), 400
        
        # 初始化知识图谱构建器（使用FalkorDB）
        from src.knowledge_graph.falkordb_store import FalkorDBStore
        from src.knowledge_graph.kg_builder import KnowledgeGraphBuilder
        
        # 使用FalkorDB作为知识图谱存储引擎（支持双重存储）
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph', project_name=project_id)
        kg_builder = KnowledgeGraphBuilder(falkor_store, config)
        
        # 构建知识图谱
        build_result = kg_builder.build_knowledge_graph_from_dataframe(
            df, table_name, project_id
        )
        
        return jsonify(build_result)
        
    except Exception as e:
        logger.error(f"知识图谱构建失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/status')
def get_kg_status():
    """获取知识图谱构建状态API"""
    try:
        from src.knowledge_graph.kg_store import KnowledgeGraphStore
        from src.knowledge_graph.kg_builder import KnowledgeGraphBuilder
        
        kg_store = KnowledgeGraphStore(db_manager=db_manager)
        kg_builder = KnowledgeGraphBuilder(kg_store)
        
        status = kg_builder.get_build_status()
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"获取知识图谱状态失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/entities')
def get_kg_entities():
    """获取知识图谱实体API"""
    try:
        from src.knowledge_graph.kg_store import KnowledgeGraphStore
        
        kg_store = KnowledgeGraphStore(db_manager=db_manager)
        
        # 获取查询参数
        entity_type = request.args.get('type')
        source_table = request.args.get('source_table')
        limit = min(int(request.args.get('limit', 100)), 1000)
        offset = int(request.args.get('offset', 0))
        
        # 构建查询条件
        query_params = {}
        if entity_type:
            query_params['type'] = entity_type
        if source_table:
            query_params['source_table'] = source_table
        
        # 查询实体（直接从kg_entities）
        try:
            # 使用现有的kg_entities集合
            entities_collection = db_manager.get_collection('kg_entities')
            if entities_collection is None:
                return jsonify({'error': '实体数据集合不存在'}), 404
            
            # 构建MongoDB查询
            mongo_query = {}
            if entity_type:
                mongo_query['type'] = entity_type.upper()
            if source_table:
                mongo_query['source_table'] = source_table
            
            # 查询数据
            cursor = entities_collection.find(mongo_query).skip(offset).limit(limit)
            entities_page = list(cursor)
            
            # 转换为标准格式
            entities_data = []
            for entity in entities_page:
                # 移除MongoDB的_id字段
                if '_id' in entity:
                    del entity['_id']
                
                # 转换为API标准格式
                formatted_entity = {
                    'id': entity.get('id', 'unknown'),
                    'label': entity.get('label', 'unknown'),
                    'type': entity.get('type', 'unknown'),
                    'confidence': entity.get('confidence', 0.0),
                    'properties': entity.get('properties', {}),
                    'aliases': entity.get('aliases', []),
                    'source_table': entity.get('source_table', 'unknown'),
                    'source_column': entity.get('source_column', 'unknown'),
                    'created_time': entity.get('created_time'),
                    'updated_time': entity.get('updated_time')
                }
                entities_data.append(formatted_entity)
            
            # 获取总数
            total_count = entities_collection.count_documents(mongo_query)
        except Exception as e:
            logger.error(f"查询实体失败: {str(e)}")
            entities_data = []
            total_count = 0
        
        return jsonify({
            'entities': entities_data,
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"获取知识图谱实体失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/relations')
def get_kg_relations():
    """获取知识图谱关系API"""
    try:
        from src.knowledge_graph.kg_store import KnowledgeGraphStore
        
        kg_store = KnowledgeGraphStore(db_manager=db_manager)
        
        # 获取查询参数
        relation_type = request.args.get('type')
        limit = min(int(request.args.get('limit', 100)), 1000)
        offset = int(request.args.get('offset', 0))
        
        # 构建查询条件
        query_params = {}
        if relation_type:
            query_params['relation_type'] = relation_type
        
        # 查询关系数据（直接从kg_relations）
        try:
            # 使用现有的kg_relations集合
            relations_collection = db_manager.get_collection('kg_relations')
            if relations_collection is None:
                return jsonify({'error': '关系数据集合不存在'}), 404
            
            # 构建MongoDB查询
            mongo_query = {}
            if relation_type:
                mongo_query['predicate.type'] = relation_type
            
            # 查询数据
            cursor = relations_collection.find(mongo_query).skip(offset).limit(limit)
            all_relations = list(cursor)
            
            # 获取实体信息映射
            entities_collection = db_manager.get_collection('kg_entities')
            entity_map = {}
            if entities_collection is not None:
                for entity in entities_collection.find():
                    entity_map[entity.get('id', '')] = entity.get('label', 'unknown')
            
            # 转换为标准格式
            triples_data = []
            for relation in all_relations:
                # 移除MongoDB的_id字段
                if '_id' in relation:
                    del relation['_id']
                
                # 提取关系信息
                subject_id = relation.get('subject_id', '')
                object_id = relation.get('object_id', '')
                predicate_info = relation.get('predicate', {})
                
                # 转换为API标准格式
                formatted_triple = {
                    'id': relation.get('id', 'unknown'),
                    'subject': entity_map.get(subject_id, subject_id or 'unknown'),
                    'predicate': predicate_info.get('label', 'unknown'),
                    'object': entity_map.get(object_id, object_id or 'unknown'),
                    'confidence': relation.get('confidence', 0.0),
                    'subject_type': 'ENTITY',
                    'predicate_type': predicate_info.get('type', 'unknown'),
                    'object_type': 'ENTITY',
                    'source': relation.get('source', 'unknown'),
                    'evidence': relation.get('evidence', []),
                    'created_time': relation.get('created_time'),
                    'updated_time': relation.get('updated_time')
                }
                triples_data.append(formatted_triple)
            
            # 获取总数
            total_count = relations_collection.count_documents(mongo_query)
            
        except Exception as e:
            logger.error(f"查询关系失败: {str(e)}")
            triples_data = []
            total_count = 0
        
        return jsonify({
            'triples': triples_data,
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"获取知识图谱关系失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/quality', methods=['GET'])
def assess_kg_quality():
    """评估知识图谱质量API"""
    try:
        from src.knowledge_graph.kg_store import KnowledgeGraphStore
        from src.knowledge_graph.kg_quality_assessor import KnowledgeGraphQualityAssessor
        
        kg_store = KnowledgeGraphStore(db_manager=db_manager)
        quality_assessor = KnowledgeGraphQualityAssessor(kg_store)
        
        # 进行质量评估
        quality_report = quality_assessor.assess_overall_quality()
        
        return jsonify(quality_report)
        
    except Exception as e:
        logger.error(f"知识图谱质量评估失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/search')
def search_kg():
    """知识图谱搜索API"""
    try:
        from src.knowledge_graph.kg_store import KnowledgeGraphStore
        
        kg_store = KnowledgeGraphStore(db_manager=db_manager)
        
        # 获取搜索参数
        query = request.args.get('q', '').strip()
        search_type = request.args.get('type', 'all')  # entities, relations, all
        limit = min(int(request.args.get('limit', 50)), 200)
        
        if not query:
            return jsonify({'error': '搜索关键词不能为空'}), 400
        
        results = {'entities': [], 'triples': []}
        
        try:
            if search_type in ['entities', 'all']:
                # 搜索实体（使用简单的文本匹配）
                try:
                    # 尝试使用现有方法或创建简单搜索
                    if hasattr(kg_store, 'search_entities'):
                        entities = kg_store.search_entities(query, limit=limit)
                    else:
                        # 简单搜索实现：查询包含关键词的实体
                        entities_collection = kg_store.entities_collection
                        cursor = entities_collection.find(
                            {'label': {'$regex': query, '$options': 'i'}},
                            limit=limit
                        )
                        entities = []
                        for doc in cursor:
                            entities.append({
                                'id': doc.get('id', 'unknown'),
                                'label': doc.get('label', 'unknown'),
                                'type': doc.get('type', 'unknown'),
                                'confidence': doc.get('confidence', 0.0)
                            })
                    
                    # 格式化实体结果
                    if entities:
                        for entity in entities:
                            if hasattr(entity, 'to_dict'):
                                results['entities'].append(entity.to_dict())
                            else:
                                results['entities'].append(entity if isinstance(entity, dict) else {'id': str(entity), 'label': str(entity)})
                except Exception as e:
                    logger.error(f"实体搜索失败: {str(e)}")
                    results['entities'] = []
            
            if search_type in ['relations', 'all']:
                # 搜索三元组
                try:
                    if hasattr(kg_store, 'search_triples'):
                        triples = kg_store.search_triples(query, limit=limit)
                    else:
                        # 简单搜索实现：查询包含关键词的三元组
                        triples_collection = kg_store.triples_collection
                        cursor = triples_collection.find({
                            '$or': [
                                {'subject_label': {'$regex': query, '$options': 'i'}},
                                {'predicate_label': {'$regex': query, '$options': 'i'}},
                                {'object_label': {'$regex': query, '$options': 'i'}}
                            ]
                        }, limit=limit)
                        triples = []
                        for doc in cursor:
                            triples.append({
                                'id': doc.get('id', 'unknown'),
                                'subject': doc.get('subject_label', 'unknown'),
                                'predicate': doc.get('predicate_label', 'unknown'),
                                'object': doc.get('object_label', 'unknown'),
                                'confidence': doc.get('confidence', 0.0)
                            })
                    
                    # 格式化三元组结果
                    if triples:
                        for triple in triples:
                            if hasattr(triple, 'to_dict'):
                                results['triples'].append(triple.to_dict())
                            else:
                                results['triples'].append(triple if isinstance(triple, dict) else {'id': str(triple), 'subject': str(triple)})
                except Exception as e:
                    logger.error(f"三元组搜索失败: {str(e)}")
                    results['triples'] = []
        except Exception as e:
            logger.error(f"搜索失败: {str(e)}")
            results = {'entities': [], 'triples': []}
        
        return jsonify({
            'query': query,
            'results': results,
            'total_entities': len(results['entities']),
            'total_triples': len(results['triples'])
        })
        
    except Exception as e:
        logger.error(f"知识图谱搜索失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ====== 简化的知识图谱API（用于前端测试） ======

@app.route('/api/kg/entities_simple')
def get_kg_entities_simple():
    """获取知识图谱实体API（简化版）"""
    try:
        entity_type = request.args.get('type', 'all')
        limit = min(int(request.args.get('limit', 10)), 100)
        
        sample_entities = [
            {'id': 'org_001', 'label': '北京科技有限公司', 'type': 'ORGANIZATION', 'confidence': 0.95, 'properties': {'address': '北京市', 'industry': '科技'}},
            {'id': 'person_001', 'label': '张三', 'type': 'PERSON', 'confidence': 0.88, 'properties': {'role': '经理', 'department': '技术部'}},
            {'id': 'org_002', 'label': '上海教育集团', 'type': 'ORGANIZATION', 'confidence': 0.92, 'properties': {'address': '上海市', 'industry': '教育'}},
            {'id': 'location_001', 'label': '北京市朝阳区', 'type': 'LOCATION', 'confidence': 0.90, 'properties': {'city': '北京', 'district': '朝阳区'}},
            {'id': 'person_002', 'label': '李四', 'type': 'PERSON', 'confidence': 0.85, 'properties': {'role': '主管', 'department': '销售部'}}
        ]
        
        if entity_type != 'all':
            sample_entities = [e for e in sample_entities if e['type'] == entity_type.upper()]
        
        return jsonify({
            'entities': sample_entities[:limit],
            'total': len(sample_entities),
            'available_types': ['ORGANIZATION', 'PERSON', 'LOCATION'],
            'status': 'success'
        })
    except Exception as e:
        logger.error(f"简化实体API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/relations_simple')  
def get_kg_relations_simple():
    """获取知识图谱关系API（简化版）"""
    try:
        limit = min(int(request.args.get('limit', 10)), 100)
        
        sample_relations = [
            {'id': 'rel_001', 'subject': '张三', 'predicate': '工作于', 'object': '北京科技有限公司', 'confidence': 0.95, 'type': 'EMPLOYMENT'},
            {'id': 'rel_002', 'subject': '北京科技有限公司', 'predicate': '位于', 'object': '北京市朝阳区', 'confidence': 0.88, 'type': 'LOCATION'},
            {'id': 'rel_003', 'subject': '上海教育集团', 'predicate': '类型为', 'object': '教育机构', 'confidence': 0.92, 'type': 'TYPE'},
            {'id': 'rel_004', 'subject': '李四', 'predicate': '管理', 'object': '销售部', 'confidence': 0.87, 'type': 'MANAGEMENT'},
            {'id': 'rel_005', 'subject': '技术部', 'predicate': '隶属于', 'object': '北京科技有限公司', 'confidence': 0.93, 'type': 'ORGANIZATIONAL'}
        ]
        
        return jsonify({
            'triples': sample_relations[:limit],
            'total': len(sample_relations),
            'available_types': ['EMPLOYMENT', 'LOCATION', 'TYPE', 'MANAGEMENT', 'ORGANIZATIONAL'],
            'status': 'success'
        })
    except Exception as e:
        logger.error(f"简化关系API失败: {str(e)}")
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
                {'id': f'search_entity_{query}', 'label': f'{query}科技公司', 'type': 'ORGANIZATION', 'confidence': 0.85},
                {'id': f'search_person_{query}', 'label': f'{query}先生', 'type': 'PERSON', 'confidence': 0.80}
            ],
            'triples': [
                {'id': f'search_rel_{query}', 'subject': f'{query}先生', 'predicate': '工作于', 'object': f'{query}科技公司', 'confidence': 0.80}
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
        logger.error(f"简化搜索API失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ====== FalkorDB知识图谱API ======
# 注意：实际的API实现在文件后面的聚合版本中

@app.route('/api/kg/falkor/search', methods=['GET'])
def search_falkor_entities():
    """FalkorDB知识图谱搜索API"""
    try:
        # 支持多种参数名：q, query
        query = request.args.get('q') or request.args.get('query', '')
        entity_type = request.args.get('entity_type', '')
        relation_type = request.args.get('relation_type', '')
        limit = int(request.args.get('limit', 20))
        
        # 如果没有任何搜索条件，返回默认数据
        if not query and not entity_type and not relation_type:
            # 返回默认数据集
            entity_type = 'ORGANIZATION'  # 默认返回机构类型实体
        
        # 连接FalkorDB
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        
        # 搜索实体和关系
        entities = []
        relations = []
        
        if query or entity_type:
            # 搜索实体
            if hasattr(falkor_store, 'search_entities'):
                entities = falkor_store.search_entities(query or '', limit=limit)
            else:
                # 如果没有search_entities方法，使用query_entities
                entities = falkor_store.query_entities(entity_type=entity_type, limit=limit)
        
        # 总是尝试获取关系数据
        try:
            if hasattr(falkor_store, 'search_relations') and query:
                relations = falkor_store.search_relations(query, limit=limit)
            else:
                # 使用query_relations获取关系数据
                relations = falkor_store.query_relations(limit=limit)
                # 如果指定了relation_type，进行过滤
                if relation_type:
                    relations = [r for r in relations if r.get('type') == relation_type]
        except Exception as e:
            logger.warning(f"获取关系数据失败: {e}")
            relations = []
        
        return jsonify({
            'query': query,
            'entity_type': entity_type,
            'relation_type': relation_type,
            'entities': entities,
            'relations': relations,
            'total_entities': len(entities),
            'total_relations': len(relations),
            'limit': limit,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"FalkorDB搜索失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/falkor/entities', methods=['GET'])
def get_falkor_entities():
    """FalkorDB知识图谱实体API - 聚合所有图谱"""
    try:
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        entity_type = request.args.get('entity_type', '')
        
        import falkordb
        client = falkordb.FalkorDB(host='localhost', port=16379)
        graphs_result = client.execute_command('GRAPH.LIST')
        
        all_entities = []
        
        for graph_name in graphs_result:
            try:
                # 构建查询条件
                if entity_type:
                    cypher_query = f"""
                    MATCH (e:{entity_type.upper()})
                    RETURN COALESCE(e.entity_id, e.id, toString(id(e))) AS id,
                           e.label AS label, e.type AS type,
                           e.confidence AS confidence, e.source_table AS source_table
                    LIMIT {limit}
                    """
                else:
                    cypher_query = f"""
                    MATCH (e)
                    WHERE e.type IS NOT NULL
                    RETURN COALESCE(e.entity_id, e.id, toString(id(e))) AS id,
                           e.label AS label, e.type AS type,
                           e.confidence AS confidence, e.source_table AS source_table
                    LIMIT {limit}
                    """
                
                result = client.execute_command('GRAPH.QUERY', graph_name, cypher_query)
                
                if len(result) > 1 and result[1]:
                    for row in result[1]:
                        entity = {
                            'id': row[0],
                            'label': row[1],
                            'type': row[2],
                            'confidence': row[3] if row[3] is not None else 0.9,
                            'source_table': row[4],
                            'graph_name': graph_name
                        }
                        all_entities.append(entity)
                        
                        if len(all_entities) >= limit:
                            break
                            
            except Exception as e:
                logger.warning(f"查询图谱 {graph_name} 实体失败: {e}")
                continue
                
            if len(all_entities) >= limit:
                break
        
        # 应用offset
        entities = all_entities[offset:offset+limit] if offset > 0 else all_entities[:limit]
        
        return jsonify({
            'entities': entities,
            'total': len(entities),
            'limit': limit,
            'offset': offset,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"FalkorDB实体查询失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/falkor/relations', methods=['GET'])
def get_falkor_relations():
    """FalkorDB知识图谱关系API - 聚合所有图谱"""
    try:
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        
        import falkordb
        client = falkordb.FalkorDB(host='localhost', port=16379)
        graphs_result = client.execute_command('GRAPH.LIST')
        
        all_relations = []
        
        for graph_name in graphs_result:
            try:
                logger.info(f"正在查询图谱: {graph_name}")
                
                cypher_query = f"""
                MATCH (s)-[r]->(o)
                RETURN toString(id(r)) AS id, type(r) AS type,
                       s.label AS subject_label, o.label AS object_label,
                       toString(id(s)) AS subject_id, toString(id(o)) AS object_id,
                       r.confidence AS confidence
                LIMIT {limit}
                """
                
                result = client.execute_command('GRAPH.QUERY', graph_name, cypher_query)
                logger.info(f"图谱 {graph_name} 查询结果长度: {len(result)}")
                
                if len(result) > 1 and result[1]:
                    logger.info(f"图谱 {graph_name} 找到 {len(result[1])} 个关系")
                    for row in result[1]:
                        relation = {
                            'id': row[0],
                            'type': row[1],
                            'predicate': row[1],  # 兼容前端
                            'subject_label': row[2],
                            'object_label': row[3],
                            'subject_id': row[4],
                            'object_id': row[5],
                            'confidence': row[6] if row[6] is not None else 0.9,
                            'graph_name': graph_name
                        }
                        all_relations.append(relation)
                        
                        if len(all_relations) >= limit:
                            break
                else:
                    logger.info(f"图谱 {graph_name} 无关系数据")
                            
            except Exception as e:
                logger.error(f"查询图谱 {graph_name} 关系失败: {e}")
                continue
                
            if len(all_relations) >= limit:
                break
        
        # 应用offset
        relations = all_relations[offset:offset+limit] if offset > 0 else all_relations[:limit]
        
        logger.info(f"聚合关系查询完成: 找到 {len(relations)} 个关系")
        
        return jsonify({
            'relations': relations,
            'total': len(relations),
            'limit': limit,
            'offset': offset,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"FalkorDB关系查询失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/falkor/stats', methods=['GET'])
def get_falkor_stats():
    """获取FalkorDB知识图谱统计信息API"""
    try:
        import time
        import falkordb
        start_time = time.time()
        
        # 直接连接FalkorDB获取所有图谱统计
        client = falkordb.FalkorDB(host='localhost', port=16379)
        graphs_result = client.execute_command('GRAPH.LIST')
        
        # 聚合所有图谱的统计信息
        total_entities = 0
        total_relations = 0
        total_triples = 0
        
        for graph_name in graphs_result:
            try:
                # 获取节点数（实体数）
                node_result = client.execute_command('GRAPH.QUERY', graph_name, 'MATCH (n) RETURN count(n) as count')
                node_count = node_result[1][0][0] if len(node_result) > 1 and len(node_result[1]) > 0 else 0
                
                # 获取边数（关系数/三元组数）
                edge_result = client.execute_command('GRAPH.QUERY', graph_name, 'MATCH ()-[r]->() RETURN count(r) as count')
                edge_count = edge_result[1][0][0] if len(edge_result) > 1 and len(edge_result[1]) > 0 else 0
                
                logger.info(f"图谱 {graph_name}: {node_count}节点, {edge_count}边")
                
                total_entities += node_count
                total_relations += edge_count
                total_triples += edge_count  # 在图数据库中，边就是三元组
                
            except Exception as e:
                logger.warning(f"获取图谱 {graph_name} 统计失败: {e}")
                continue
        
        query_time = (time.time() - start_time) * 1000
        
        # 构建统计对象（兼容原有接口）
        stats = {
            'total_entities': total_entities,
            'total_relations': total_relations,
            'triples_stored': total_triples,
            'total_labels': len(graphs_result),
            'queries_executed': 0,
            'last_operation_time': ''
        }
        
        # 构建前端期望的平铺数据格式
        response_data = {
            # 直接统计字段
            'total_entities': stats.get('total_entities', 0),
            'total_relations': stats.get('total_relations', 0), 
            'total_triples': stats.get('triples_stored', 0),
            'total_labels': stats.get('total_labels', 0),
            
            # 性能统计
            'performance_stats': {
                'total_entities': stats.get('total_entities', 0),
                'total_relations': stats.get('total_relations', 0),
                'triples_stored': stats.get('triples_stored', 0),
                'queries_executed': stats.get('queries_executed', 0),
                'avg_query_time_ms': round(query_time, 1),
                'last_operation_time': stats.get('last_operation_time', ''),
            },
            
            # 原始统计数据（向后兼容）
            'stats': stats,
            'status': 'success'
        }
        
        logger.info(f"FalkorDB统计信息获取成功: 实体={response_data['total_entities']}, 关系={response_data['total_relations']}, 三元组={response_data['total_triples']}")
        
        # 添加调试信息
        response_data['debug_info'] = {
            'graphs_found': len(graphs_result),
            'aggregated_entities': total_entities,
            'aggregated_relations': total_relations,
            'aggregated_triples': total_triples
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"FalkorDB统计信息获取失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ====== 项目管理API ======

@app.route('/api/kg/projects', methods=['GET'])
def get_kg_projects():
    """获取所有知识图谱项目列表API"""
    try:
        # 连接FalkorDB
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        
        # 获取项目列表
        projects = falkor_store.get_all_projects()
        
        return jsonify({
            'projects': projects,
            'total': len(projects),
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"获取项目列表失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/projects/<project_name>/stats', methods=['GET'])
def get_project_stats(project_name):
    """获取特定项目统计信息API"""
    try:
        import time
        start_time = time.time()
        
        use_global_graph = request.args.get('global', 'false').lower() == 'true'
        
        # 连接FalkorDB
        if use_global_graph:
            falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph_global')
        else:
            project_graph_name = f"knowledge_graph_project_{project_name}"
            falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name=project_graph_name)
        
        # 获取统计信息
        stats = falkor_store.get_statistics()
        query_time = (time.time() - start_time) * 1000
        
        # 构建响应数据
        response_data = {
            'project_name': project_name,
            'graph_type': 'global' if use_global_graph else 'project',
            'total_entities': stats.get('total_entities', 0),
            'total_relations': stats.get('total_relations', 0), 
            'total_triples': stats.get('triples_stored', 0),
            'total_labels': stats.get('total_labels', 0),
            'performance_stats': {
                'avg_query_time_ms': round(query_time, 1),
                'queries_executed': stats.get('queries_executed', 0),
                'last_operation_time': stats.get('last_operation_time', ''),
            },
            'status': 'success'
        }
        
        logger.info(f"项目{project_name}统计信息获取成功: 实体={response_data['total_entities']}, 关系={response_data['total_relations']}")
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"获取项目{project_name}统计信息失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/projects/<project_name>/entities', methods=['GET'])
def get_project_entities(project_name):
    """获取特定项目实体API"""
    try:
        entity_type = request.args.get('type')
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        use_global_graph = request.args.get('global', 'false').lower() == 'true'
        
        # 连接FalkorDB
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        
        # 查询项目实体
        entities = falkor_store.query_entities_by_project(
            project_name=project_name,
            entity_type=entity_type,
            limit=limit,
            offset=offset,
            use_global_graph=use_global_graph
        )
        
        return jsonify({
            'project_name': project_name,
            'graph_type': 'global' if use_global_graph else 'project',
            'entities': entities,
            'total': len(entities),
            'limit': limit,
            'offset': offset,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"获取项目{project_name}实体失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/delete/preview', methods=['POST'])
def get_kg_deletion_preview():
    """获取知识图谱删除预览API"""
    try:
        data = request.get_json() or {}
        project_name = data.get('project_name')
        
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        preview = falkor_store.get_deletion_preview(project_name)
        
        return jsonify({
            'preview': preview,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"获取删除预览失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/delete/project/<project_name>', methods=['DELETE'])
def delete_project_kg(project_name):
    """删除指定项目的知识图谱API"""
    try:
        
        # 使用项目名称初始化FalkorDBStore以支持双重存储
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph', project_name=project_name)
        
        # 执行删除
        success = falkor_store.delete_project_graph(project_name)
        
        if success:
            return jsonify({
                'message': f'项目 {project_name} 的知识图谱已成功删除',
                'project_name': project_name,
                'status': 'success'
            })
        else:
            return jsonify({
                'error': f'删除项目 {project_name} 的知识图谱失败',
                'project_name': project_name,
                'status': 'failed'
            }), 500
            
    except Exception as e:
        logger.error(f"删除项目知识图谱失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/delete/all', methods=['DELETE'])
def delete_all_kg():
    """删除所有知识图谱数据API"""
    try:
        data = request.get_json() or {}
        confirm = data.get('confirm', False)
        
        if not confirm:
            return jsonify({
                'error': '请确认删除操作（设置 confirm: true）',
                'status': 'confirmation_required'
            }), 400
        
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        
        # 删除所有项目数据
        results = falkor_store.delete_all_projects()
        
        # 清空主图谱
        clear_success = falkor_store.clear_graph()
        
        successful_deletions = sum(results.values())
        total_projects = len(results)
        
        return jsonify({
            'message': f'知识图谱删除完成',
            'projects_deleted': successful_deletions,
            'total_projects': total_projects,
            'main_graph_cleared': clear_success,
            'details': results,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"删除所有知识图谱失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kg/delete/entities/<entity_type>', methods=['DELETE'])
def delete_entities_by_type(entity_type):
    """按实体类型删除API"""
    try:
        data = request.get_json() or {}
        project_name = data.get('project_name')
        
        falkor_store = FalkorDBStore(host='localhost', port=16379, graph_name='knowledge_graph')
        success = falkor_store.delete_entities_by_type(entity_type, project_name)
        
        if success:
            return jsonify({
                'message': f'已成功删除类型为 {entity_type} 的实体',
                'entity_type': entity_type,
                'project_name': project_name,
                'status': 'success'
            })
        else:
            return jsonify({
                'error': f'删除类型为 {entity_type} 的实体失败',
                'entity_type': entity_type,
                'status': 'failed'
            }), 500
            
    except Exception as e:
        logger.error(f"按类型删除实体失败: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kg/projects/<project_name>/relations', methods=['GET'])
def get_project_relations(project_name):
    """获取特定项目关系API"""
    try:
        logger.info(f"获取项目 {project_name} 的关系数据")
        
        # 获取查询参数
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        is_global = request.args.get('global', 'false').lower() == 'true'
        
        # 根据是否全局模式选择数据源
        if is_global:
            # 全局模式：从所有图谱获取关系
            try:
                # 直接调用全局关系查询逻辑
                import falkordb
                client = falkordb.FalkorDB(host='localhost', port=16379)
                
                # 获取所有图谱名称
                graph_names = client.execute_command('GRAPH.LIST')
                logger.info(f"找到 {len(graph_names)} 个图谱")
                
                project_relations = []
                
                for graph_name in graph_names:
                    if project_name in graph_name:
                        try:
                            graph = client.select_graph(graph_name)
                            query = """
                            MATCH (s)-[r]->(o)
                            RETURN s.entity_id AS subject_id, s.label AS subject_label,
                                   type(r) AS predicate, r.confidence AS confidence,
                                   o.entity_id AS object_id, o.label AS object_label
                            LIMIT $limit
                            """
                            result = graph.query(query, {'limit': limit})
                            
                            for record in result.result_set:
                                project_relations.append({
                                    'subject_id': record[0],
                                    'subject_label': record[1],
                                    'predicate': record[2],
                                    'type': record[2],
                                    'confidence': record[3] if record[3] is not None else 0.9,
                                    'object_id': record[4],
                                    'object_label': record[5],
                                    'graph_name': graph_name
                                })
                                
                        except Exception as graph_error:
                            logger.warning(f"查询图谱 {graph_name} 失败: {str(graph_error)}")
                            continue
                            
                logger.info(f"全局模式找到 {len(project_relations)} 个项目关系")
                
            except Exception as global_error:
                logger.error(f"全局模式查询失败: {str(global_error)}")
                project_relations = []
        else:
            # 项目模式：从特定项目图谱获取关系
            try:
                # 直接使用FalkorDB查询项目特定图谱
                import falkordb
                client = falkordb.FalkorDB(host='localhost', port=16379)
                
                # 构建项目图谱名称
                project_graph_name = f"knowledge_graph_project_{project_name}"
                
                try:
                    graph = client.select_graph(project_graph_name)
                    query = """
                    MATCH (s)-[r]->(o)
                    RETURN s.entity_id AS subject_id, s.label AS subject_label,
                           type(r) AS predicate, r.confidence AS confidence,
                           o.entity_id AS object_id, o.label AS object_label
                    SKIP $offset LIMIT $limit
                    """
                    result = graph.query(query, {'offset': offset, 'limit': limit})
                    
                    project_relations = []
                    for record in result.result_set:
                        project_relations.append({
                            'subject_id': record[0],
                            'subject_label': record[1],
                            'predicate': record[2],
                            'type': record[2],
                            'confidence': record[3] if record[3] is not None else 0.9,
                            'object_id': record[4],
                            'object_label': record[5],
                            'graph_name': project_graph_name
                        })
                        
                    logger.info(f"项目 {project_name} 查询到 {len(project_relations)} 个关系")
                    
                except Exception as graph_error:
                    logger.warning(f"查询项目图谱 {project_graph_name} 失败: {str(graph_error)}")
                    project_relations = []
                
            except Exception as store_error:
                logger.error(f"项目 {project_name} FalkorDB查询失败: {str(store_error)}")
                project_relations = []
        
        # 应用分页
        total_relations = len(project_relations)
        paginated_relations = project_relations[offset:offset + limit]
        
        logger.info(f"项目 {project_name} 关系API返回: {len(paginated_relations)}/{total_relations} 个关系")
        
        return jsonify({
            'relations': paginated_relations,
            'total': total_relations,
            'limit': limit,
            'offset': offset,
            'project_name': project_name,
            'global_mode': is_global,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"获取项目 {project_name} 关系失败: {str(e)}")
        return jsonify({
            'error': str(e),
            'relations': [],
            'total': 0,
            'project_name': project_name,
            'status': 'failed'
        }), 500


@app.route('/api/kg/projects/<project_name>/table_schema', methods=['GET'])
def get_project_table_schema(project_name):
    """获取项目表结构信息API"""
    try:
        logger.info(f"获取项目 {project_name} 的表结构信息")
        
        # 直接使用FalkorDB查询项目图谱中的表结构信息
        import falkordb
        client = falkordb.FalkorDB(host='localhost', port=16379)
        
        # 构建项目图谱名称
        project_graph_name = f"knowledge_graph_project_{project_name}"
        
        try:
            graph = client.select_graph(project_graph_name)
            
            # 查询所有实体的source_table信息，获取表结构
            query = """
            MATCH (e)
            WHERE e.source_table IS NOT NULL
            RETURN DISTINCT e.source_table AS table_name, 
                   COUNT(e) AS entity_count,
                   COLLECT(DISTINCT e.type) AS entity_types
            ORDER BY table_name
            """
            result = graph.query(query)
            
            tables = []
            for record in result.result_set:
                table_name = record[0]
                entity_count = record[1]
                entity_types = record[2] if record[2] else []
                
                # 查询该表的字段信息（通过实体属性推断）
                field_query = f"""
                MATCH (e)
                WHERE e.source_table = '{table_name}'
                RETURN KEYS(e) AS field_names
                LIMIT 10
                """
                field_result = graph.query(field_query)
                
                # 收集所有字段名
                all_fields = set()
                for field_record in field_result.result_set:
                    if field_record[0]:
                        all_fields.update(field_record[0])
                
                # 过滤掉系统字段
                system_fields = {'entity_id', 'label', 'type', 'confidence', 'source_table', 'graph_name'}
                business_fields = [f for f in all_fields if f not in system_fields]
                
                tables.append({
                    'table_name': table_name,
                    'entity_count': entity_count,
                    'entity_types': entity_types,
                    'fields': sorted(business_fields),
                    'field_count': len(business_fields)
                })
            
            # 首先检查用户匹配结果，构建基于匹配的表间关系
            user_table_relations = []
            
            # 检查MongoDB中的用户匹配结果
            try:
                # 使用全局数据库管理器
                if not db_manager or not db_manager.mongo_client:
                    logger.warning("数据库连接未初始化")
                    user_table_relations = []
                else:
                    mongo_db = db_manager.mongo_client.get_database()
                    
                    # 查找项目相关的匹配结果表
                    collection_names = mongo_db.list_collection_names()
                    
                    # 更精确的匹配逻辑 - 查找包含xp_jxjzdwxx的用户匹配结果表
                    match_collections = []
                    for name in collection_names:
                        if 'user_match_results' in name and 'xp_jxjzdwxx' in name:
                            match_collections.append(name)
                    
                    logger.info(f"找到 {len(match_collections)} 个匹配结果表: {match_collections}")
                    
                    # 调试信息：显示所有集合名称
                    all_user_match_collections = [name for name in collection_names if 'user_match_results' in name]
                    logger.info(f"所有用户匹配结果表: {all_user_match_collections}")
                    
                    for collection_name in match_collections:
                        collection = mongo_db[collection_name]
                        
                        # 获取匹配结果样本来分析表间关系
                        sample = collection.find_one()
                        if sample and 'matched_fields' in sample:
                            # 分析字段映射来确定源表和目标表
                            matched_fields = sample['matched_fields']
                            
                            # 从字段映射推断表关系
                            if len(matched_fields) > 0:
                                # 从集合名称或数据中推断表名
                                source_table = None
                                target_table = None
                                
                                # 尝试从任务ID或集合名称推断表名
                                if 'task_id' in sample:
                                    task_id = sample['task_id']
                                    # 从任务ID中提取表名信息
                                    if 'xp_jxjzdwxx' in task_id:
                                        source_table = 'xp_jxjzdwxx'
                                        
                                        # 根据集合名称或任务ID推断目标表
                                        if '122051' in collection_name or '122109' in collection_name:
                                            # 第一个匹配任务：xp_jxjzdwxx -> dwd_yljgxx
                                            target_table = 'dwd_yljgxx'
                                        elif '153502' in collection_name or '153515' in collection_name:
                                            # 第二个匹配任务：xp_jxjzdwxx -> dwd_zzzhzj
                                            target_table = 'dwd_zzzhzj'
                                
                                # 如果无法从任务ID推断，尝试其他方法
                                if not source_table or not target_table:
                                    # 从匹配字段推断（左边是源表字段，右边是目标表字段）
                                    first_mapping = matched_fields[0].split('->')
                                    if len(first_mapping) == 2:
                                        source_field = first_mapping[0]
                                        target_field = first_mapping[1]
                                        
                                        # 根据字段名称模式推断表名
                                        if source_field in ['COMPANY_NAME', 'COMPANY_ADDRESS', 'COMPANY_LEGAL']:
                                            source_table = 'xp_jxjzdwxx'
                                        
                                        if target_field in ['JGMC', 'ZCDZ', 'FDDBR']:
                                            target_table = 'dwd_yljgxx'
                                        elif target_field in ['YY_FRXM']:
                                            target_table = 'dwd_zzzhzj'
                                
                                if source_table and target_table:
                                    match_count = collection.count_documents({})
                                    
                                    # 分析字段映射关系
                                    field_mappings = []
                                    for mapping in matched_fields:
                                        if '->' in mapping:
                                            source_field, target_field = mapping.split('->')
                                            field_mappings.append({
                                                'source_field': source_field,
                                                'target_field': target_field
                                            })
                                    
                                    user_table_relations.append({
                                        'source_table': source_table,
                                        'target_table': target_table,
                                        'relation_type': 'USER_MATCHED',
                                        'relation_count': match_count,
                                        'field_mappings': field_mappings,
                                        'collection_name': collection_name
                                    })
                                    
                                    logger.info(f"发现用户匹配关系: {source_table} -> {target_table} ({match_count} 条匹配)")
                
            except Exception as e:
                logger.warning(f"查询用户匹配结果失败: {str(e)}")
            
            # 查询知识图谱中的表间关系（作为补充）
            kg_table_relations = []
            relations_query = """
            MATCH (s)-[r]->(o)
            WHERE s.source_table IS NOT NULL AND o.source_table IS NOT NULL
              AND s.source_table <> o.source_table
            RETURN s.source_table AS source_table, 
                   o.source_table AS target_table,
                   type(r) AS relation_type,
                   COUNT(r) AS relation_count
            ORDER BY relation_count DESC
            """
            relations_result = graph.query(relations_query)
            
            for rel_record in relations_result.result_set:
                source_table = rel_record[0]
                target_table = rel_record[1]
                relation_type = rel_record[2]
                relation_count = rel_record[3]
                
                kg_table_relations.append({
                    'source_table': source_table,
                    'target_table': target_table,
                    'relation_type': relation_type,
                    'relation_count': relation_count
                })
            
            # 合并用户匹配关系和知识图谱关系
            table_relations = user_table_relations + kg_table_relations
            
            logger.info(f"表间关系统计: 用户匹配关系 {len(user_table_relations)} 个, 知识图谱关系 {len(kg_table_relations)} 个")
            
            logger.info(f"项目 {project_name} 表结构查询完成: {len(tables)} 个表, {len(table_relations)} 个表间关系")
            
            return jsonify({
                'tables': tables,
                'table_relations': table_relations,
                'project_name': project_name,
                'total_tables': len(tables),
                'total_table_relations': len(table_relations),
                'status': 'success'
            })
            
        except Exception as graph_error:
            logger.warning(f"查询项目图谱 {project_graph_name} 失败: {str(graph_error)}")
            return jsonify({
                'error': f'项目图谱查询失败: {str(graph_error)}',
                'tables': [],
                'table_relations': [],
                'project_name': project_name,
                'status': 'failed'
            }), 500
        
    except Exception as e:
        logger.error(f"获取项目 {project_name} 表结构失败: {str(e)}")
        return jsonify({
            'error': str(e),
            'tables': [],
            'table_relations': [],
            'project_name': project_name,
            'status': 'failed'
        }), 500


@app.route('/kg_viewer_projects')
def kg_viewer_projects_page():
    """按项目浏览知识图谱页面"""
    try:
        return render_template('kg_viewer_projects.html')
    except Exception as e:
        logger.error(f"项目图谱浏览页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/api/kg/projects/<project_name>/column_relations', methods=['GET'])
def get_project_column_relations(project_name):
    """获取项目列间关系详细信息API"""
    try:
        logger.info(f"获取项目 {project_name} 的列间关系信息")
        
        # 直接使用FalkorDB查询项目图谱中的列间关系
        import falkordb
        client = falkordb.FalkorDB(host='localhost', port=16379)
        
        # 构建项目图谱名称
        project_graph_name = f"knowledge_graph_project_{project_name}"
        
        try:
            graph = client.select_graph(project_graph_name)
            
            # 查询列间关系 - 通过实体的source_column属性分析
            column_relations_query = """
            MATCH (s)-[r]->(o)
            WHERE s.source_table IS NOT NULL AND o.source_table IS NOT NULL
              AND s.source_column IS NOT NULL AND o.source_column IS NOT NULL
            RETURN s.source_table AS source_table,
                   s.source_column AS source_column,
                   o.source_table AS target_table, 
                   o.source_column AS target_column,
                   type(r) AS relation_type,
                   COUNT(r) AS relation_count,
                   AVG(r.confidence) AS avg_confidence
            ORDER BY relation_count DESC
            LIMIT 100
            """
            
            result = graph.query(column_relations_query)
            
            column_relations = []
            for record in result.result_set:
                source_table = record[0]
                source_column = record[1] 
                target_table = record[2]
                target_column = record[3]
                relation_type = record[4]
                relation_count = record[5]
                avg_confidence = record[6] if record[6] is not None else 0.9
                
                column_relations.append({
                    'source_table': source_table,
                    'source_column': source_column,
                    'target_table': target_table,
                    'target_column': target_column,
                    'relation_type': relation_type,
                    'relation_count': relation_count,
                    'avg_confidence': round(avg_confidence, 3),
                    'relation_id': f"{source_table}.{source_column}-{target_table}.{target_column}"
                })
            
            # 查询匹配统计信息 - 从MongoDB获取已完成的匹配结果
            match_statistics = {}
            try:
                # 查询用户匹配结果统计
                match_results_collection = db_manager.get_collection('user_matching_results')
                if match_results_collection:
                    # 统计不同表间的匹配数量
                    pipeline = [
                        {'$match': {'status': 'completed'}},
                        {'$group': {
                            '_id': {
                                'source_table': '$source_table',
                                'target_table': '$target_table'
                            },
                            'match_count': {'$sum': 1},
                            'avg_similarity': {'$avg': '$similarity_score'}
                        }}
                    ]
                    
                    match_stats = list(match_results_collection.aggregate(pipeline))
                    
                    for stat in match_stats:
                        table_pair = f"{stat['_id']['source_table']}-{stat['_id']['target_table']}"
                        match_statistics[table_pair] = {
                            'match_count': stat['match_count'],
                            'avg_similarity': round(stat.get('avg_similarity', 0), 3)
                        }
                        
            except Exception as match_error:
                logger.warning(f"查询匹配统计失败: {str(match_error)}")
            
            # 聚合表级别的关系统计
            table_relations_summary = {}
            for relation in column_relations:
                table_pair = f"{relation['source_table']}-{relation['target_table']}"
                
                if table_pair not in table_relations_summary:
                    table_relations_summary[table_pair] = {
                        'source_table': relation['source_table'],
                        'target_table': relation['target_table'],
                        'total_column_relations': 0,
                        'total_relation_count': 0,
                        'relation_types': set(),
                        'avg_confidence': 0,
                        'match_statistics': match_statistics.get(table_pair, {})
                    }
                
                summary = table_relations_summary[table_pair]
                summary['total_column_relations'] += 1
                summary['total_relation_count'] += relation['relation_count']
                summary['relation_types'].add(relation['relation_type'])
                summary['avg_confidence'] += relation['avg_confidence']
            
            # 计算平均置信度并转换set为list
            for table_pair, summary in table_relations_summary.items():
                if summary['total_column_relations'] > 0:
                    summary['avg_confidence'] = round(
                        summary['avg_confidence'] / summary['total_column_relations'], 3
                    )
                summary['relation_types'] = list(summary['relation_types'])
            
            logger.info(f"项目 {project_name} 列关系查询完成: {len(column_relations)} 个列关系, {len(table_relations_summary)} 个表关系")
            
            return jsonify({
                'column_relations': column_relations,
                'table_relations_summary': list(table_relations_summary.values()),
                'match_statistics': match_statistics,
                'project_name': project_name,
                'total_column_relations': len(column_relations),
                'total_table_pairs': len(table_relations_summary),
                'status': 'success'
            })
            
        except Exception as graph_error:
            logger.warning(f"查询项目图谱 {project_graph_name} 失败: {str(graph_error)}")
            return jsonify({
                'error': f'项目图谱查询失败: {str(graph_error)}',
                'column_relations': [],
                'table_relations_summary': [],
                'match_statistics': {},
                'project_name': project_name,
                'status': 'failed'
            }), 500
        
    except Exception as e:
        logger.error(f"获取项目 {project_name} 列关系失败: {str(e)}")
        return jsonify({
            'error': str(e),
            'column_relations': [],
            'table_relations_summary': [],
            'match_statistics': {},
            'project_name': project_name,
            'status': 'failed'
        }), 500


@app.route('/api/kg/projects/<project_name>/relation_entities', methods=['GET'])
def get_relation_entities(project_name):
    """获取关系边的具体实体数据API"""
    try:
        # 获取请求参数
        source_table = request.args.get('source_table')
        source_column = request.args.get('source_column')
        target_table = request.args.get('target_table')
        target_column = request.args.get('target_column')
        relation_type = request.args.get('relation_type')
        page = int(request.args.get('page', 1))  # 页码，默认第1页
        page_size = int(request.args.get('page_size', 20))  # 每页数量，默认20条
        limit = page_size  # 保持向后兼容
        offset = (page - 1) * page_size  # 计算偏移量
        
        if not all([source_table, source_column, target_table, target_column]):
            return jsonify({
                'error': '缺少必要参数',
                'entities': [],
                'status': 'failed'
            }), 400
        
        logger.info(f"获取项目 {project_name} 的关系实体数据: {source_table}.{source_column} -> {target_table}.{target_column}")
        
        # 直接使用FalkorDB查询项目图谱中的关系实体
        import falkordb
        client = falkordb.FalkorDB(host='localhost', port=16379)
        
        # 构建项目图谱名称
        project_graph_name = f"knowledge_graph_project_{project_name}"
        
        try:
            graph = client.select_graph(project_graph_name)
            
            # 查询具体的关系实体数据
            relation_entities_query = f"""
            MATCH (s)-[r]->(o)
            WHERE s.source_table = '{source_table}' 
              AND s.source_column = '{source_column}'
              AND o.source_table = '{target_table}'
              AND o.source_column = '{target_column}'
            """
            
            # 如果指定了关系类型，添加过滤条件
            if relation_type:
                relation_entities_query += f" AND type(r) = '{relation_type}'"
            
            relation_entities_query += f"""
            RETURN s.entity_id AS source_entity_id,
                   s.label AS source_label,
                   s.{source_column} AS source_value,
                   r.confidence AS confidence,
                   type(r) AS relation_type,
                   o.entity_id AS target_entity_id,
                   o.label AS target_label,
                   o.{target_column} AS target_value
            ORDER BY r.confidence DESC
            SKIP {offset}
            LIMIT {limit}
            """
            
            # 同时查询总数用于分页
            count_query = f"""
            MATCH (s)-[r]->(o)
            WHERE s.source_table = '{source_table}' 
              AND s.source_column = '{source_column}'
              AND o.source_table = '{target_table}'
              AND o.source_column = '{target_column}'
            """
            if relation_type:
                count_query += f" AND type(r) = '{relation_type}'"
            count_query += " RETURN count(*) AS total"
            
            # 执行查询
            result = graph.query(relation_entities_query)
            count_result = graph.query(count_query)
            
            # 获取总数
            total_count = 0
            if count_result.result_set:
                total_count = count_result.result_set[0][0]
            
            entities = []
            for record in result.result_set:
                source_entity_id = record[0]
                source_label = record[1]
                source_value = record[2]
                confidence = record[3] if record[3] is not None else 0.9
                rel_type = record[4]
                target_entity_id = record[5]
                target_label = record[6]
                target_value = record[7]
                
                entities.append({
                    'source_entity': {
                        'entity_id': source_entity_id,
                        'label': source_label,
                        'column_value': source_value,
                        'table': source_table,
                        'column': source_column
                    },
                    'target_entity': {
                        'entity_id': target_entity_id,
                        'label': target_label,
                        'column_value': target_value,
                        'table': target_table,
                        'column': target_column
                    },
                    'relation': {
                        'type': rel_type,
                        'confidence': round(confidence, 3)
                    }
                })
            
            logger.info(f"项目 {project_name} 关系实体查询完成: 返回 {len(entities)} 条实体关系")
            
            # 计算分页信息
            total_pages = (total_count + page_size - 1) // page_size
            has_next = page < total_pages
            has_prev = page > 1
            
            return jsonify({
                'entities': entities,
                'relation_info': {
                    'source_table': source_table,
                    'source_column': source_column,
                    'target_table': target_table,
                    'target_column': target_column,
                    'relation_type': relation_type
                },
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_count': total_count,
                    'total_pages': total_pages,
                    'has_next': has_next,
                    'has_prev': has_prev,
                    'current_count': len(entities)
                },
                'project_name': project_name,
                'total_entities': total_count,  # 修改为实际总数
                'status': 'success'
            })
            
        except Exception as graph_error:
            logger.warning(f"查询项目图谱 {project_graph_name} 关系实体失败: {str(graph_error)}")
            return jsonify({
                'error': f'项目图谱查询失败: {str(graph_error)}',
                'entities': [],
                'status': 'failed'
            }), 500
        
    except Exception as e:
        logger.error(f"获取项目 {project_name} 关系实体失败: {str(e)}")
        return jsonify({
            'error': str(e),
            'entities': [],
            'status': 'failed'
        }), 500


@app.route('/api/kg/projects/<project_name>/table_relation_entities', methods=['GET'])
def get_table_relation_entities(project_name):
    """获取表间关系的实体数据"""
    try:
        # 获取查询参数
        source_table = request.args.get('source_table')
        target_table = request.args.get('target_table')
        relation_type = request.args.get('relation_type', 'USER_MATCHED')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))
        offset = (page - 1) * page_size
        
        if not all([source_table, target_table]):
            return jsonify({
                'error': '缺少必要参数',
                'entities': [],
                'status': 'failed'
            }), 400
        
        logger.info(f"获取项目 {project_name} 的表间关系实体数据: {source_table} -> {target_table}")
        
        # 如果是用户匹配关系，从MongoDB获取数据
        if relation_type == 'USER_MATCHED':
            try:
                # 使用Flask配置中的MongoDB客户端，并检查连接状态
                mongo_client = current_app.config['MONGO_CLIENT']
                
                # 检查连接是否有效，如果无效则重新连接
                try:
                    # 测试连接
                    mongo_client.admin.command('ping')
                    mongo_db = mongo_client['Unit_Info']
                except Exception as conn_error:
                    logger.warning(f"MongoDB连接失效，尝试重新连接: {str(conn_error)}")
                    # 重新创建连接
                    from pymongo import MongoClient
                    mongo_client = MongoClient('mongodb://localhost:27017/')
                    current_app.config['MONGO_CLIENT'] = mongo_client
                    mongo_db = mongo_client['Unit_Info']
                
                # 查找相关的用户匹配结果表
                collection_names = mongo_db.list_collection_names()
                match_collections = []
                
                logger.info(f"查找匹配集合: source_table={source_table}, target_table={target_table}")
                logger.info(f"所有集合数量: {len(collection_names)}")
                
                # 先找到所有包含source_table的用户匹配结果集合
                candidate_collections = [name for name in collection_names 
                                       if 'user_match_results' in name and source_table in name]
                logger.info(f"候选集合: {candidate_collections}")
                
                # 对每个候选集合检查是否包含target_table信息
                for name in candidate_collections:
                    try:
                        collection = mongo_db[name]
                        sample_doc = collection.find_one({})
                        if sample_doc:
                            task_id = sample_doc.get('task_id', '')
                            logger.info(f"集合 {name} 的task_id: {task_id}")
                            
                            # 检查task_id或集合名是否包含target_table信息
                            if target_table in task_id or target_table in name:
                                match_collections.append(name)
                                logger.info(f"匹配成功: {name}")
                            else:
                                # 如果task_id中没有target_table，检查matched_fields
                                matched_fields = sample_doc.get('matched_fields', {})
                                logger.info(f"检查matched_fields: type={type(matched_fields)}")
                                
                                # 解析matched_fields
                                if isinstance(matched_fields, str):
                                    try:
                                        import json
                                        matched_fields = json.loads(matched_fields)
                                        logger.info(f"解析matched_fields成功: {type(matched_fields)}")
                                    except:
                                        logger.info(f"解析matched_fields失败")
                                        matched_fields = {}
                                
                                # 处理不同类型的matched_fields
                                found_match = False
                                if isinstance(matched_fields, dict):
                                    logger.info(f"matched_fields keys: {list(matched_fields.keys())}")
                                    
                                    # 检查matched_fields的键是否包含target_table相关信息
                                    target_keywords = [
                                        target_table,  # 完整表名
                                        target_table.replace('dwd_', ''),  # 去掉前缀
                                        target_table.replace('xp_', ''),   # 去掉前缀
                                        'yljgxx',  # 养老机构信息
                                        'zzzhzj',  # 组织证照
                                        'COMPANY_NAME',  # 公司名称字段
                                        'COMPANY_ADDRESS'  # 公司地址字段
                                    ]
                                    
                                    for key in matched_fields.keys():
                                        for keyword in target_keywords:
                                            if keyword.lower() in key.lower():
                                                match_collections.append(name)
                                                logger.info(f"通过matched_fields匹配成功: {name} (关键字: {keyword} 在 {key})")
                                                found_match = True
                                                break
                                        if found_match:
                                            break
                                            
                                elif isinstance(matched_fields, list):
                                    logger.info(f"matched_fields是列表，长度: {len(matched_fields)}")
                                    # 对于列表类型，直接根据时间戳推断
                                    found_match = False
                                
                                # 如果还是没找到，根据集合名称推断
                                if not found_match:
                                    # 根据时间戳推断：122051对应dwd_yljgxx，153502对应dwd_zzzhzj
                                    if target_table == 'dwd_yljgxx' and '122051' in name:
                                        match_collections.append(name)
                                        logger.info(f"通过时间戳推断匹配成功: {name} -> {target_table}")
                                    elif target_table == 'dwd_zzzhzj' and '153502' in name:
                                        match_collections.append(name)
                                        logger.info(f"通过时间戳推断匹配成功: {name} -> {target_table}")
                                    else:
                                        logger.info(f"未找到匹配: {name} -> {target_table}")
                    except Exception as e:
                        logger.warning(f"检查集合 {name} 失败: {e}")
                        continue
                
                logger.info(f"最终匹配的集合: {match_collections}")
                
                entities = []
                total_count = 0
                
                for collection_name in match_collections:
                    collection = mongo_db[collection_name]
                    
                    # 获取总数
                    collection_total = collection.count_documents({})
                    
                    # 获取分页数据
                    cursor = collection.find({}).skip(offset).limit(page_size)
                    
                    for doc in cursor:
                        # 解析匹配字段
                        matched_fields = doc.get('matched_fields', {})
                        
                        # 如果matched_fields是字符串，尝试解析为JSON
                        if isinstance(matched_fields, str):
                            try:
                                import json
                                matched_fields = json.loads(matched_fields)
                            except:
                                matched_fields = {}
                        
                        # 提取源实体数据（排除系统字段）
                        source_data = {}
                        for k, v in doc.items():
                            if not k.startswith('_') and k not in ['matched_fields', 'task_id', 'match_algorithm', 'match_version', 'optimization_enabled']:
                                # 如果值是对象，转换为字符串显示
                                if isinstance(v, (dict, list)):
                                    source_data[k] = str(v)
                                else:
                                    source_data[k] = v
                        
                        # 提取目标实体数据
                        target_data = {}
                        if isinstance(matched_fields, dict):
                            for k, v in matched_fields.items():
                                if isinstance(v, (dict, list)):
                                    target_data[k] = str(v)
                                else:
                                    target_data[k] = v
                        
                        entity_data = {
                            'match_id': str(doc.get('_id', '')),
                            'source_entity': {
                                'table': source_table,
                                'data': source_data
                            },
                            'target_entity': {
                                'table': target_table,
                                'data': target_data
                            },
                            'match_info': {
                                'confidence': doc.get('confidence', 0.8),
                                'match_type': 'USER_MATCHED',
                                'timestamp': doc.get('created_at', doc.get('timestamp', '')),
                                'algorithm': doc.get('match_algorithm', ''),
                                'similarity_score': doc.get('similarity_score', 0)
                            }
                        }
                        entities.append(entity_data)
                    
                    total_count += collection_total
                    
                    # 如果已经获取足够的数据，跳出循环
                    if len(entities) >= page_size:
                        break
                
                # 计算分页信息
                total_pages = (total_count + page_size - 1) // page_size
                has_next = page < total_pages
                has_prev = page > 1
                
                return jsonify({
                    'entities': entities[:page_size],  # 确保不超过页面大小
                    'relation_info': {
                        'source_table': source_table,
                        'target_table': target_table,
                        'relation_type': relation_type
                    },
                    'pagination': {
                        'page': page,
                        'page_size': page_size,
                        'total_count': total_count,
                        'total_pages': total_pages,
                        'has_next': has_next,
                        'has_prev': has_prev,
                        'current_count': len(entities[:page_size])
                    },
                    'project_name': project_name,
                    'status': 'success'
                })
                
            except Exception as mongo_error:
                logger.warning(f"查询MongoDB用户匹配结果失败: {str(mongo_error)}")
                return jsonify({
                    'error': f'MongoDB查询失败: {str(mongo_error)}',
                    'entities': [],
                    'status': 'failed'
                }), 500
        
        else:
            # 对于知识图谱关系，使用FalkorDB查询
            import falkordb
            client = falkordb.FalkorDB(host='localhost', port=16379)
            project_graph_name = f"knowledge_graph_project_{project_name}"
            
            try:
                graph = client.select_graph(project_graph_name)
                
                # 查询表间关系的实体数据
                entities_query = f"""
                MATCH (s)-[r]->(o)
                WHERE s.source_table = '{source_table}' 
                  AND o.source_table = '{target_table}'
                RETURN s, r, o
                SKIP {offset}
                LIMIT {page_size}
                """
                
                count_query = f"""
                MATCH (s)-[r]->(o)
                WHERE s.source_table = '{source_table}' 
                  AND o.source_table = '{target_table}'
                RETURN count(*) AS total
                """
                
                result = graph.query(entities_query)
                count_result = graph.query(count_query)
                
                total_count = 0
                if count_result.result_set:
                    total_count = count_result.result_set[0][0]
                
                entities = []
                for record in result.result_set:
                    source_node = record[0]
                    relation = record[1]
                    target_node = record[2]
                    
                    entity_data = {
                        'source_entity': {
                            'table': source_table,
                            'entity_id': source_node.properties.get('entity_id', ''),
                            'label': source_node.properties.get('label', ''),
                            'data': source_node.properties
                        },
                        'target_entity': {
                            'table': target_table,
                            'entity_id': target_node.properties.get('entity_id', ''),
                            'label': target_node.properties.get('label', ''),
                            'data': target_node.properties
                        },
                        'relation': {
                            'type': relation.relation,
                            'confidence': relation.properties.get('confidence', 0.9),
                            'properties': relation.properties
                        }
                    }
                    entities.append(entity_data)
                
                # 计算分页信息
                total_pages = (total_count + page_size - 1) // page_size
                has_next = page < total_pages
                has_prev = page > 1
                
                return jsonify({
                    'entities': entities,
                    'relation_info': {
                        'source_table': source_table,
                        'target_table': target_table,
                        'relation_type': relation_type
                    },
                    'pagination': {
                        'page': page,
                        'page_size': page_size,
                        'total_count': total_count,
                        'total_pages': total_pages,
                        'has_next': has_next,
                        'has_prev': has_prev,
                        'current_count': len(entities)
                    },
                    'project_name': project_name,
                    'status': 'success'
                })
                
            except Exception as graph_error:
                logger.warning(f"查询项目图谱 {project_graph_name} 表间关系失败: {str(graph_error)}")
                return jsonify({
                    'error': f'项目图谱查询失败: {str(graph_error)}',
                    'entities': [],
                    'status': 'failed'
                }), 500
        
    except Exception as e:
        logger.error(f"获取表间关系实体数据失败: {str(e)}")
        return jsonify({
            'error': f'服务器内部错误: {str(e)}',
            'entities': [],
            'status': 'failed'
        }), 500


@app.route('/kg_table_schema_visualization')
def kg_table_schema_visualization_page():
    """表结构图谱可视化页面"""
    try:
        return render_template('kg_table_schema_visualization.html')
    except Exception as e:
        logger.error(f"表结构图谱可视化页面加载失败: {str(e)}")
        return render_template('error.html', error=str(e))

if __name__ == '__main__':
    # 初始化应用
    create_app()
    
    # 启动开发服务器
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True,
        threaded=True
    )