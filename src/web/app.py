"""
Flask Web应用主程序
提供数据匹配进度管理的Web界面
"""

from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
import logging
import yaml
import os
from datetime import datetime
from typing import Dict, List
import pandas as pd
import io

# 导入自定义模块
from src.database.connection import DatabaseManager
from src.matching.match_processor import MatchProcessor
from src.matching.multi_match_processor import MultiMatchProcessor
from src.matching.enhanced_association_processor import EnhancedAssociationProcessor, AssociationStrategy
from src.matching.optimized_match_processor import OptimizedMatchProcessor
from src.utils.logger import setup_logger
from src.utils.config import ConfigManager
from src.utils.helpers import safe_json_response, generate_match_id

# 设置日志
logger = setup_logger(__name__)

# 创建Flask应用
app = Flask(__name__)
CORS(app)

# 全局变量
db_manager = None
match_processor = None
multi_match_processor = None
enhanced_association_processor = None
optimized_match_processor = None
config_manager = None


def create_app():
    """创建和配置Flask应用"""
    global db_manager, match_processor, multi_match_processor, enhanced_association_processor, optimized_match_processor, config_manager
    
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
        
        logger.info("Flask应用初始化成功")
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