#!/usr/bin/env python3
"""
系统监控脚本
监控MongoDB连接、内存使用、任务状态等关键指标
"""

import os
import sys
import time
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import ConfigManager
from src.database.connection import DatabaseManager
from src.utils.memory_manager import get_memory_manager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / f'system_monitor_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SystemMonitor:
    """系统监控器"""
    
    def __init__(self):
        """初始化监控器"""
        self.config_manager = ConfigManager()
        self.db_manager = None
        self.memory_manager = get_memory_manager()
        self.last_check_time = None
        self.alert_thresholds = {
            'memory_warning': 0.8,
            'memory_critical': 0.9,
            'task_timeout_hours': 2,
            'connection_retry_max': 3
        }
        
    def initialize_database(self):
        """初始化数据库连接"""
        try:
            config = self.config_manager.get_database_config()
            self.db_manager = DatabaseManager(config)
            logger.info("数据库连接初始化成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接初始化失败: {e}")
            return False
    
    def check_database_health(self):
        """检查数据库健康状态"""
        try:
            if not self.db_manager:
                return {
                    'status': 'error',
                    'message': '数据库管理器未初始化'
                }
            
            # 检查MongoDB连接
            start_time = time.time()
            mongo_client = self.db_manager.mongo_client
            mongo_client.admin.command('ping')
            mongo_response_time = time.time() - start_time
            
            # 获取数据库统计信息
            db = self.db_manager.get_db()
            stats = db.command('dbstats')
            
            # 检查Redis连接
            start_time = time.time()
            redis_client = self.db_manager.get_redis_client()
            redis_client.ping()
            redis_response_time = time.time() - start_time
            
            return {
                'status': 'healthy',
                'mongodb': {
                    'connected': True,
                    'response_time': round(mongo_response_time * 1000, 2),  # 毫秒
                    'database_size': stats.get('dataSize', 0),
                    'storage_size': stats.get('storageSize', 0),
                    'collections': stats.get('collections', 0),
                    'indexes': stats.get('indexes', 0)
                },
                'redis': {
                    'connected': True,
                    'response_time': round(redis_response_time * 1000, 2)  # 毫秒
                }
            }
            
        except Exception as e:
            logger.error(f"数据库健康检查失败: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'mongodb': {'connected': False},
                'redis': {'connected': False}
            }
    
    def check_memory_status(self):
        """检查内存状态"""
        try:
            memory_info = self.memory_manager.get_memory_info()
            memory_status = self.memory_manager.check_memory_status(force=True)
            recommendations = self.memory_manager.get_memory_recommendations(memory_info)
            
            return {
                'status': memory_status.get('status', 'unknown'),
                'system_usage': memory_status.get('system_usage', 0),
                'process_usage': memory_status.get('process_usage', 0),
                'memory_info': memory_info,
                'recommendations': recommendations.get('recommendations', [])
            }
            
        except Exception as e:
            logger.error(f"内存状态检查失败: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def check_running_tasks(self):
        """检查运行中的任务"""
        try:
            if not self.db_manager:
                return {'status': 'error', 'message': '数据库未连接'}
            
            db = self.db_manager.get_db()
            tasks_collection = db['user_matching_tasks']
            
            # 查询各种状态的任务
            running_tasks = list(tasks_collection.find({'status': 'running'}))
            pending_tasks = list(tasks_collection.find({'status': 'pending'}))
            failed_tasks = list(tasks_collection.find({
                'status': 'failed',
                'created_at': {'$gte': (datetime.now() - timedelta(hours=24)).isoformat()}
            }))
            
            # 检查超时任务
            timeout_time = datetime.now() - timedelta(hours=self.alert_thresholds['task_timeout_hours'])
            timeout_tasks = list(tasks_collection.find({
                'status': {'$in': ['running', 'pending']},
                'created_at': {'$lt': timeout_time.isoformat()}
            }))
            
            return {
                'status': 'healthy',
                'running_count': len(running_tasks),
                'pending_count': len(pending_tasks),
                'failed_count_24h': len(failed_tasks),
                'timeout_count': len(timeout_tasks),
                'timeout_tasks': [
                    {
                        'task_id': task.get('task_id'),
                        'status': task.get('status'),
                        'created_at': task.get('created_at'),
                        'config_id': task.get('config_id')
                    }
                    for task in timeout_tasks[:5]  # 只显示前5个
                ]
            }
            
        except Exception as e:
            logger.error(f"任务状态检查失败: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def check_system_resources(self):
        """检查系统资源"""
        try:
            import psutil
            
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            
            # 磁盘使用率
            disk_usage = psutil.disk_usage('/')
            
            # 网络统计
            net_io = psutil.net_io_counters()
            
            return {
                'status': 'healthy',
                'cpu': {
                    'usage_percent': cpu_percent,
                    'count': cpu_count
                },
                'disk': {
                    'total': disk_usage.total,
                    'used': disk_usage.used,
                    'free': disk_usage.free,
                    'usage_percent': (disk_usage.used / disk_usage.total) * 100
                },
                'network': {
                    'bytes_sent': net_io.bytes_sent,
                    'bytes_recv': net_io.bytes_recv,
                    'packets_sent': net_io.packets_sent,
                    'packets_recv': net_io.packets_recv
                }
            }
            
        except Exception as e:
            logger.error(f"系统资源检查失败: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def generate_alerts(self, health_report):
        """生成告警"""
        alerts = []
        
        # 内存告警
        memory_status = health_report.get('memory', {})
        if memory_status.get('status') == 'critical':
            alerts.append({
                'level': 'critical',
                'type': 'memory',
                'message': f"内存使用临界: 系统 {memory_status.get('system_usage', 0):.1%}, 进程 {memory_status.get('process_usage', 0):.1%}"
            })
        elif memory_status.get('status') == 'warning':
            alerts.append({
                'level': 'warning',
                'type': 'memory',
                'message': f"内存使用警告: 系统 {memory_status.get('system_usage', 0):.1%}, 进程 {memory_status.get('process_usage', 0):.1%}"
            })
        
        # 数据库告警
        database_status = health_report.get('database', {})
        if database_status.get('status') == 'error':
            alerts.append({
                'level': 'critical',
                'type': 'database',
                'message': f"数据库连接异常: {database_status.get('message', '未知错误')}"
            })
        
        # 任务告警
        tasks_status = health_report.get('tasks', {})
        if tasks_status.get('timeout_count', 0) > 0:
            alerts.append({
                'level': 'warning',
                'type': 'tasks',
                'message': f"发现 {tasks_status.get('timeout_count')} 个超时任务"
            })
        
        if tasks_status.get('failed_count_24h', 0) > 10:
            alerts.append({
                'level': 'warning',
                'type': 'tasks',
                'message': f"24小时内失败任务过多: {tasks_status.get('failed_count_24h')} 个"
            })
        
        # 系统资源告警
        resources_status = health_report.get('resources', {})
        if resources_status.get('status') == 'healthy':
            cpu_usage = resources_status.get('cpu', {}).get('usage_percent', 0)
            disk_usage = resources_status.get('disk', {}).get('usage_percent', 0)
            
            if cpu_usage > 90:
                alerts.append({
                    'level': 'warning',
                    'type': 'cpu',
                    'message': f"CPU使用率过高: {cpu_usage:.1f}%"
                })
            
            if disk_usage > 90:
                alerts.append({
                    'level': 'critical',
                    'type': 'disk',
                    'message': f"磁盘使用率过高: {disk_usage:.1f}%"
                })
        
        return alerts
    
    def run_health_check(self):
        """执行完整的健康检查"""
        logger.info("开始系统健康检查...")
        
        health_report = {
            'timestamp': datetime.now().isoformat(),
            'database': self.check_database_health(),
            'memory': self.check_memory_status(),
            'tasks': self.check_running_tasks(),
            'resources': self.check_system_resources()
        }
        
        # 生成告警
        alerts = self.generate_alerts(health_report)
        health_report['alerts'] = alerts
        
        # 记录告警
        for alert in alerts:
            if alert['level'] == 'critical':
                logger.error(f"🚨 CRITICAL: {alert['message']}")
            elif alert['level'] == 'warning':
                logger.warning(f"⚠️ WARNING: {alert['message']}")
        
        if not alerts:
            logger.info("✅ 系统状态正常，无告警")
        
        # 保存健康报告
        self.save_health_report(health_report)
        
        return health_report
    
    def save_health_report(self, health_report):
        """保存健康报告"""
        try:
            reports_dir = project_root / 'logs' / 'health_reports'
            reports_dir.mkdir(exist_ok=True)
            
            report_file = reports_dir / f"health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(health_report, f, indent=2, ensure_ascii=False, default=str)
            
            # 清理旧报告（保留最近7天）
            self.cleanup_old_reports(reports_dir)
            
        except Exception as e:
            logger.error(f"保存健康报告失败: {e}")
    
    def cleanup_old_reports(self, reports_dir, days_to_keep=7):
        """清理旧的健康报告"""
        try:
            cutoff_time = datetime.now() - timedelta(days=days_to_keep)
            
            for report_file in reports_dir.glob('health_report_*.json'):
                if report_file.stat().st_mtime < cutoff_time.timestamp():
                    report_file.unlink()
                    logger.debug(f"已清理旧报告: {report_file.name}")
                    
        except Exception as e:
            logger.error(f"清理旧报告失败: {e}")
    
    def run_continuous_monitoring(self, interval_minutes=5):
        """运行连续监控"""
        logger.info(f"开始连续监控，检查间隔: {interval_minutes} 分钟")
        
        while True:
            try:
                health_report = self.run_health_check()
                
                # 如果有严重告警，可以在这里添加通知逻辑
                critical_alerts = [alert for alert in health_report.get('alerts', []) if alert['level'] == 'critical']
                if critical_alerts:
                    logger.error(f"检测到 {len(critical_alerts)} 个严重告警")
                    # 这里可以添加邮件通知、短信通知等逻辑
                
                # 等待下次检查
                time.sleep(interval_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("收到中断信号，停止监控")
                break
            except Exception as e:
                logger.error(f"监控过程中发生错误: {e}")
                time.sleep(60)  # 错误时等待1分钟再重试

def main():
    """主函数"""
    monitor = SystemMonitor()
    
    # 初始化数据库连接
    if not monitor.initialize_database():
        logger.error("无法初始化数据库连接，退出监控")
        return
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == '--once':
            # 单次检查
            health_report = monitor.run_health_check()
            print(json.dumps(health_report, indent=2, ensure_ascii=False, default=str))
        elif sys.argv[1] == '--continuous':
            # 连续监控
            interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            monitor.run_continuous_monitoring(interval)
        else:
            print("用法:")
            print("  python system_monitor.py --once          # 单次健康检查")
            print("  python system_monitor.py --continuous [间隔分钟]  # 连续监控")
    else:
        # 默认单次检查
        health_report = monitor.run_health_check()
        print(json.dumps(health_report, indent=2, ensure_ascii=False, default=str))

if __name__ == '__main__':
    main()
