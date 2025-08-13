"""
内存管理和监控模块
用于监控系统内存使用情况，防止内存溢出
"""

import logging
import psutil
import gc
import os
from typing import Dict, Optional, Callable
from threading import Lock
import time

logger = logging.getLogger(__name__)

class MemoryManager:
    """内存管理器 - 监控和管理系统内存使用"""
    
    def __init__(self, warning_threshold: float = 0.8, critical_threshold: float = 0.9):
        """
        初始化内存管理器
        
        Args:
            warning_threshold: 内存使用警告阈值（0-1）
            critical_threshold: 内存使用临界阈值（0-1）
        """
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self._lock = Lock()
        self._callbacks = {
            'warning': [],
            'critical': [],
            'recovery': []
        }
        self._last_check_time = 0
        self._check_interval = 5  # 检查间隔（秒）
        
    def get_memory_info(self) -> Dict:
        """获取当前内存使用信息"""
        try:
            # 系统内存信息
            memory = psutil.virtual_memory()
            
            # 当前进程内存信息
            process = psutil.Process(os.getpid())
            process_memory = process.memory_info()
            
            return {
                'system': {
                    'total': memory.total,
                    'available': memory.available,
                    'used': memory.used,
                    'usage_percent': memory.percent,
                    'free': memory.free
                },
                'process': {
                    'rss': process_memory.rss,  # 物理内存
                    'vms': process_memory.vms,  # 虚拟内存
                    'percent': process.memory_percent(),
                    'num_threads': process.num_threads()
                },
                'timestamp': time.time()
            }
        except Exception as e:
            logger.error(f"获取内存信息失败: {e}")
            return {}
    
    def check_memory_status(self, force: bool = False) -> Dict:
        """
        检查内存状态
        
        Args:
            force: 是否强制检查（忽略检查间隔）
            
        Returns:
            Dict: 内存状态信息
        """
        current_time = time.time()
        
        # 检查是否需要执行检查
        if not force and (current_time - self._last_check_time) < self._check_interval:
            return {'status': 'skipped', 'reason': 'check_interval'}
        
        with self._lock:
            self._last_check_time = current_time
            
            memory_info = self.get_memory_info()
            if not memory_info:
                return {'status': 'error', 'reason': 'failed_to_get_memory_info'}
            
            system_usage = memory_info['system']['usage_percent'] / 100.0
            process_usage = memory_info['process']['percent'] / 100.0
            
            status = 'normal'
            
            # 判断内存状态
            if system_usage >= self.critical_threshold or process_usage >= self.critical_threshold:
                status = 'critical'
                self._trigger_callbacks('critical', memory_info)
                logger.error(f"内存使用临界: 系统 {system_usage:.1%}, 进程 {process_usage:.1%}")
                
            elif system_usage >= self.warning_threshold or process_usage >= self.warning_threshold:
                status = 'warning'
                self._trigger_callbacks('warning', memory_info)
                logger.warning(f"内存使用警告: 系统 {system_usage:.1%}, 进程 {process_usage:.1%}")
                
            else:
                # 检查是否从高内存使用状态恢复
                if hasattr(self, '_last_status') and self._last_status in ['warning', 'critical']:
                    self._trigger_callbacks('recovery', memory_info)
                    logger.info(f"内存使用恢复正常: 系统 {system_usage:.1%}, 进程 {process_usage:.1%}")
            
            self._last_status = status
            
            return {
                'status': status,
                'memory_info': memory_info,
                'system_usage': system_usage,
                'process_usage': process_usage,
                'timestamp': current_time
            }
    
    def force_garbage_collection(self) -> Dict:
        """强制垃圾回收"""
        try:
            logger.info("开始强制垃圾回收...")
            
            # 获取回收前的内存信息
            before_info = self.get_memory_info()
            before_rss = before_info.get('process', {}).get('rss', 0)
            
            # 执行垃圾回收
            collected = []
            for generation in range(3):
                count = gc.collect(generation)
                collected.append(count)
            
            # 获取回收后的内存信息
            after_info = self.get_memory_info()
            after_rss = after_info.get('process', {}).get('rss', 0)
            
            freed_memory = before_rss - after_rss
            
            logger.info(f"垃圾回收完成: 释放内存 {freed_memory / 1024 / 1024:.2f}MB, "
                       f"回收对象 {sum(collected)} 个")
            
            return {
                'success': True,
                'collected_objects': collected,
                'freed_memory': freed_memory,
                'before_memory': before_info,
                'after_memory': after_info
            }
            
        except Exception as e:
            logger.error(f"垃圾回收失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def optimize_memory_usage(self) -> Dict:
        """优化内存使用"""
        try:
            logger.info("开始内存优化...")
            
            optimization_results = []
            
            # 1. 强制垃圾回收
            gc_result = self.force_garbage_collection()
            optimization_results.append(('garbage_collection', gc_result))
            
            # 2. 清理模块缓存（谨慎操作）
            try:
                import sys
                module_count_before = len(sys.modules)
                
                # 清理一些可以安全清理的缓存
                if hasattr(sys, '_clear_type_cache'):
                    sys._clear_type_cache()
                
                optimization_results.append(('type_cache_cleared', True))
                logger.info("类型缓存已清理")
                
            except Exception as e:
                logger.warning(f"清理缓存时出现问题: {e}")
                optimization_results.append(('cache_cleanup', False))
            
            # 3. 检查优化效果
            final_memory = self.get_memory_info()
            
            return {
                'success': True,
                'optimizations': optimization_results,
                'final_memory': final_memory
            }
            
        except Exception as e:
            logger.error(f"内存优化失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def register_callback(self, event_type: str, callback: Callable):
        """
        注册内存事件回调函数
        
        Args:
            event_type: 事件类型 ('warning', 'critical', 'recovery')
            callback: 回调函数
        """
        if event_type in self._callbacks:
            self._callbacks[event_type].append(callback)
        else:
            logger.warning(f"不支持的事件类型: {event_type}")
    
    def _trigger_callbacks(self, event_type: str, memory_info: Dict):
        """触发回调函数"""
        callbacks = self._callbacks.get(event_type, [])
        for callback in callbacks:
            try:
                callback(event_type, memory_info)
            except Exception as e:
                logger.error(f"回调函数执行失败: {e}")
    
    def get_memory_recommendations(self, memory_info: Optional[Dict] = None) -> Dict:
        """
        获取内存优化建议
        
        Args:
            memory_info: 内存信息，如果不提供则自动获取
            
        Returns:
            Dict: 优化建议
        """
        if memory_info is None:
            memory_info = self.get_memory_info()
        
        if not memory_info:
            return {'error': 'Unable to get memory info'}
        
        system_usage = memory_info['system']['usage_percent'] / 100.0
        process_usage = memory_info['process']['percent'] / 100.0
        
        recommendations = []
        
        if system_usage > 0.9:
            recommendations.append({
                'type': 'critical',
                'message': '系统内存使用过高，建议立即减少批处理大小或重启应用',
                'action': 'reduce_batch_size'
            })
        elif system_usage > 0.8:
            recommendations.append({
                'type': 'warning', 
                'message': '系统内存使用较高，建议减少并发任务数量',
                'action': 'reduce_concurrency'
            })
        
        if process_usage > 0.8:
            recommendations.append({
                'type': 'warning',
                'message': '进程内存使用较高，建议执行垃圾回收',
                'action': 'garbage_collection'
            })
        
        if memory_info['process']['num_threads'] > 50:
            recommendations.append({
                'type': 'info',
                'message': '线程数量较多，建议检查线程池配置',
                'action': 'check_thread_pools'
            })
        
        return {
            'system_usage': system_usage,
            'process_usage': process_usage,
            'recommendations': recommendations,
            'timestamp': time.time()
        }

# 全局内存管理器实例
_global_memory_manager = None
_manager_lock = Lock()

def get_memory_manager() -> MemoryManager:
    """获取全局内存管理器实例（单例模式）"""
    global _global_memory_manager
    
    if _global_memory_manager is None:
        with _manager_lock:
            if _global_memory_manager is None:
                _global_memory_manager = MemoryManager()
    
    return _global_memory_manager

def check_memory_before_task(min_available_mb: int = 1000) -> bool:
    """
    任务执行前检查内存是否充足
    
    Args:
        min_available_mb: 最小可用内存（MB）
        
    Returns:
        bool: 内存是否充足
    """
    try:
        memory_manager = get_memory_manager()
        memory_info = memory_manager.get_memory_info()
        
        if not memory_info:
            logger.warning("无法获取内存信息，假设内存充足")
            return True
        
        available_mb = memory_info['system']['available'] / 1024 / 1024
        
        if available_mb < min_available_mb:
            logger.warning(f"可用内存不足: {available_mb:.0f}MB < {min_available_mb}MB")
            
            # 尝试释放内存
            logger.info("尝试释放内存...")
            memory_manager.force_garbage_collection()
            
            # 重新检查
            updated_info = memory_manager.get_memory_info()
            updated_available = updated_info['system']['available'] / 1024 / 1024
            
            if updated_available < min_available_mb:
                logger.error(f"释放内存后仍不足: {updated_available:.0f}MB < {min_available_mb}MB")
                return False
            else:
                logger.info(f"内存释放成功: {updated_available:.0f}MB")
                return True
        
        return True
        
    except Exception as e:
        logger.error(f"内存检查失败: {e}")
        return True  # 检查失败时假设内存充足，避免阻塞任务
