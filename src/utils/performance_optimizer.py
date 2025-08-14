#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能优化器模块
实现智能内存管理、对象池、流式处理等优化策略
"""

import logging
import gc
import time
import psutil
from typing import Dict, List, Any, Optional, Generator, Callable
from threading import Lock, RLock
from collections import deque
import weakref
from datetime import datetime

logger = logging.getLogger(__name__)


class ObjectPool:
    """通用对象池实现"""
    
    def __init__(self, factory: Callable, max_size: int = 1000, reset_func: Optional[Callable] = None):
        """
        初始化对象池
        
        Args:
            factory: 对象工厂函数
            max_size: 池的最大大小
            reset_func: 对象重置函数
        """
        self.factory = factory
        self.max_size = max_size
        self.reset_func = reset_func
        self._pool = deque()
        self._lock = Lock()
        self._created_count = 0
        self._reused_count = 0
        
    def acquire(self):
        """获取对象"""
        with self._lock:
            if self._pool:
                obj = self._pool.popleft()
                self._reused_count += 1
                return obj
            else:
                obj = self.factory()
                self._created_count += 1
                return obj
    
    def release(self, obj):
        """释放对象回池中"""
        if obj is None:
            return
            
        with self._lock:
            if len(self._pool) < self.max_size:
                # 重置对象状态
                if self.reset_func:
                    self.reset_func(obj)
                self._pool.append(obj)
    
    def get_stats(self) -> Dict:
        """获取池统计信息"""
        with self._lock:
            return {
                'pool_size': len(self._pool),
                'max_size': self.max_size,
                'created_count': self._created_count,
                'reused_count': self._reused_count,
                'reuse_ratio': self._reused_count / max(1, self._created_count + self._reused_count)
            }
    
    def clear(self):
        """清空对象池"""
        with self._lock:
            self._pool.clear()


class MemoryAwareProcessor:
    """内存感知处理器"""
    
    def __init__(self, memory_threshold: float = 0.8):
        """
        初始化内存感知处理器
        
        Args:
            memory_threshold: 内存使用阈值（0-1）
        """
        self.memory_threshold = memory_threshold
        self._lock = RLock()
        
    def check_memory_usage(self) -> Dict:
        """检查内存使用情况"""
        try:
            memory = psutil.virtual_memory()
            process = psutil.Process()
            process_memory = process.memory_info()
            
            return {
                'system_usage_percent': memory.percent / 100.0,
                'process_memory_mb': process_memory.rss / (1024 * 1024),
                'available_memory_mb': memory.available / (1024 * 1024),
                'is_critical': memory.percent / 100.0 > self.memory_threshold
            }
        except Exception as e:
            logger.error(f"检查内存使用失败: {e}")
            return {'is_critical': False}
    
    def adaptive_batch_size(self, base_batch_size: int, data_size_estimate: int = 1024) -> int:
        """
        根据内存情况自适应调整批次大小
        
        Args:
            base_batch_size: 基础批次大小
            data_size_estimate: 每条数据的估计大小（字节）
            
        Returns:
            调整后的批次大小
        """
        memory_info = self.check_memory_usage()
        
        if memory_info['is_critical']:
            # 内存紧张，减小批次
            adjusted_size = max(100, base_batch_size // 4)
            logger.warning(f"内存紧张，批次大小从 {base_batch_size} 调整为 {adjusted_size}")
            return adjusted_size
        
        elif memory_info['system_usage_percent'] < 0.5:
            # 内存充足，可以增大批次
            available_mb = memory_info['available_memory_mb']
            max_batch_by_memory = int(available_mb * 1024 * 1024 * 0.1 / data_size_estimate)  # 使用10%可用内存
            adjusted_size = min(base_batch_size * 2, max_batch_by_memory)
            
            if adjusted_size > base_batch_size:
                logger.info(f"内存充足，批次大小从 {base_batch_size} 调整为 {adjusted_size}")
                return adjusted_size
        
        return base_batch_size
    
    def force_gc_if_needed(self):
        """在需要时强制垃圾回收"""
        memory_info = self.check_memory_usage()
        
        if memory_info['is_critical']:
            logger.info("内存使用过高，执行垃圾回收")
            collected = gc.collect()
            logger.info(f"垃圾回收完成，回收对象数: {collected}")
            return True
        
        return False


class StreamProcessor:
    """流式处理器"""
    
    def __init__(self, chunk_size: int = 1000):
        """
        初始化流式处理器
        
        Args:
            chunk_size: 流处理块大小
        """
        self.chunk_size = chunk_size
        self.memory_processor = MemoryAwareProcessor()
        
    def process_stream(self, data_generator: Generator, processor_func: Callable) -> Generator:
        """
        流式处理数据
        
        Args:
            data_generator: 数据生成器
            processor_func: 处理函数
            
        Yields:
            处理后的数据块
        """
        chunk = []
        processed_count = 0
        
        try:
            for item in data_generator:
                chunk.append(item)
                
                if len(chunk) >= self.chunk_size:
                    # 处理当前块
                    processed_chunk = processor_func(chunk)
                    yield processed_chunk
                    
                    processed_count += len(chunk)
                    chunk.clear()
                    
                    # 检查内存并在需要时进行垃圾回收
                    if processed_count % (self.chunk_size * 10) == 0:
                        self.memory_processor.force_gc_if_needed()
            
            # 处理剩余数据
            if chunk:
                processed_chunk = processor_func(chunk)
                yield processed_chunk
                
        except Exception as e:
            logger.error(f"流式处理失败: {e}")
            raise
    
    def batch_to_stream(self, data_list: List, batch_size: Optional[int] = None) -> Generator:
        """
        将批量数据转换为流式数据
        
        Args:
            data_list: 数据列表
            batch_size: 批次大小，如果为None则使用自适应大小
            
        Yields:
            数据块
        """
        if batch_size is None:
            batch_size = self.memory_processor.adaptive_batch_size(self.chunk_size)
        
        for i in range(0, len(data_list), batch_size):
            yield data_list[i:i + batch_size]


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        """初始化性能监控器"""
        self.metrics = {}
        self._lock = Lock()
        self.start_time = time.time()
        
    def record_metric(self, name: str, value: float, timestamp: Optional[float] = None):
        """记录性能指标"""
        if timestamp is None:
            timestamp = time.time()
            
        with self._lock:
            if name not in self.metrics:
                self.metrics[name] = []
            
            self.metrics[name].append({
                'value': value,
                'timestamp': timestamp
            })
            
            # 保持最近1000个数据点
            if len(self.metrics[name]) > 1000:
                self.metrics[name] = self.metrics[name][-1000:]
    
    def get_average(self, name: str, window_seconds: int = 60) -> Optional[float]:
        """获取指定时间窗口内的平均值"""
        with self._lock:
            if name not in self.metrics:
                return None
            
            current_time = time.time()
            cutoff_time = current_time - window_seconds
            
            recent_values = [
                m['value'] for m in self.metrics[name]
                if m['timestamp'] >= cutoff_time
            ]
            
            if not recent_values:
                return None
            
            return sum(recent_values) / len(recent_values)
    
    def get_current_stats(self) -> Dict:
        """获取当前性能统计"""
        memory_info = psutil.virtual_memory()
        process = psutil.Process()
        
        return {
            'uptime_seconds': time.time() - self.start_time,
            'memory_usage_percent': memory_info.percent,
            'process_memory_mb': process.memory_info().rss / (1024 * 1024),
            'cpu_percent': process.cpu_percent(),
            'thread_count': process.num_threads(),
            'metrics_count': sum(len(values) for values in self.metrics.values())
        }
    
    def export_metrics(self) -> Dict:
        """导出所有指标"""
        with self._lock:
            return {
                'metrics': dict(self.metrics),
                'stats': self.get_current_stats(),
                'export_time': datetime.now().isoformat()
            }


class SmartCache:
    """智能缓存系统"""
    
    def __init__(self, max_size: int = 10000, ttl_seconds: int = 3600):
        """
        初始化智能缓存
        
        Args:
            max_size: 最大缓存条目数
            ttl_seconds: 生存时间（秒）
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache = {}
        self._access_times = {}
        self._lock = RLock()
        
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        with self._lock:
            if key not in self._cache:
                return None
            
            # 检查TTL
            if time.time() - self._cache[key]['timestamp'] > self.ttl_seconds:
                del self._cache[key]
                if key in self._access_times:
                    del self._access_times[key]
                return None
            
            # 更新访问时间
            self._access_times[key] = time.time()
            return self._cache[key]['value']
    
    def set(self, key: str, value: Any):
        """设置缓存值"""
        with self._lock:
            current_time = time.time()
            
            # 如果缓存已满，移除最久未访问的条目
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._evict_lru()
            
            self._cache[key] = {
                'value': value,
                'timestamp': current_time
            }
            self._access_times[key] = current_time
    
    def _evict_lru(self):
        """移除最久未使用的条目"""
        if not self._access_times:
            return
        
        lru_key = min(self._access_times.keys(), key=lambda k: self._access_times[k])
        del self._cache[lru_key]
        del self._access_times[lru_key]
    
    def clear(self):
        """清空缓存"""
        with self._lock:
            self._cache.clear()
            self._access_times.clear()
    
    def get_stats(self) -> Dict:
        """获取缓存统计"""
        with self._lock:
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'hit_rate': 'not_implemented',  # 需要额外的统计逻辑
                'ttl_seconds': self.ttl_seconds
            }


# 全局性能优化器实例
_performance_monitor = None
_memory_processor = None
_global_cache = None

def get_performance_monitor() -> PerformanceMonitor:
    """获取全局性能监控器"""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor()
    return _performance_monitor

def get_memory_processor() -> MemoryAwareProcessor:
    """获取全局内存处理器"""
    global _memory_processor
    if _memory_processor is None:
        _memory_processor = MemoryAwareProcessor()
    return _memory_processor

def get_global_cache() -> SmartCache:
    """获取全局缓存"""
    global _global_cache
    if _global_cache is None:
        _global_cache = SmartCache()
    return _global_cache


# 装饰器函数
def monitor_performance(metric_name: str):
    """性能监控装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                get_performance_monitor().record_metric(f"{metric_name}_duration", duration)
        return wrapper
    return decorator

def memory_aware(threshold: float = 0.8):
    """内存感知装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            memory_processor = get_memory_processor()
            memory_info = memory_processor.check_memory_usage()
            
            if memory_info['is_critical']:
                logger.warning(f"内存使用过高 ({memory_info['system_usage_percent']:.1%})，执行 {func.__name__}")
                memory_processor.force_gc_if_needed()
            
            return func(*args, **kwargs)
        return wrapper
    return decorator
