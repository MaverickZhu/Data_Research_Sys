"""
配置管理模块
处理系统配置文件的读取和管理
"""

import os
import yaml
import logging
import json
from typing import Dict, Any, Optional
from pathlib import Path
from threading import Lock

logger = logging.getLogger(__name__)


class ConfigManager:
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self.config_dir = Path(__file__).parent.parent.parent / "config"
        self._db_config = self._load_config('database.yaml')
        self._matching_config = self._load_config('matching.yaml')
        self._web_config = self._load_config('web.yaml')
        self._performance_config = self._load_config('high_performance.json', is_json=True, default={
            "parallel_processing": {
                "max_workers": 8,
                "max_db_connections": 4
            }
        })
        self._initialized = True

    def _load_config(self, filename: str, is_json: bool = False, default: Dict = None) -> Dict:
        config_path = self.config_dir / filename
        if not config_path.exists():
            logger.warning(f"配置文件 '{filename}' 未找到。")
            if default is not None:
                logger.info(f"使用默认配置 for '{filename}'.")
                return default
            return {}
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if is_json:
                    return json.load(f)
                else:
                    return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"读取配置文件失败 {config_path}: {str(e)}")
            if default is not None:
                return default
            return {}

    def get_database_config(self) -> Dict:
        return self._db_config

    def get_matching_config(self) -> Dict:
        return self._matching_config

    def get_web_config(self) -> Dict:
        return self._web_config

    def get_performance_config(self) -> Dict:
        return self._performance_config

    def update_matching_config(self, new_config: Dict[str, Any]) -> bool:
        """
        更新匹配算法配置
        
        Args:
            new_config: 新的配置内容
            
        Returns:
            bool: 更新是否成功
        """
        try:
            # 合并配置
            self._matching_config.update(new_config)
            
            # 保存到文件
            config_path = self.config_dir / "matching.yaml"
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self._matching_config, f, default_flow_style=False, allow_unicode=True)
                
            logger.info("匹配算法配置更新成功")
            return True
            
        except Exception as e:
            logger.error(f"更新配置失败: {str(e)}")
            return False
            
    def get_config_value(self, config_type: str, key_path: str, default: Any = None) -> Any:
        """
        获取指定配置值
        
        Args:
            config_type: 配置类型 ('database', 'matching', 'web')
            key_path: 配置键路径，使用点号分隔 (如 'mongodb.host')
            default: 默认值
            
        Returns:
            Any: 配置值
        """
        try:
            # 获取配置字典
            if config_type == 'database':
                config = self._db_config
            elif config_type == 'matching':
                config = self._matching_config
            elif config_type == 'web':
                config = self._web_config
            else:
                raise ValueError(f"不支持的配置类型: {config_type}")
                
            # 按点号分割路径
            keys = key_path.split('.')
            value = config
            
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default
                    
            return value
            
        except Exception as e:
            logger.error(f"获取配置值失败 {config_type}.{key_path}: {str(e)}")
            return default
            
    def validate_configs(self) -> Dict[str, bool]:
        """
        验证配置完整性
        
        Returns:
            Dict[str, bool]: 各配置文件的验证结果
        """
        results = {}
        
        # 验证数据库配置
        try:
            db_config = self.get_database_config()
            required_db_keys = ['mongodb', 'redis']
            results['database'] = all(key in db_config for key in required_db_keys)
        except Exception:
            results['database'] = False
            
        # 验证匹配配置
        try:
            match_config = self.get_matching_config()
            required_match_keys = ['exact_match', 'fuzzy_match']
            results['matching'] = all(key in match_config for key in required_match_keys)
        except Exception:
            results['matching'] = False
            
        # 验证Web配置
        try:
            web_config = self.get_web_config()
            required_web_keys = ['flask']
            results['web'] = all(key in web_config for key in required_web_keys)
        except Exception:
            results['web'] = False
            
        return results 