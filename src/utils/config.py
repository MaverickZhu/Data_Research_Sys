"""
配置管理模块
处理系统配置文件的读取和管理
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录路径
        """
        if config_dir is None:
            # 默认使用项目根目录下的config文件夹
            project_root = Path(__file__).parent.parent.parent
            self.config_dir = project_root / "config"
        else:
            self.config_dir = Path(config_dir)
            
        # 验证配置目录存在
        if not self.config_dir.exists():
            raise FileNotFoundError(f"配置目录不存在: {self.config_dir}")
            
        # 加载配置
        self._database_config = None
        self._matching_config = None
        self._web_config = None
        
        self._load_configs()
        
    def _load_configs(self):
        """加载所有配置文件"""
        try:
            # 加载数据库配置
            self._database_config = self._load_yaml_config("database.yaml")
            logger.info("数据库配置加载成功")
            
            # 加载匹配算法配置
            self._matching_config = self._load_yaml_config("matching.yaml")
            logger.info("匹配算法配置加载成功")
            
            # 尝试加载Web配置（可选）
            web_config_path = self.config_dir / "web.yaml"
            if web_config_path.exists():
                self._web_config = self._load_yaml_config("web.yaml")
                logger.info("Web配置加载成功")
            else:
                # 使用默认Web配置
                self._web_config = self._get_default_web_config()
                logger.info("使用默认Web配置")
                
        except Exception as e:
            logger.error(f"配置加载失败: {str(e)}")
            raise
            
    def _load_yaml_config(self, filename: str) -> Dict[str, Any]:
        """
        加载YAML配置文件
        
        Args:
            filename: 配置文件名
            
        Returns:
            Dict[str, Any]: 配置内容
        """
        config_path = self.config_dir / filename
        
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
            if config is None:
                raise ValueError(f"配置文件为空: {config_path}")
                
            return config
            
        except yaml.YAMLError as e:
            logger.error(f"YAML解析错误 {config_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"读取配置文件失败 {config_path}: {str(e)}")
            raise
            
    def _get_default_web_config(self) -> Dict[str, Any]:
        """
        获取默认Web配置
        
        Returns:
            Dict[str, Any]: 默认Web配置
        """
        return {
            'flask': {
                'host': '0.0.0.0',
                'port': 5000,
                'debug': False,
                'secret_key': 'your-secret-key-here'
            },
            'cors': {
                'origins': ['*'],
                'methods': ['GET', 'POST', 'PUT', 'DELETE'],
                'allow_headers': ['Content-Type', 'Authorization']
            },
            'pagination': {
                'default_per_page': 20,
                'max_per_page': 100
            }
        }
        
    def get_database_config(self) -> Dict[str, Any]:
        """
        获取数据库配置
        
        Returns:
            Dict[str, Any]: 数据库配置
        """
        if self._database_config is None:
            raise RuntimeError("数据库配置未加载")
        return self._database_config.copy()
        
    def get_matching_config(self) -> Dict[str, Any]:
        """
        获取匹配算法配置
        
        Returns:
            Dict[str, Any]: 匹配算法配置
        """
        if self._matching_config is None:
            raise RuntimeError("匹配算法配置未加载")
        return self._matching_config.copy()
        
    def get_web_config(self) -> Dict[str, Any]:
        """
        获取Web配置
        
        Returns:
            Dict[str, Any]: Web配置
        """
        if self._web_config is None:
            raise RuntimeError("Web配置未加载")
        return self._web_config.copy()
        
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
                config = self._database_config
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
            
    def reload_configs(self):
        """重新加载所有配置"""
        try:
            self._load_configs()
            logger.info("配置重新加载成功")
        except Exception as e:
            logger.error(f"重新加载配置失败: {str(e)}")
            raise
            
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