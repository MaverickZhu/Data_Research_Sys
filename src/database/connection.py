"""
数据库连接管理模块
处理MongoDB和Redis的连接管理
"""

import logging
from typing import Dict, Optional, List, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import redis
from datetime import datetime
from threading import Lock
from src.utils.helpers import convert_objectid_to_str
from pymongo.errors import ConnectionFailure
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器（单例模式）"""
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化数据库管理器
        
        Args:
            config: 数据库配置。仅在首次创建实例时需要。
        """
        # 防止重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        if config is None:
            raise ValueError("Configuration is required for the first initialization.")
            
        with self._lock:
            if hasattr(self, '_initialized') and self._initialized:
                return

            self.config = config
            self.mongodb_config = config.get('mongodb', {})
            self.redis_config = config.get('redis', {})
            
            # 初始化连接
            self._mongo_client = None
            self._mongo_db = None
            self._redis_client = None
            
            # 建立连接
            self._connect_mongodb()
            self._connect_redis()
            self._initialized = True
        
    def _connect_mongodb(self):
        """连接MongoDB"""
        try:
            self._mongo_client = MongoClient(
                self.mongodb_config.get('uri', 'mongodb://localhost:27017/'),
                maxPoolSize=self.mongodb_config.get('connection_pool', {}).get('max_pool_size', 100),
                minPoolSize=self.mongodb_config.get('connection_pool', {}).get('min_pool_size', 10),
                maxIdleTimeMS=self.mongodb_config.get('connection_pool', {}).get('max_idle_time_ms', 30000),
                connectTimeoutMS=self.mongodb_config.get('connection_pool', {}).get('connect_timeout_ms', 20000),
                serverSelectionTimeoutMS=self.mongodb_config.get('connection_pool', {}).get('server_selection_timeout_ms', 30000)
            )
            
            # 从客户端获取数据库，更稳健
            self._mongo_db = self._mongo_client.get_database()
            db_name = self._mongo_db.name
            
            connection_info = f"URI: {self.mongodb_config.get('uri', 'mongodb://localhost:27017/')} (DB: {db_name})"
            
            # 测试连接
            self._mongo_client.admin.command('ping')
            logger.info(f"MongoDB连接成功: {connection_info}")
            
        except ConnectionFailure as e:
            logger.error(f"MongoDB连接失败: {e}")
            self._mongo_client = None
            self._mongo_db = None
            raise
            
    def _connect_redis(self):
        """连接Redis"""
        try:
            redis_host = self.redis_config.get('host', 'localhost')
            redis_port = self.redis_config.get('port', 6379)
            redis_db = self.redis_config.get('db', 0)
            redis_password = self.redis_config.get('password')
            redis_username = self.redis_config.get('username')
            
            # 构建Redis连接参数
            redis_params = {
                'host': redis_host,
                'port': redis_port,
                'db': redis_db,
                'decode_responses': self.redis_config.get('decode_responses', True)
            }
            
            # 添加认证参数（如果提供）
            if redis_password:
                redis_params['password'] = redis_password
            if redis_username:
                redis_params['username'] = redis_username
            
            self._redis_client = Redis(**redis_params)
            
            # 测试连接
            self._redis_client.ping()
            logger.info(f"Redis连接成功: {redis_host}:{redis_port}/{redis_db}")
            
        except RedisConnectionError as e:
            logger.error(f"Redis连接失败: {e}")
            self._redis_client = None
            raise
            
    def get_collection(self, collection_name: str) -> Collection:
        """
        获取MongoDB集合
        
        Args:
            collection_name: 集合名称
            
        Returns:
            Collection: MongoDB集合对象
        """
        if self._mongo_db is None:
            raise Exception("MongoDB未连接")
        return self._mongo_db[collection_name]
        
    def get_redis_client(self) -> Redis:
        """
        获取Redis客户端
        
        Returns:
            redis.Redis: Redis客户端对象
        """
        if self._redis_client is None:
            raise Exception("Redis未连接")
        return self._redis_client
        
    def get_db(self) -> Database:
        """
        获取MongoDB数据库实例
        
        Returns:
            Database: MongoDB数据库实例
        """
        if self._mongo_db is None:
            raise Exception("MongoDB未连接")
        return self._mongo_db
        
    def get_collection_count(self, collection_name: str) -> int:
        """
        获取集合记录数
        
        Args:
            collection_name: 集合名称
            
        Returns:
            int: 记录数
        """
        try:
            collection = self.get_collection(collection_name)
            return collection.count_documents({})
        except Exception as e:
            logger.error(f"获取集合{collection_name}记录数失败: {str(e)}")
            return 0
            
    def get_supervision_units(self, skip: int = 0, limit: int = 100) -> List[Dict]:
        """
        获取消防监督管理系统单位数据
        
        Args:
            skip: 跳过记录数
            limit: 限制记录数
            
        Returns:
            List[Dict]: 单位数据列表
        """
        try:
            collection_name = self.mongodb_config.get('collections', {}).get('supervision_units', 'xxj_shdwjbxx')
            collection = self.get_collection(collection_name)
            
            cursor = collection.find({}).skip(skip).limit(limit)
            results = list(cursor)
            
            # 转换ObjectId为字符串，确保JSON序列化正常
            return convert_objectid_to_str(results)
            
        except Exception as e:
            logger.error(f"获取监督管理系统数据失败: {str(e)}")
            return []
            
    def get_inspection_units(self, skip: int = 0, limit: int = 100) -> List[Dict]:
        """
        获取消防隐患安全排查系统单位数据
        
        Args:
            skip: 跳过记录数
            limit: 限制记录数
            
        Returns:
            List[Dict]: 单位数据列表
        """
        try:
            collection_name = self.mongodb_config.get('collections', {}).get('inspection_units', 'xfaqpc_jzdwxx')
            collection = self.get_collection(collection_name)
            
            cursor = collection.find({}).skip(skip).limit(limit)
            results = list(cursor)
            
            # 转换ObjectId为字符串，确保JSON序列化正常
            return convert_objectid_to_str(results)
            
        except Exception as e:
            logger.error(f"获取安全排查系统数据失败: {str(e)}")
            return []
            
    def save_match_result(self, match_result: Dict) -> bool:
        """
        保存匹配结果
        
        Args:
            match_result: 匹配结果数据
            
        Returns:
            bool: 保存是否成功
        """
        try:
            collection_name = self.mongodb_config.get('collections', {}).get('match_results', 'unit_match_results')
            collection = self.get_collection(collection_name)
            
            # 添加时间戳
            match_result['created_time'] = datetime.now()
            match_result['updated_time'] = datetime.now()
            
            # 插入记录
            result = collection.insert_one(match_result)
            
            logger.debug(f"匹配结果保存成功: {result.inserted_id}")
            return True
            
        except Exception as e:
            logger.error(f"保存匹配结果失败: {str(e)}")
            return False
            
    def get_match_results(self, page: int = 1, per_page: int = 20, 
                          match_type: Optional[str] = None, 
                          search_term: Optional[str] = None) -> Dict:
        """
        获取匹配结果
        
        Args:
            page: 页码
            per_page: 每页记录数
            match_type: 匹配类型筛选
            search_term: 单位名称搜索关键词
            
        Returns:
            Dict: 分页结果
        """
        try:
            collection_name = self.mongodb_config.get('collections', {}).get('match_results', 'unit_match_results')
            collection = self.get_collection(collection_name)
            
            # 构建查询条件
            query = {}
            if match_type:
                # 智能匹配类型筛选
                if match_type == 'exact':
                    # 精确匹配：匹配所有以exact开头的类型
                    query['match_type'] = {'$regex': '^exact'}
                elif match_type == 'fuzzy':
                    # 模糊匹配：匹配所有以fuzzy开头的类型，但排除none
                    query['match_type'] = {'$regex': '^fuzzy'}
                elif match_type == 'none':
                    # 未匹配：精确匹配none
                    query['match_type'] = 'none'
                else:
                    # 其他情况：精确匹配
                    query['match_type'] = match_type
            
            if search_term:
                # 使用正则表达式进行模糊搜索，不区分大小写，同时搜索源单位和匹配单位
                query['$or'] = [
                    {'unit_name': {'$regex': search_term, '$options': 'i'}},
                    {'matched_unit_name': {'$regex': search_term, '$options': 'i'}}
                ]
                
            # 计算跳过记录数
            skip = (page - 1) * per_page
            
            # 查询数据
            cursor = collection.find(query).sort('created_time', -1).skip(skip).limit(per_page)
            results = list(cursor)
            
            # 总记录数
            total = collection.count_documents(query)
            
            # 转换ObjectId为字符串，确保JSON序列化正常
            response_data = {
                'results': convert_objectid_to_str(results),
                'total': total,
                'page': page,
                'per_page': per_page,
                'pages': (total + per_page - 1) // per_page
            }
            
            return response_data
            
        except Exception as e:
            logger.error(f"获取匹配结果失败: {str(e)}")
            return {
                'results': [],
                'total': 0,
                'page': page,
                'per_page': per_page,
                'pages': 0
            }
            
    def get_match_statistics(self) -> Dict:
        """
        获取匹配统计信息
        
        Returns:
            Dict: 统计信息
        """
        try:
            collection_name = self.mongodb_config.get('collections', {}).get('match_results', 'unit_match_results')
            collection = self.get_collection(collection_name)
            
            # 聚合统计 - 按原始类型统计
            pipeline = [
                {
                    '$group': {
                        '_id': '$match_type',
                        'count': {'$sum': 1},
                        'avg_score': {'$avg': '$similarity_score'}
                    }
                }
            ]
            
            cursor = collection.aggregate(pipeline)
            raw_stats_by_type = list(cursor)
            
            # 总统计
            total_matches = collection.count_documents({})
            
            # 重新组织统计数据，按前端需要的格式
            stats_by_type = []
            exact_count = 0
            fuzzy_count = 0 
            none_count = 0
            
            # 统计各类型数量
            for stat in raw_stats_by_type:
                match_type = stat['_id']
                count = stat['count']
                
                if match_type and match_type.startswith('exact'):
                    exact_count += count
                elif match_type and match_type.startswith('fuzzy'):
                    fuzzy_count += count
                elif match_type == 'none':
                    none_count += count
            
            # 构建前端需要的统计格式
            if exact_count > 0:
                stats_by_type.append({
                    '_id': 'exact',
                    'count': exact_count,
                    'avg_score': 1.0  # 精确匹配默认100%
                })
            
            if fuzzy_count > 0:
                # 计算模糊匹配的平均分数
                fuzzy_avg = 0
                fuzzy_total = 0
                for stat in raw_stats_by_type:
                    if stat['_id'] and stat['_id'].startswith('fuzzy'):
                        fuzzy_avg += (stat.get('avg_score', 0) or 0) * stat['count']
                        fuzzy_total += stat['count']
                
                stats_by_type.append({
                    '_id': 'fuzzy',
                    'count': fuzzy_count,
                    'avg_score': fuzzy_avg / fuzzy_total if fuzzy_total > 0 else 0
                })
            
            if none_count > 0:
                stats_by_type.append({
                    '_id': 'none',
                    'count': none_count,
                    'avg_score': 0
                })
            
            # 审核状态统计
            review_pipeline = [
                {
                    '$group': {
                        '_id': '$review_status',
                        'count': {'$sum': 1}
                    }
                }
            ]
            
            review_cursor = collection.aggregate(review_pipeline)
            review_stats = list(review_cursor)
            
            # 转换ObjectId和datetime为JSON兼容格式
            response_data = {
                'total_matches': total_matches,
                'stats_by_type': stats_by_type,
                'raw_stats_by_type': convert_objectid_to_str(raw_stats_by_type),
                'review_stats': convert_objectid_to_str(review_stats),
                'last_updated': datetime.now().isoformat()
            }
            
            return response_data
            
        except Exception as e:
            logger.error(f"获取匹配统计失败: {str(e)}")
            return {
                'total_matches': 0,
                'stats_by_type': [],
                'raw_stats_by_type': [],
                'review_stats': [],
                'last_updated': datetime.now().isoformat()
            }
            
    def set_cache(self, key: str, value: str, ttl: Optional[int] = None):
        """
        设置缓存
        
        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒）
        """
        try:
            if ttl:
                self._redis_client.setex(key, ttl, value)
            else:
                self._redis_client.set(key, value)
        except Exception as e:
            logger.error(f"设置缓存失败: {str(e)}")
            
    def get_cache(self, key: str) -> Optional[str]:
        """
        获取缓存
        
        Args:
            key: 缓存键
            
        Returns:
            Optional[str]: 缓存值
        """
        try:
            return self._redis_client.get(key)
        except Exception as e:
            logger.error(f"获取缓存失败: {str(e)}")
            return None
            
    def close(self):
        """关闭数据库连接"""
        try:
            if self._mongo_client:
                self._mongo_client.close()
                logger.info("MongoDB连接已关闭")
                
            if self._redis_client:
                self._redis_client.close()
                logger.info("Redis连接已关闭")
                
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {str(e)}")

    def connect_mongo(self, mongo_config: Dict[str, Any]):
        uri = mongo_config.get('uri', 'mongodb://localhost:27017/')
        db_name = mongo_config.get('database', 'Unit_Info')
        
        try:
            self._db_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self._db_client.server_info()
            self._db = self._db_client[db_name]
            logger.info(f"MongoDB连接成功: URI: {uri} (DB: {db_name})")
        except ConnectionFailure as e:
            logger.error(f"MongoDB连接失败: {e}")
            self._db_client = None
            self._db = None
            raise

    def connect_redis(self, redis_config: Dict[str, Any]):
        host = redis_config.get('host', 'localhost')
        port = redis_config.get('port', 6379)
        db = redis_config.get('db', 0)
        password = redis_config.get('password')
        username = redis_config.get('username')
        
        # 构建Redis连接参数
        redis_params = {
            'host': host,
            'port': port,
            'db': db,
            'decode_responses': redis_config.get('decode_responses', True)
        }
        
        # 添加认证参数（如果提供）
        if password:
            redis_params['password'] = password
        if username:
            redis_params['username'] = username
        
        self._redis_client = Redis(**redis_params)
        
        # 测试连接
        self._redis_client.ping()
        logger.info(f"Redis连接成功: {host}:{port}/{db}")
        
        return self._redis_client