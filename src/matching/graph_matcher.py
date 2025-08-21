import networkx as nx
from pymongo.database import Database
import numpy as np
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class GraphMatcher:
    """
    使用图论方法进行实体匹配。
    通过构建实体-属性关系图，利用共享的邻居节点（如地址、法人）来发现实体间的深层联系。
    """
    def __init__(self, db: Database, config: Dict = None):
        """
        初始化GraphMatcher

        Args:
            db (Database): MongoDB数据库实例
            config (Dict, optional): 包含权重的配置字典
        """
        if db is None:
            raise ValueError("Database instance 'db' is required.")
        self.db = db
        self.config = config.get('graph_matching', {}) if config else {}
        self.weights = self.config.get('attribute_weights', {
            'address': 0.6,
            'legal_person': 1.0
        })
        self.graph = nx.Graph()
        self._is_built = False

    def build_graph(self, limit: int = 0):
        """
        从数据库构建实体-属性关系图。
        该方法将单位和其关键属性（如地址、法人）作为节点添加到图中。

        Args:
            limit (int, optional): 从每个集合中加载的最大记录数。0表示无限制。默认为0。
        """
        if self._is_built:
            print("Graph has already been built.")
            return

        print(f"Building entity-attribute graph (limit: {limit} per collection)...")
        
        # 【关键修复】添加批处理和连接管理
        batch_size = 1000  # 每批处理1000条记录
        processed_count = 0
        
        try:
            # 定义查询参数
            query_filter = {}
            projection = {"_id": 1, "UNIT_NAME": 1, "UNIT_ADDRESS": 1, "LEGAL_PEOPLE": 1}
            
            # 1. 批量加载 消防隐患安全排查系统 (xfaqpc_jzdwxx)
            print("Processing xfaqpc_jzdwxx collection...")
            collection = self.db.xfaqpc_jzdwxx
            
            # 计算实际需要处理的数量
            actual_limit = limit if limit > 0 else collection.count_documents(query_filter)
            
            for skip in range(0, actual_limit, batch_size):
                current_batch_size = min(batch_size, actual_limit - skip)
                
                # 批量查询
                cursor = collection.find(query_filter, projection).skip(skip).limit(current_batch_size)
                
                batch_count = 0
                for unit in cursor:
                    self.add_unit_to_graph(unit, 'xfaqpc', 
                                           name_field='UNIT_NAME', 
                                           address_field='UNIT_ADDRESS', 
                                           person_field='LEGAL_PEOPLE')
                    batch_count += 1
                
                processed_count += batch_count
                print(f"  Processed {processed_count}/{actual_limit} xfaqpc records...")
                
                # 如果批次不满，说明已经处理完了
                if batch_count < current_batch_size:
                    break

            # 2. 批量加载 消防监督管理系统 (xxj_shdwjbxx)
            print("Processing xxj_shdwjbxx collection...")
            projection_xxj = {"_id": 1, "dwmc": 1, "dwdz": 1, "fddbr": 1}
            collection_xxj = self.db.xxj_shdwjbxx
            
            # 计算实际需要处理的数量
            actual_limit_xxj = limit if limit > 0 else collection_xxj.count_documents(query_filter)
            processed_count_xxj = 0
            
            for skip in range(0, actual_limit_xxj, batch_size):
                current_batch_size = min(batch_size, actual_limit_xxj - skip)
                
                # 批量查询
                cursor = collection_xxj.find(query_filter, projection_xxj).skip(skip).limit(current_batch_size)
                
                batch_count = 0
                for unit in cursor:
                    self.add_unit_to_graph(unit, 'xxj', 
                                           name_field='dwmc', 
                                           address_field='dwdz', 
                                           person_field='fddbr')
                    batch_count += 1
                
                processed_count_xxj += batch_count
                print(f"  Processed {processed_count_xxj}/{actual_limit_xxj} xxj records...")
                
                # 如果批次不满，说明已经处理完了
                if batch_count < current_batch_size:
                    break

            self._is_built = True
            print(f"Graph built successfully with {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges.")
            
        except Exception as e:
            print(f"Error building graph: {str(e)}")
            logger.error(f"图结构构建失败: {str(e)}")
            # 即使失败也标记为已构建，避免重复尝试
            self._is_built = True

    def get_shared_attributes(self, source_unit_id: str, target_unit_id: str) -> list:
        """
        查找两个单位节点之间共享的属性节点。

        Args:
            source_unit_id (str): 源单位的图节点ID。
            target_unit_id (str): 目标单位的图节点ID。

        Returns:
            list: 共享属性节点的列表。
        """
        if not self._is_built:
            self.build_graph()
            
        try:
            source_neighbors = set(self.graph.neighbors(source_unit_id))
            target_neighbors = set(self.graph.neighbors(target_unit_id))
            
            shared_neighbors = source_neighbors.intersection(target_neighbors)
            return list(shared_neighbors)
        except nx.NetworkXError:
            # 如果节点不存在，返回空列表
            return []

    def calculate_graph_score(self, source_unit: dict, target_unit: dict) -> float:
        """
        根据共享属性计算两个单位的图匹配分数。
        分数 = Σ (属性基础权重 * 稀有度权重)

        Args:
            source_unit (dict): 源单位的完整文档。
            target_unit (dict): 目标单位的完整文档。

        Returns:
            float: 基于图的相似度分数（0.0到1.0之间）。
        """
        import numpy as np # 在方法内部导入
        
        # 构造节点ID
        source_id = f"xfaqpc_{source_unit['_id']}"
        target_id = f"xxj_{target_unit['_id']}"

        shared_attributes = self.get_shared_attributes(source_id, target_id)
        
        if not shared_attributes:
            return 0.0
        
        total_score = 0.0
        for attr_node in shared_attributes:
            try:
                # 获取属性节点的类型和权重
                node_data = self.graph.nodes[attr_node]
                attr_type = node_data.get('type')
                base_weight = self.weights.get(attr_type, 0.1) # 对于未知类型给一个很小的默认权重
                
                # 计算稀有度权重 (degree越低，越稀有，权重越高)
                # 使用对数函数来平滑度的影响，避免极단值
                degree = self.graph.degree[attr_node]
                rarity_weight = 1 / (1 + np.log1p(degree)) # log1p(x) = log(1+x)
                
                # 计算此共享属性的最终得分
                attr_score = base_weight * rarity_weight
                total_score += attr_score

                logger.debug(
                    f"共享属性: {attr_node}, 类型: {attr_type}, "
                    f"基础权重: {base_weight:.2f}, 度: {degree}, "
                    f"稀有度权重: {rarity_weight:.2f}, 得分: {attr_score:.2f}"
                )

            except (KeyError, nx.NetworkXError):
                # 节点或其属性在图中不存在，跳过
                continue
        
        # 使用sigmoid函数进行归一化，将得分映射到(0, 1)区间
        # 这种方式比简单的裁剪更平滑
        normalized_score = 1 / (1 + np.exp(-total_score))
        
        return normalized_score

    def add_unit_to_graph(self, unit_doc: dict, collection_prefix: str, 
                          name_field: str, address_field: str, person_field: str):
        """
        将单个单位及其属性动态添加到图中。

        Args:
            unit_doc (dict): 从数据库获取的单位文档。
            collection_prefix (str): 集合前缀（如 'xfaqpc' 或 'xxj'）。
            name_field (str): 单位名称的字段名。
            address_field (str): 地址的字段名。
            person_field (str): 法定代表人的字段名。
        """
        unit_id = f"{collection_prefix}_{unit_doc['_id']}"
        self.graph.add_node(unit_id, type='unit', name=unit_doc.get(name_field))
        
        # 添加地址节点和边
        address = unit_doc.get(address_field)
        if address and isinstance(address, str):
            address_node = f"addr_{address.strip()}"
            self.graph.add_node(address_node, type='address')
            self.graph.add_edge(unit_id, address_node)
        
        # 添加法人节点和边
        legal_person = unit_doc.get(person_field)
        if legal_person and isinstance(legal_person, str):
            legal_person_node = f"person_{legal_person.strip()}"
            self.graph.add_node(legal_person_node, type='legal_person')
            self.graph.add_edge(unit_id, legal_person_node) 