# 🔥 消防单位建筑数据关联系统 (Data Research System)

## 📖 项目简介

本系统旨在解决消防监督管理系统和消防隐患安全排查系统中单位建筑数据的关联匹配问题。通过智能匹配算法，实现两个系统中同一单位信息的精确识别和关联，提升数据质量和管理效率。

## 🎯 核心功能

### ✨ 精确匹配
- **单位名称匹配**：完全一致的单位名称自动关联
- **信用代码匹配**：基于统一社会信用代码的精确匹配
- **优先级机制**：信用代码优先，单位名称次之

### 🔍 智能模糊匹配
- **多维度相似度计算**
  - 法定代表人姓名相似度
  - 联系电话号码匹配
  - 消防安全管理人对比
  - 建筑面积数值匹配
  - 单位地址地理相似度

- **AI增强算法**
  - TF-IDF向量化文本相似度
  - 编辑距离(Levenshtein)算法
  - Jaro-Winkler字符串匹配
  - 中文拼音相似度计算
  - 机器学习分类器

### 🌐 Web管理界面
- **实时进度监控**：匹配任务进度可视化
- **结果管理**：匹配结果查看、审核、导出
- **参数配置**：相似度阈值、权重调整
- **数据统计**：匹配成功率、分布图表

## 🏗️ 系统架构

```
消防单位建筑数据关联系统
├── 数据源层
│   ├── 消防监督管理系统 (xxj_shdwjbxx)
│   └── 消防隐患安全排查系统 (xfaqpc_jzdwxx)
├── 业务逻辑层
│   ├── 精确匹配器 (ExactMatcher)
│   ├── 模糊匹配器 (FuzzyMatcher)
│   ├── 相似度计算器 (SimilarityCalculator)
│   └── 批处理引擎 (BatchProcessor)
├── 表示层
│   ├── Web管理界面
│   ├── REST API接口
│   └── 数据可视化
└── 数据存储层
    ├── MongoDB数据库
    └── Redis缓存系统
```

## 🚀 快速开始

### 环境要求
- Python 3.8+
- MongoDB 4.4+
- Redis 6.0+

### 安装步骤

1. **克隆项目**
```bash
git clone <repository-url>
cd Data_Research_Sys
```

2. **安装依赖**
```bash
pip install -r requirements.txt
```

3. **配置数据库**
```bash
# 修改 config/database.yaml 中的数据库连接信息
cp config/database.yaml.example config/database.yaml
```

4. **运行安装配置脚本**
```bash
python setup.py
```

5. **启动服务**
```bash
python run.py
```

6. **访问Web界面**
```
http://localhost:5000
```

## 🗄️ 数据库配置

### MongoDB连接配置

系统支持两种MongoDB连接方式：

#### 1. URI连接方式（推荐）
在 `config/database.yaml` 中配置完整的MongoDB连接URI：

```yaml
mongodb:
  # MongoDB连接URI (推荐使用，优先级高于单独的host/port配置)
  uri: "mongodb://localhost:27017/Unit_Info"
```

#### 2. 传统连接方式
```yaml
mongodb:
  host: "localhost"
  port: 27017
  database: "Unit_Info"
  # 认证配置 (可选)
  username: null
  password: null
  auth_source: null
```

#### 支持的URI格式示例
```bash
# 本地连接
mongodb://localhost:27017/Unit_Info

# 带认证的连接
mongodb://username:password@localhost:27017/Unit_Info

# 副本集连接
mongodb://host1:27017,host2:27017,host3:27017/Unit_Info?replicaSet=myReplicaSet

# 集群连接
mongodb+srv://username:password@cluster.mongodb.net/Unit_Info
```

### 连接测试
使用专门的测试脚本验证数据库连接：

```bash
# 测试数据库连接
python scripts/test_connection.py

# 或者使用Windows批处理工具
start_system.bat
```

## 📊 数据字段映射

### 精确匹配字段
| 字段含义 | 监督管理系统 | 安全排查系统 | 优先级 |
|---------|-------------|-------------|-------|
| 单位名称 | dwmc | UNIT_NAME | 2 |
| 统一社会信用代码 | tyshxydm | CREDIT_CODE | 1 |

### 模糊匹配字段
| 字段含义 | 监督管理系统 | 安全排查系统 | 权重 |
|---------|-------------|-------------|------|
| 法定代表人 | fddbr | LEGAL_PEOPLE | 0.25 |
| 法人联系电话 | frlxdh | LEGAL_TEL | 0.20 |
| 消防安全管理人 | xfaqglr | SECURITY_PEOPLE | 0.25 |
| 建筑面积 | jzmj | BUILDING_SPACE | 0.15 |
| 单位地址 | dwdz | - | 0.15 |

## 🔧 配置说明

### 匹配参数配置 (config/matching.yaml)
```yaml
fuzzy_match:
  similarity_threshold: 0.75    # 相似度阈值
  fields:
    legal_person:
      weight: 0.25             # 法人权重
    legal_tel:
      weight: 0.20             # 电话权重
    # ... 其他字段配置
```

### 性能优化配置
```yaml
batch_processing:
  batch_size: 100              # 批处理大小
  max_workers: 4               # 最大工作线程
  timeout: 300                 # 超时时间(秒)
```

## 📈 使用指南

### 1. 启动匹配任务
- 访问"匹配管理"页面
- 选择匹配类型（精确/模糊/全部）
- 设置批处理参数
- 点击"开始匹配"

### 2. 监控匹配进度
- 实时查看处理进度
- 监控匹配成功率
- 查看异常报告

### 3. 结果管理
- 查看匹配结果列表
- 筛选不同匹配类型
- 手动审核可疑匹配
- 导出Excel报告

## 📋 输出结果

匹配完成后，系统将生成 `unit_match_results` 表，包含以下关键字段：

```javascript
{
  "primary_unit_id": "安全排查系统单位ID",
  "primary_unit_name": "XX消防安全检查有限公司",
  "matched_unit_id": "监督管理系统单位ID",
  "matched_unit_name": "XX消防安全检查公司",
  "combined_unit_name": "XX消防安全检查有限公司（XX消防安全检查公司）",
  "match_type": "fuzzy",
  "similarity_score": 0.87,
  "match_details": {
    "credit_code_match": false,
    "name_similarity": 0.90,
    "legal_person_similarity": 0.85,
    "tel_similarity": 1.0
  },
  "status": "matched",
  "created_time": "2024-01-15T10:30:00Z"
}
```

## 🛠️ 开发指南

### 项目结构
```
Data_Research_Sys/
├── src/
│   ├── matching/              # 匹配算法模块
│   │   ├── exact_matcher.py   # 精确匹配
│   │   ├── fuzzy_matcher.py   # 模糊匹配
│   │   └── similarity_scorer.py # 相似度计算
│   ├── database/              # 数据库操作
│   ├── web/                   # Web应用
│   └── utils/                 # 工具函数
├── config/                    # 配置文件
├── logs/                      # 日志文件
└── docs/                      # 文档
```

### 扩展开发
1. **添加新的相似度算法**
   - 在 `similarity_scorer.py` 中添加新方法
   - 更新配置文件权重设置

2. **自定义匹配规则**
   - 继承 `BaseMatcherSSSS` 类
   - 实现自定义匹配逻辑

3. **添加新的数据源**
   - 扩展数据库模型
   - 更新字段映射配置

## 🔍 性能优化

### 数据库优化
- MongoDB索引优化
- 查询语句优化  
- 连接池配置

### 算法优化
- 批处理并行计算
- 缓存频繁计算结果
- 提前终止低相似度计算

### 内存优化
- 分批加载大数据集
- 及时释放内存
- 使用生成器处理

## 📊 监控指标

### 系统性能指标
- 匹配处理速度 (条/秒)
- 内存使用率
- CPU使用率
- 数据库响应时间

### 业务质量指标
- 精确匹配率
- 模糊匹配成功率
- 误匹配率
- 人工审核通过率

## 🚨 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查MongoDB服务状态
   - 验证连接配置
   - 检查网络连通性

2. **匹配速度慢**
   - 调整批处理大小
   - 增加工作线程数
   - 优化相似度算法

3. **内存不足**
   - 减少批处理大小
   - 清理临时缓存
   - 增加系统内存

### 日志查看
```bash
# 查看应用日志
tail -f logs/app.log

# 查看错误日志
tail -f logs/error.log

# 查看匹配日志
tail -f logs/matching.log
```

## 🤝 贡献指南

1. Fork项目
2. 创建功能分支
3. 提交代码更改
4. 推送到分支
5. 创建Pull Request

## 📄 许可证

本项目采用MIT许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 📞 技术支持

- 📧 邮箱：support@example.com
- 📱 电话：400-123-4567
- 🌐 官网：https://example.com

---

**注意**：本系统涉及敏感的消防安全数据，请确保在安全的网络环境中部署和使用，并遵守相关数据保护法规。