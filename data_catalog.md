# 消防数据系统主数据与元数据目录

本文档旨在根据项目的数据调用、业务重要性及数据特征，对消防监督管理系统和消防安全隐患排查系统的核心数据资产进行编目。编目遵循主数据和元数据的标准定义，为数据治理、系统维护和未来功能扩展提供清晰的数据蓝图。

---

## 1. 主数据 (Master Data)

主数据是消防安全监管业务的核心业务实体——"社会单位"的权威、一致且持久的记录。这些数据在匹配、审核、统计等多个业务流程中被反复引用，是系统所有分析和决策的基础。

### 1.1. 核心实体：消防安全单位 (Fire Safety Unit)

以下字段构成了"消防安全单位"这一核心实体的主数据。它们分布于原始的两个源数据集合中。

| 字段名 (Field Name)         | 数据类型 (Data Type) | 业务描述 (Business Description)                                   | 来源系统/集合 (Source System/Collection)                               | 备注 (Notes)                                                                      |
| --------------------------- | -------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `UNIT_NAME`                 | `String`             | 单位的完整法定名称。                                              | `xfjg_xfjdjgxx_processed`, `xfaqpc_jzdwxx_processed`                   | 核心匹配字段，质量直接影响匹配准确率。                                          |
| `CREDIT_CODE`               | `String`             | 统一社会信用代码，是单位唯一的官方身份标识。                      | `xfjg_xfjdjgxx_processed`, `xfaqpc_jzdwxx_processed`                   | 最高置信度的匹配依据。数据在导入时已从`Float`修复为`String`，避免精度丢失。 |
| `ADDRESS`                   | `String`             | 单位的注册或经营地址。                                            | `xfjg_xfjdjgxx_processed`, `xfaqpc_jzdwxx_processed`                   | 重要的辅助匹配字段，用于模糊匹配和地址相似度计算。                                  |
| `LEGAL_REPRESENTATIVE`      | `String`             | 单位的法定代表人姓名。                                            | `xfjg_xfjdjgxx_processed`, `xfaqpc_jzdwxx_processed`                   | 辅助识别字段，用于人工审核。                                                      |
| `MANAGER`                   | `String`             | 单位的消防安全管理人姓名。                                        | `xfjg_xfjdjgxx_processed`, `xfaqpc_jzdwxx_processed`                   | 辅助识别字段，用于人工审核。                                                      |
| `ID`                        | `String`             | 记录在源业务系统中的唯一标识符（非信用代码）。                  | `xfjg_xfjdjgxx_processed`, `xfaqpc_jzdwxx_processed`                   | 用于数据溯源，确保与原始系统的关联性。数据类型已从`Float`修复为`String`。   |
| `BUILDING_ID`               | `String`             | 建筑实体ID，关联具体物理建筑。                                    | `xfaqpc_jzdwxx_processed`                                              | 用于"一对多"匹配模式，关联同一建筑内的多个单位。                                |
| `COMPANY_TYPE`              | `String`             | 公司类型，如"有限责任公司"。                                      | `xfjg_xfjdjgxx_processed` (推断)                                       | 分类字段，可用于统计分析。                                                        |
| `INDUSTRY_CATEGORY`         | `String`             | 行业分类。                                                        | `xfjg_xfjdjgxx_processed` (推断)                                       | 分类字段，可用于风险评估和统计。                                                  |

---

## 2. 元数据 (Metadata)

元数据是"关于数据的数据"，它描述了主数据的结构、含义、处理过程和质量状态，是理解和管理数据资产的关键。

### 2.1. 技术元数据 (Technical Metadata)

技术元数据描述了数据的物理存储、结构和访问优化信息。

#### 2.1.1. 数据库集合 (Collections)

| 集合名称                          | 描述                                                                                              |
| --------------------------------- | ------------------------------------------------------------------------------------------------- |
| `xfjg_xfjdjgxx_processed`         | 存储来自"消防监督管理系统"的单位数据，是主要的数据源之一。                                        |
| `xfaqpc_jzdwxx_processed`         | 存储来自"消防安全隐患排查系统"的单位数据，是另一个主要数据源。                                    |
| `unit_match_results`              | 存储两个源系统单位之间两两匹配的结果，包括匹配得分、类型和审核状态。                              |
| `enhanced_association_results`    | 存储"一对多"增强关联模式的匹配结果，将一个单位与另一系统中可能相关的多个单位进行聚合。          |

#### 2.1.2. 关键索引 (Indexes)

索引是确保查询性能、避免数据库崩溃的核心技术保障。

| 集合名称                          | 索引字段                                                      | 索引类型 | 作用                                                              |
| --------------------------------- | ------------------------------------------------------------- | -------- | ----------------------------------------------------------------- |
| `xfjg_xfjdjgxx_processed`         | `CREDIT_CODE`                                                 | `Ascending` | 加速精确匹配。                                                    |
| `xfjg_xfjdjgxx_processed`         | `UNIT_NAME_TRUNC`, `UNIT_NAME_SLICE_1/2/3`, `ADDRESS_KEYWORDS` | `Ascending` | 加速基于名称和地址切片、关键词的预筛选和模糊匹配。                |
| `xfaqpc_jzdwxx_processed`         | `CREDIT_CODE`                                                 | `Ascending` | 加速精确匹配。                                                    |
| `xfaqpc_jzdwxx_processed`         | `UNIT_NAME_TRUNC`, `UNIT_NAME_SLICE_1/2/3`, `ADDRESS_KEYWORDS` | `Ascending` | 加速基于名称和地址切片、关键词的预筛选和模糊匹配。                |
| `unit_match_results`              | `unit_name`, `primary_credit_code`, `building_id`             | `Ascending` | 加速匹配结果的查询、搜索和聚合分析。                              |
| `enhanced_association_results`    | `association_id`                                              | `Unique` | 确保"一对多"关联结果的唯一性，并加速查询。                      |

### 2.2. 业务元数据 (Business Metadata)

业务元数据从业务视角描述数据，定义了数据的业务含义、规则和关系。

| 元数据项                   | 描述                                                                                                                                              |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| **数据源 (Data Source)**   | - **消防监督管理系统**: 提供权威、全面的单位注册信息。<br>- **消防安全隐患排查系统**: 提供与具体建筑绑定的、动态的现场排查信息。                    |
| **匹配逻辑 (Matching Logic)** | 定义了数据如何关联：<br>1. **精确匹配**: 基于 `CREDIT_CODE`。<br>2. **增强模糊匹配**: 综合单位核心名称、地址、拼音相似度进行加权评分。<br>3. **结构化名称匹配**: 处理单位名称中包含分公司、分店等情况。<br>4. **一对多关联**: 基于 `BUILDING_ID` 进行关联。 |
| **数据质量规则 (Quality Rule)** | - `CREDIT_CODE` 应为18位字符串。<br>- `UNIT_NAME` 不应为空或纯数字。<br>- 匹配算法中，核心名称相似度低于0.7的直接拒绝，以防止低质量匹配。 |
| **数据生命周期 (Lifecycle)** | 原始数据 -> 预处理（提取关键词、名称切片） -> 匹配（生成`unit_match_results`） -> 人工审核（更新状态） -> 统计分析 -> 归档/导出。 |

### 2.3. 操作元数据 (Operational Metadata)

操作元数据记录了数据处理和交互过程中产生的信息，主要存在于匹配结果集合中。

| 字段名 (Field Name)         | 数据类型 (Data Type) | 业务描述 (Business Description)                                   | 所在集合 (Collection)                                          | 备注 (Notes)                                                                      |
| --------------------------- | -------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `match_score`               | `Float`              | 匹配算法给出的相似度得分，范围0-1。                               | `unit_match_results`, `enhanced_association_results`             | 决定了匹配结果的置信度，是自动审批和排序的主要依据。                              |
| `match_type`                | `String`             | 匹配的类型，如"信用代码精确匹配"、"增强模糊匹配"等。            | `unit_match_results`, `enhanced_association_results`             | 解释了得分的来源，帮助理解匹配结果。                                              |
| `explanation`               | `Object`             | 对匹配过程的详细解释，包含各部分的得分贡献。                      | `unit_match_results`                                             | 用于"决策分析"模态框，为人工审核提供透明的决策依据。                              |
| `review_status`             | `String`             | 人工审核状态，包含`pending`, `approved`, `rejected`。              | `unit_match_results`, `enhanced_association_results`             | 核心工作流字段，驱动人工审核流程。                                                |
| `review_reason`             | `String`             | 人工审核时填写的理由。                                            | `unit_match_results`, `enhanced_association_results`             | 记录审核决策的依据，用于追溯和分析。                                              |
| `reviewer`                  | `String`             | 执行审核操作的用户名。                                            | `unit_match_results`, `enhanced_association_results`             | 操作审计字段。                                                                    |
| `review_timestamp`          | `DateTime`           | 审核操作发生的时间。                                              | `unit_match_results`, `enhanced_association_results`             | 操作审计字段。                                                                    |
| `source_id` / `matched_source_id` | `String`       | 匹配对中，各自记录在源系统中的 `ID`。                             | `unit_match_results`                                             | 用于数据溯源，关联回原始记录。                                                    |
| `source_system`             | `String`             | 记录来源的系统名称，如"消防监督管理系统"。                        | `unit_match_results`                                             | 明确数据归属。                                                                    |
| `association_id`            | `String`             | 在"一对多"模式下，为一组关联生成的唯一ID。                        | `enhanced_association_results`                                   | 聚合关联结果的主键。                                                              |
| `match_timestamp`           | `DateTime`           | 匹配任务执行的时间。                                              | (推断存在于日志或任务记录中)                                     | 用于监控系统性能和数据新鲜度。                                                    |

</rewritten_file> 