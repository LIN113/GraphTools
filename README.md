# 图谱数据迁移系统

MySQL 到 Neo4j 的图数据库 ETL 迁移工具，支持批量处理、REST API 和容错机制。

## 项目概述

本项目实现了从 MySQL 关系数据库到 Neo4j 图数据库的数据迁移，采用模块化设计，包含数据提取、图数据写入和业务调度三个核心模块，并提供统一的 REST API 接口。

### 核心特性

- **REST API**：统一的 HTTP 接口，方便集成和调用
- **批量处理**：支持大规模数据分批迁移（默认 2000 条/批）
- **容错机制**：自动记录失败的关系创建，生成错误报告
- **唯一性约束**：防止数据重复写入
- **模块化设计**：各模块职责清晰，易于维护和扩展

## 项目结构

```
GraphTools/
├── mysql_extractor/        # MySQL 数据提取模块
│   ├── mysql_extractor.py # MySQL 连接和数据提取
│   └── app.py             # 独立 API 服务
├── neo4j_module/          # Neo4j 图数据库交互模块
│   └── neo4j_loader.py    # Neo4j 连接和数据写入
├── etl_controller/        # ETL 业务调度与容错模块
│   └── etl_controller.py  # 迁移流程控制器
├── app.py                 # 统一 API 入口
└── README.md
```

## 模块说明

### 1. MySQL 提取模块 (`mysql_extractor/`)

负责从 MySQL 数据库提取数据。

**核心功能：**
- 获取表结构和元数据
- 分批提取实体和关系数据
- 数据验证和类型查询

### 2. Neo4j 交互模块 (`neo4j_module/`)

负责与 Neo4j 数据库的所有交互操作。

**核心功能：**
- 连接管理
- Schema 初始化（约束和索引）
- 批量节点写入（UNWIND + MERGE）
- 批量关系创建（返回成功/失败统计）

### 3. ETL 控制器模块 (`etl_controller/`)

整个迁移流程的调度中心。

**核心功能：**
- 节点迁移调度（循环拉取 + 批量写入）
- 关系迁移调度（含容错处理）
- 错误日志导出（CSV 格式）

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

创建 `.env` 文件：

```env
# MySQL 配置
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=your_database
DB_PORT=3306

# Neo4j 配置
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password
```

### 3. 启动服务

```bash
python app.py
```

服务将在 `http://localhost:5000` 启动。

## 完整数据迁移流程

### 步骤 1：查看 MySQL 数据结构

```bash
# 获取所有表
curl http://localhost:5000/mysql/tables

# 查看实体表结构
curl "http://localhost:5000/mysql/columns?table_name=entities"

# 查看关系表结构
curl "http://localhost:5000/mysql/columns?table_name=relations"
```

### 步骤 2：验证数据完整性

```bash
# 验证实体表必需字段
curl "http://localhost:5000/mysql/check?table_name=entities&required_columns=id,name,type"

# 获取实体类型列表
curl "http://localhost:5000/mysql/entity-types?table_name=entities&type_column=type"
```

### 步骤 3：初始化 Neo4j Schema

```bash
curl -X POST http://localhost:5000/neo4j/init-schema \
  -H "Content-Type: application/json" \
  -d '{
    "entity_types": ["Person", "Company", "Product"],
    "unique_id_key": "id",
    "index_keys": ["name", "type"]
  }'
```

### 步骤 4：执行节点迁移

```bash
curl -X POST http://localhost:5000/etl/migrate-nodes \
  -H "Content-Type: application/json" \
  -d '{
    "entity_table": "entities",
    "batch_size": 2000
  }'
```

### 步骤 5：执行关系迁移

```bash
curl -X POST http://localhost:5000/etl/migrate-edges \
  -H "Content-Type: application/json" \
  -d '{
    "relation_table": "relations",
    "batch_size": 2000
  }'
```

如果有失败记录，会生成 `error_report.csv` 文件。

### 步骤 6：验证迁移结果

通过 REST API 查看数据库元数据：

```bash
# 获取所有标签、关系类型和属性键
curl http://localhost:5000/neo4j/schema
```

返回示例：
```json
{
  "labels": ["Entity", "User", "Product"],
  "relationship_types": ["PURCHASED", "FOLLOWS"],
  "property_keys": ["id", "name", "type", "age", "price"]
}
```

在 Neo4j Browser 中执行：

```cypher
// 查看节点数量
MATCH (n) RETURN count(n)

// 查看关系数量
MATCH ()-[r]->() RETURN count(r)

// 查看数据样例
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 10
```

## 快捷方式：一键完整迁移

如果你想一次性完成所有迁移步骤，可以使用一键迁移接口：

```bash
curl -X POST http://localhost:5000/etl/migrate-all \
  -H "Content-Type: application/json" \
  -d '{
    "entity_table": "entities",
    "relation_table": "relations",
    "batch_size": 2000,
    "id_field": "实例ID",
    "name_field": "实例名称",
    "init_schema": true,
    "entity_types": ["Person", "Company", "Product"],
    "unique_id_key": "id",
    "index_keys": ["name", "type"],
    "field_mapping": {
      "source_id": "起始实例ID",
      "target_id": "目标实例ID",
      "relation_type": "关系名称"
    }
  }'
```

**参数说明：**

| 参数 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `entity_table` | 是 | - | 实体表名 |
| `relation_table` | 是 | - | 关系表名 |
| `batch_size` | 否 | 2000 | 批次大小 |
| `id_field` | 否 | "id" | 实体表的ID字段名 |
| `name_field` | 否 | "name" | 实体表的名称字段名 |
| `init_schema` | 否 | false | 是否初始化 schema |
| `field_mapping` | 否 | 见下方 | 关系表字段映射 |

**field_mapping 默认值：**
```json
{
  "source_id": "source_id",
  "target_id": "target_id",
  "relation_type": "relation_type"
}
```

该接口会自动完成：Schema 初始化 → 节点迁移 → 关系迁移 → 错误报告生成。

**返回结果示例：**
```json
{
  "success": true,
  "nodes": {
    "total_read": 1000,
    "total_filtered": 50,
    "total_written": 950
  },
  "edges": {
    "total_read": 2000,
    "total_filtered": 100,
    "total_written": 1850,
    "total_failed": 50
  },
  "error_report": "error_report.csv"
}
```

## API 接口文档

### MySQL 数据提取

| 接口 | 方法 | 说明 |
|------|------|------|
| `/mysql/tables` | GET | 获取所有表名 |
| `/mysql/columns` | GET | 获取表字段信息 |
| `/mysql/entities` | GET | 分批获取实体数据 |
| `/mysql/relations` | GET | 分批获取关系数据 |
| `/mysql/check` | GET | 验证表结构 |
| `/mysql/entity-types` | GET | 获取实体类型列表 |

### Neo4j 操作

| 接口 | 方法 | 说明 |
|------|------|------|
| `/neo4j/init-schema` | POST | 初始化约束和索引 |
| `/neo4j/load-nodes` | POST | 批量加载节点 |
| `/neo4j/load-relationships` | POST | 批量加载关系 |
| `/neo4j/schema` | GET | 获取数据库元数据（标签、关系类型、属性键） |

### ETL 迁移

| 接口 | 方法 | 说明 |
|------|------|------|
| `/etl/migrate-nodes` | POST | 自动迁移节点 |
| `/etl/migrate-edges` | POST | 自动迁移关系 |
| `/etl/migrate-all` | POST | 一键完整迁移（节点+关系） |

## 注意事项

- 确保 MySQL 和 Neo4j 数据库已启动
- **Neo4j 需要安装 APOC 插件**（用于动态关系类型创建）
  - 下载 APOC jar 文件到 `plugins` 目录
  - 在 `neo4j.conf` 添加：`dbms.security.procedures.unrestricted=apoc.*`
  - 重启 Neo4j
- 必须先执行节点迁移，再执行关系迁移
- 关系迁移前确保相关节点已存在
- 定期检查 `error_report.csv` 并处理失败记录
- 大数据量迁移建议调整 `batch_size` 参数
- 使用 `id_field` 和 `name_field` 参数适配不同的表结构
- 使用 `field_mapping` 参数映射关系表字段名

