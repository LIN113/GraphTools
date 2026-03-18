# 图谱数据迁移系统

MySQL 到 Neo4j 的图数据库 ETL 迁移工具，支持批量处理和容错机制。

## 项目概述

本项目实现了从 MySQL 关系数据库到 Neo4j 图数据库的数据迁移，采用模块化设计，包含数据提取、图数据写入和业务调度三个核心模块。

### 核心特性

- **批量处理**：支持大规模数据分批迁移（默认 2000 条/批）
- **容错机制**：自动记录失败的关系创建，生成错误报告
- **唯一性约束**：防止数据重复写入
- **模块化设计**：各模块职责清晰，易于维护和扩展

## 项目结构

```
图谱处理/
├── neo4j_module/           # Neo4j 图数据库交互模块
│   ├── neo4j_loader.py    # Neo4j 连接和数据写入
│   └── README.md
├── etl_controller/         # ETL 业务调度与容错模块
│   ├── etl_controller.py  # 迁移流程控制器
│   └── README.md
└── README.md              # 本文件
```

## 模块说明

### 1. Neo4j 交互模块 (`neo4j_module/`)

负责与 Neo4j 数据库的所有交互操作。

**核心功能：**
- 连接管理
- 创建唯一性约束
- 批量节点写入（UNWIND + MERGE）
- 批量关系创建（返回成功/失败统计）

详见：[neo4j_module/README.md](neo4j_module/README.md)

### 2. ETL 控制器模块 (`etl_controller/`)

整个迁移流程的调度中心。

**核心功能：**
- 节点迁移调度（循环拉取 + 批量写入）
- 关系迁移调度（含容错处理）
- 错误日志导出（CSV 格式）

详见：[etl_controller/README.md](etl_controller/README.md)

## 快速开始

### 安装依赖

```bash
pip install neo4j pymysql
```

### 使用示例

```python
from neo4j_module.neo4j_loader import Neo4jConnector
from etl_controller.etl_controller import GraphMigrationController

# 初始化连接器（需要先实现 MySQL 连接器）
neo4j = Neo4jConnector(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="your_password"
)

# 连接数据库
neo4j.connect()

# 创建控制器并执行迁移
controller = GraphMigrationController(mysql_conn, neo4j)
controller.run_node_migration()
controller.run_edge_migration(relation_filter="流向")
controller.generate_error_report("errors.csv")

# 清理
neo4j.disconnect()
```

## 工作流程

1. **节点迁移**：从 MySQL 批量读取实体数据 → 写入 Neo4j
2. **关系迁移**：从 MySQL 批量读取关系数据 → 创建 Neo4j 关系
3. **容错处理**：记录失败的关系（如节点不存在）
4. **生成报告**：导出错误日志供人工处理

## 配置说明

### Neo4j 配置

- URI: `bolt://localhost:7687`
- 用户名/密码：根据实际环境配置

### 批处理参数

- 默认批大小：2000 条/批
- 可在调用时自定义：`run_node_migration(batch_size=5000)`

## 注意事项

- 确保 Neo4j 数据库已启动
- 建议先执行节点迁移，再执行关系迁移
- 关系迁移前确保相关节点已存在
- 定期检查错误报告并处理失败记录
