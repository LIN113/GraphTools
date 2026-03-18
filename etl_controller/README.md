# ETL 业务调度与容错模块

## 模块职责

整个 ETL 流程的调度中心，串联 MySQL 数据提取和 Neo4j 写入，实现批处理和容错日志功能。

## 核心类：GraphMigrationController

### 初始化

```python
from etl_controller import GraphMigrationController

controller = GraphMigrationController(
    mysql_connector=mysql_conn,  # MySQLConnector 实例
    neo4j_connector=neo4j_conn   # Neo4jConnector 实例
)
```

### 主要接口

#### 1. run_node_migration(batch_size=2000)

执行节点数据迁移流程。

**流程：**
1. 在 Neo4j 中创建唯一性约束
2. 循环从 MySQL 拉取数据（每次 2000 条）
3. 批量写入 Neo4j
4. 直到数据读取完毕

**参数：**
- `batch_size`: 每批处理的记录数，默认 2000

```python
controller.run_node_migration(batch_size=2000)
```

#### 2. run_edge_migration(relation_filter=None, batch_size=2000)

执行关系数据迁移流程，包含容错处理。

**流程：**
1. 循环从 MySQL 拉取关系数据
2. 批量创建 Neo4j 关系
3. 收集失败记录到 `error_records`
4. 继续处理直到完成

**参数：**
- `relation_filter`: 关系类型过滤（如 "流向"）
- `batch_size`: 每批处理的记录数，默认 2000

```python
# 迁移所有关系
controller.run_edge_migration()

# 只迁移"流向"关系
controller.run_edge_migration(relation_filter="流向")
```

#### 3. generate_error_report(output_path="error_report.csv")

导出错误日志到 CSV 文件。

**参数：**
- `output_path`: 输出文件路径，默认 "error_report.csv"

```python
controller.generate_error_report("errors.csv")
```

## 容错机制

- 关系创建失败时（如节点不存在），自动记录到 `error_records`
- 不中断整体流程，继续处理后续数据
- 最后统一导出错误报告供人工处理

## 完整使用示例

```python
from mysql_connector import MySQLConnector
from neo4j_loader import Neo4jConnector
from etl_controller import GraphMigrationController

# 初始化连接器
mysql = MySQLConnector(host="localhost", user="root", password="pwd", database="graph_db")
neo4j = Neo4jConnector(uri="bolt://localhost:7687", user="neo4j", password="pwd")

mysql.connect()
neo4j.connect()

# 创建控制器
controller = GraphMigrationController(mysql, neo4j)

# 执行迁移
controller.run_node_migration()
controller.run_edge_migration(relation_filter="流向")

# 导出错误报告
controller.generate_error_report("migration_errors.csv")

# 清理
mysql.disconnect()
neo4j.disconnect()
```

## 依赖

需要配合使用：
- MySQL 数据提取模块（提供 `fetch_entities` 和 `fetch_relations` 接口）
- Neo4j 交互模块（提供数据写入接口）
