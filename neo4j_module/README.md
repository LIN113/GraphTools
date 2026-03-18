# Neo4j 图数据库交互模块

## 模块职责

专注处理与 Neo4j 的交互，负责数据写入和索引创建。假定传入的数据已经过清洗处理。

## 核心类：Neo4jConnector

### 初始化

```python
from neo4j_loader import Neo4jConnector

connector = Neo4jConnector(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="your_password"
)
```

### 主要接口

#### 1. connect() / disconnect()

管理 Neo4j Driver 的连接生命周期。

```python
connector.connect()
# ... 执行操作
connector.disconnect()
```

#### 2. create_unique_constraint(label, property_name)

为指定标签和属性创建唯一性约束，防止重复写入。

**参数：**
- `label`: 节点标签（如 "河流"）
- `property_name`: 属性名（如 "id"）

```python
connector.create_unique_constraint("Entity", "id")
```

#### 3. batch_merge_nodes(node_list)

批量插入/更新节点，使用 UNWIND + MERGE 语句。

**参数：**
- `node_list`: 节点字典列表，每个字典必须包含 `id` 字段

```python
nodes = [
    {"id": "R001", "name": "长江", "type": "河流"},
    {"id": "R002", "name": "黄河", "type": "河流"}
]
connector.batch_merge_nodes(nodes)
```

#### 4. batch_merge_relationships(edge_list)

批量创建关系，返回统计信息和失败记录。

**参数：**
- `edge_list`: 边字典列表，每个字典包含 `source_id`, `target_id`, `relation_type`

**返回：**
- `success_count`: 成功创建的关系数量
- `failed_edges`: 失败的边列表（通常因节点不存在）

```python
edges = [
    {"source_id": "R001", "target_id": "L001", "relation_type": "流向"},
    {"source_id": "R002", "target_id": "L002", "relation_type": "流向"}
]
success, failed = connector.batch_merge_relationships(edges)
print(f"成功: {success}, 失败: {len(failed)}")
```

## 依赖

```bash
pip install neo4j
```

## 使用示例

```python
connector = Neo4jConnector("bolt://localhost:7687", "neo4j", "password")
connector.connect()

# 创建约束
connector.create_unique_constraint("Entity", "id")

# 批量写入节点
nodes = [{"id": "001", "name": "实体1"}]
connector.batch_merge_nodes(nodes)

# 批量创建关系
edges = [{"source_id": "001", "target_id": "002", "relation_type": "关联"}]
success, failed = connector.batch_merge_relationships(edges)

connector.disconnect()
```
