# 图谱处理 - MySQL 数据提取 API

基于 Flask 的 MySQL 数据库交互 REST API，用于提取和查询关系数据库中的实体和关系数据。

## 功能特性

- 🔍 数据库元数据探测（表名、字段信息）
- 📊 分页查询实体数据
- 🔗 条件过滤的关系数据查询
- 🔐 环境变量配置数据库连接

## 快速开始

### 1. 安装依赖

```bash
pip install flask pymysql python-dotenv
```

### 2. 配置数据库

复制 `.env.example` 为 `.env` 并填入数据库信息：

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=your_database
DB_PORT=3306
```

### 3. 运行服务

```bash
python app.py
```

服务将在 `http://localhost:5000` 启动。

## API 接口

### 获取所有表名

```
GET /tables
```

**响应示例：**
```json
{
  "tables": ["entity_table", "relation_table"]
}
```

### 获取表字段信息

```
GET /tables/<table_name>/columns
```

**响应示例：**
```json
{
  "columns": [
    {"field": "实例ID", "type": "varchar(50)"},
    {"field": "实例名称", "type": "varchar(100)"}
  ]
}
```

### 获取实体数据

```
GET /tables/<table_name>/entities?batch_size=100&offset=0
```

**参数：**
- `batch_size`: 每页数量（默认 100）
- `offset`: 偏移量（默认 0）

**响应示例：**
```json
{
  "entities": [
    {"实例ID": "F00S", "实例名称": "长江", "实例类型": "河流"}
  ],
  "count": 1
}
```

### 获取关系数据

```
GET /tables/<table_name>/relations?batch_size=100&offset=0&关系名称=流向
```

**参数：**
- `batch_size`: 每页数量（默认 100）
- `offset`: 偏移量（默认 0）
- 其他参数作为过滤条件

**响应示例：**
```json
{
  "relations": [
    {
      "关系ID": "RO001",
      "起始实例ID": "F8...",
      "目标实例ID": "F0...",
      "关系名称": "流向"
    }
  ],
  "count": 1
}
```

## 项目结构

```
.
├── app.py                 # Flask 应用主文件
├── mysql_extractor.py     # MySQL 数据提取模块
├── .env                   # 数据库配置（不提交到 git）
├── .env.example           # 配置模板
└── README.md              # 项目说明
```

## 核心模块

### MySQLExtractor

数据库交互核心类，提供以下方法：

- `connect()` / `disconnect()` - 连接管理
- `get_all_tables()` - 获取所有表名
- `get_table_columns(table_name)` - 获取表结构
- `fetch_entities(table_name, batch_size, offset)` - 分页查询实体
- `fetch_relations(table_name, relation_filter, batch_size, offset)` - 条件查询关系

## 注意事项

- 确保 `.env` 文件不要提交到版本控制系统
- 生产环境建议关闭 Flask 的 debug 模式
- 根据实际数据量调整 `batch_size` 参数
