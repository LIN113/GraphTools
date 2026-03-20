# 元数据驱动的图数据迁移服务

基于 `obj_rel_info` 元数据表，自动解析关系定义并将 MySQL 数据迁移到 Neo4j 图数据库的 Flask RESTful API 服务。

---

## 功能特点

- **元数据驱动**：通过 `obj_rel_info` 表自动解析关系定义，无需手动配置字段映射
- **动态表名解析**：根据对象类型代码自动构建实体表名（如 `EX_WRZ` → `ATT_WRZ_BASE`）
- **内存缓存**：实体数据自动缓存到内存，提升查询性能
- **流式处理**：关系表数据分批处理，支持大数据量迁移
- **友好异常处理**：完善的错误提示，便于问题排查

---

## 环境要求

- Python 3.8+
- MySQL 5.7+
- Neo4j 4.4+ (需安装 APOC 插件)

---

## 依赖安装

```bash
pip install flask pymysql python-dotenv neo4j
```

---

## 配置说明

在项目根目录创建 `.env` 文件：

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
NEO4J_DATABASE=neo4j
```

---

## 数据结构说明

### 元数据表 (obj_rel_info)

| 字段 | 说明 |
|-----|------|
| `rel_table_identify` | 关系表标识（如 `REL_WRZ_AD`） |
| `obj_rel_name` | 关系类型名称（如 "关联"） |
| `start_obj_type_code` | 起始对象类型（如 `EX_WRZ`） |
| `end_obj_type_code` | 目标对象类型（如 `RL_RV`） |
| `rel_table_fields` | 字段定义 JSON（fieldkey: 1=源, 2=目标） |

### 实体表命名规则

```
对象类型代码 → 实体表名
EX_WRZ       → ATT_WRZ_BASE
RL_RV        → ATT_RV_BASE
MS_ST        → ATT_ST_BASE
```

### 主键字段命名规则

```
对象类型代码 → 主键字段
EX_WRZ       → WRZ_CODE
RL_RV        → RV_CODE
```

---

## 启动服务

```bash
python app_SL_T_809_2021.py
```

服务运行在 `http://localhost:5002`

---

## API 接口文档

### 1. 健康检查

```
GET /health
```

**响应示例：**
```json
{
  "status": "healthy",
  "mysql": true,
  "neo4j": true,
  "errors": []
}
```

---

### 2. 获取所有关系表列表

```
GET /metadata/relations
```

**响应示例：**
```json
{
  "success": true,
  "relations": [
    {
      "rel_table_identify": "REL_WRZ_AD",
      "obj_rel_name": "关联",
      "start_obj_type_code": "EX_WRZ",
      "end_obj_type_code": "EX_AD"
    }
  ],
  "count": 1
}
```

---

### 3. 获取指定关系表元数据

```
GET /metadata/relation?rel_table_identify=REL_WRZ_AD
```

**响应示例：**
```json
{
  "success": true,
  "metadata": {
    "rel_table_identify": "REL_WRZ_AD",
    "obj_rel_name": "关联",
    "start_obj_type_code": "EX_WRZ",
    "end_obj_type_code": "EX_AD",
    "rel_table_fields": [
      {
        "fieldkey": 1,
        "fieldName": "水资源分区编码",
        "fieldIdentify": "WRZ_CODE"
      },
      {
        "fieldkey": 2,
        "fieldName": "行政区划编码",
        "fieldIdentify": "AD_CODE"
      }
    ]
  }
}
```

---

### 4. 获取实体表信息

```
GET /metadata/entity-table?obj_type_code=EX_WRZ
```

**响应示例：**
```json
{
  "success": true,
  "table_name": "ATT_WRZ_BASE",
  "pk_field": "WRZ_CODE",
  "columns": [
    {"field": "WRZ_CODE", "type": "varchar(50)"},
    {"field": "WRZ_NAME", "type": "varchar(100)"}
  ],
  "record_count": 1500
}
```

---

### 5. 验证迁移前置条件

```
POST /migrate/validate
Content-Type: application/json

{
  "rel_table_identify": "REL_WRZ_AD"
}
```

**成功响应：**
```json
{
  "success": true,
  "message": "验证通过",
  "metadata": {
    "rel_table_identify": "REL_WRZ_AD",
    "relation_name": "关联",
    "start_type": "EX_WRZ",
    "end_type": "EX_AD",
    "start_table": "ATT_WRZ_BASE",
    "end_table": "ATT_AD_BASE",
    "source_field": "WRZ_CODE",
    "target_field": "AD_CODE"
  }
}
```

**失败响应：**
```json
{
  "success": false,
  "error": "验证失败",
  "issues": [
    "关系表 REL_WRZ_AD 不存在",
    "起始实体表 ATT_WRZ_BASE 不存在"
  ]
}
```

---

### 6. 执行数据迁移

```
POST /migrate/run
Content-Type: application/json

{
  "rel_table_identify": "REL_ST_RV",
  "batch_size": 2000
}
```

**响应示例：**
```json
{
  "success": true,
  "relation_table": "REL_ST_RV",
  "metadata": {
    "relation_name": "所属",
    "start_type": "MS_ST",
    "end_type": "RL_RV",
    "start_table": "ATT_ST_BASE",
    "end_table": "ATT_RV_BASE"
  },
  "stats": {
    "total_read": 5000,
    "total_filtered": 10,
    "nodes_merged": 4500,
    "relationships_created": 4990,
    "errors": 0
  }
}
```

---

## 数据流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                     obj_rel_info (元数据中心)                     │
├─────────────────────────────────────────────────────────────────┤
│ rel_table_identify  →  REL_ST_RV                                │
│ obj_rel_name        →  "所属"                                    │
│ rel_table_fields    →  [{"fieldkey": 1, "fieldIdentify": "ST_CODE"},│
│                          {"fieldkey": 2, "fieldIdentify": "RV_CODE"}]│
│ start_obj_type_code →  MS_ST  →  ATT_ST_BASE                     │
│ end_obj_type_code   →  RL_RV  →  ATT_RV_BASE                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              REL_ST_RV (关系事实表)                               │
├─────────────────────────────────────────────────────────────────┤
│ ST_CODE            RV_CODE                                       │
│ ----------------   -----------                                  │
│ 10912000           EA4GD000000P                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
        ┌─────────────────────┴─────────────────────┐
        ↓                                           ↓
┌───────────────┐                           ┌───────────────┐
│ ATT_ST_BASE   │                           │ ATT_RV_BASE   │
├───────────────┤                           ├───────────────┤
│ ST_CODE (PK)  │                           │ RV_CODE (PK)  │
│ ST_NAME       │                           │ RV_NAME       │
│ ...           │                           │ RV_WIDTH      │
│               │                           │ RV_GRAD       │
└───────────────┘                           └───────────────┘
        ↓                                           ↓
        └───────────────────┬───────────────────────┘
                            ↓
                    ┌───────────────┐
                    │    Neo4j      │
                    ├───────────────┤
                    │ (st:Entity)   │
                    │   -[:所属]->   │
                    │   (rv:Entity) │
                    └───────────────┘
```

---

## 使用示例

### 完整迁移流程

```bash
# 1. 检查服务健康状态
curl http://localhost:5002/health

# 2. 查看所有可用关系表
curl http://localhost:5002/metadata/relations

# 3. 查看指定关系表的元数据
curl "http://localhost:5002/metadata/relation?rel_table_identify=REL_ST_RV"

# 4. 验证迁移条件
curl -X POST http://localhost:5002/migrate/validate \
  -H "Content-Type: application/json" \
  -d '{"rel_table_identify": "REL_ST_RV"}'

# 5. 执行迁移
curl -X POST http://localhost:5002/migrate/run \
  -H "Content-Type: application/json" \
  -d '{"rel_table_identify": "REL_ST_RV", "batch_size": 2000}'
```

---

## 错误处理

| 错误码 | 说明 |
|-------|------|
| 400 | 请求参数错误或验证失败 |
| 404 | 资源不存在（如关系表未找到） |
| 500 | 服务器内部错误（如数据库连接失败） |

**错误响应示例：**
```json
{
  "success": false,
  "error": "元数据表 obj_rel_info 不存在，请检查数据库连接配置"
}
```

---

## 注意事项

1. **Neo4j APOC 插件**：Neo4j 必须安装 APOC 插件才能支持动态关系类型
2. **内存缓存**：实体表数据会加载到内存，建议单表记录数 < 10万
3. **批处理大小**：默认 2000，可根据服务器性能调整
4. **数据库连接**：确保 `.env` 中的数据库配置正确

---

## 目录结构

```
图谱处理/
├── app_SL_T_809_2021.py          # Flask 服务主文件
├── mysql_extractor/
│   └── mysql_extractor.py        # MySQL 数据提取模块
├── neo4j_module/
│   └── neo4j_loader.py          # Neo4j 数据加载模块
├── .env                          # 环境配置文件
└── README_SL_T_809_2021.md      # 本文档
```
