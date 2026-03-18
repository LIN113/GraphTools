import pymysql
from typing import List, Dict, Optional

class MySQLExtractor:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port
        }
        self.connection = None
        self.cursor = None

    def connect(self):
        """建立数据库连接"""
        self.connection = pymysql.connect(**self.config)
        self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)

    def disconnect(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        self.cursor.execute("SHOW TABLES")
        return [list(row.values())[0] for row in self.cursor.fetchall()]

    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """获取表的所有字段及类型"""
        self.cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        return [{'field': row['Field'], 'type': row['Type']} for row in self.cursor.fetchall()]

    def fetch_entities(self, table_name: str, batch_size: int = 100, offset: int = 0) -> List[Dict]:
        """分批读取实体数据"""
        sql = f"SELECT * FROM `{table_name}` LIMIT %s OFFSET %s"
        self.cursor.execute(sql, (batch_size, offset))
        return self.cursor.fetchall()

    def fetch_relations(self, table_name: str, relation_filter: Optional[Dict] = None,
                       batch_size: int = 100, offset: int = 0) -> List[Dict]:
        """分批读取关系数据"""
        sql = f"SELECT * FROM `{table_name}`"
        params = []

        if relation_filter:
            conditions = [f"`{k}` = %s" for k in relation_filter.keys()]
            sql += " WHERE " + " AND ".join(conditions)
            params.extend(relation_filter.values())

        sql += " LIMIT %s OFFSET %s"
        params.extend([batch_size, offset])

        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def check_metadata(self, table_name: str, required_columns: List[str]) -> bool:
        """健康检查：确认表是否存在及是否包含必需字段"""
        tables = self.get_all_tables()
        if table_name not in tables:
            raise ValueError(f"表 '{table_name}' 不存在")

        columns = self.get_table_columns(table_name)
        column_names = [col['field'] for col in columns]

        missing = [col for col in required_columns if col not in column_names]
        if missing:
            raise ValueError(f"表 '{table_name}' 缺少必需字段: {missing}")

        return True

    def get_distinct_entity_types(self, table_name: str, type_column_name: str) -> List[str]:
        """从实体表中去重查询所有实例类型"""
        sql = f"SELECT DISTINCT `{type_column_name}` FROM `{table_name}` WHERE `{type_column_name}` IS NOT NULL"
        self.cursor.execute(sql)
        return [row[type_column_name] for row in self.cursor.fetchall()]
