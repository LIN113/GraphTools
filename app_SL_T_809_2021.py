"""
元数据驱动的图数据迁移服务 - Flask API
根据 obj_rel_info 元数据表，自动解析关系定义并迁移到 Neo4j
"""
import sys
import json
from typing import Dict, List, Optional, Tuple
from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os

sys.path.append('.')
from mysql_extractor.mysql_extractor import MySQLExtractor
from neo4j_module.neo4j_loader import Neo4jConnector

load_dotenv()

app = Flask(__name__)

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT', 3306))
}

NEO4J_CONFIG = {
    'database': os.getenv('NEO4J_DATABASE', 'neo4j')
}


class MetadataDrivenMigration:
    """元数据驱动的图迁移控制器"""

    def __init__(self, mysql_config: Dict = None, neo4j_config: Dict = None):
        # MySQL 配置
        mysql_config = mysql_config or DB_CONFIG
        self.mysql = MySQLExtractor(**mysql_config)
        self.mysql.connect()

        # Neo4j 配置
        neo4j_config = neo4j_config or NEO4J_CONFIG
        self.neo4j = Neo4jConnector(**neo4j_config)
        self.neo4j.connect()

        # 缓存
        self.metadata_cache: Dict = {}
        self.entity_cache: Dict = {}

    def parse_obj_type_code(self, obj_type_code: str) -> str:
        """EX_WRZ → WRZ, RL_RV → RV"""
        return obj_type_code.split('_')[-1]

    def get_entity_table_name(self, obj_type_code: str) -> str:
        """EX_WRZ → ATT_WRZ_BASE"""
        suffix = self.parse_obj_type_code(obj_type_code)
        return f"ATT_{suffix}_BASE"

    def get_entity_pk_field(self, obj_type_code: str) -> str:
        """EX_WRZ → WRZ_CODE"""
        suffix = self.parse_obj_type_code(obj_type_code)
        return f"{suffix}_CODE"

    def load_relation_metadata(self, rel_table_identify: str) -> Optional[Dict]:
        """从 obj_rel_info 加载关系元数据"""
        if rel_table_identify in self.metadata_cache:
            return self.metadata_cache[rel_table_identify]

        sql = """
        SELECT obj_rel_name, start_obj_type_code, end_obj_type_code, rel_table_fields
        FROM obj_rel_info
        WHERE rel_table_identify = %s
        """
        self.mysql.cursor.execute(sql, (rel_table_identify,))
        result = self.mysql.cursor.fetchone()

        if not result:
            raise ValueError(f"关系表 '{rel_table_identify}' 在 obj_rel_info 中未找到")

        rel_table_fields = json.loads(result['rel_table_fields']) if result['rel_table_fields'] else []

        source_field = None
        target_field = None
        for field in rel_table_fields:
            field_key = field.get('fieldkey')
            field_identify = field.get('fieldIdentify')
            if field_key == 1:
                source_field = field_identify
            elif field_key == 2:
                target_field = field_identify

        if not source_field or not target_field:
            raise ValueError(f"rel_table_fields 中未找到 fieldkey=1 或 fieldkey=2 的字段定义")

        metadata = {
            'obj_rel_name': result['obj_rel_name'],
            'start_obj_type_code': result['start_obj_type_code'],
            'end_obj_type_code': result['end_obj_type_code'],
            'rel_table_fields': rel_table_fields,
            'source_field': source_field,
            'target_field': target_field
        }

        self.metadata_cache[rel_table_identify] = metadata
        return metadata

    def load_entity_table(self, entity_table: str, pk_field: str) -> Dict[str, Dict]:
        """加载实体表数据到内存缓存"""
        if entity_table in self.entity_cache:
            return self.entity_cache[entity_table]

        tables = self.mysql.get_all_tables()
        if entity_table not in tables:
            print(f"警告: 实体表 '{entity_table}' 不存在，跳过")
            return {}

        sql = f"SELECT * FROM `{entity_table}`"
        self.mysql.cursor.execute(sql)
        rows = self.mysql.cursor.fetchall()

        cache = {}
        for row in rows:
            code = row.get(pk_field)
            if code:
                cache[code] = row

        self.entity_cache[entity_table] = cache
        print(f"加载实体表 {entity_table}: {len(cache)} 条记录")
        return cache

    def migrate(self, rel_table_identify: str, batch_size: int = 2000) -> Dict:
        """执行元数据驱动的数据迁移"""
        print(f"\n{'='*60}")
        print(f"开始迁移: {rel_table_identify}")
        print(f"{'='*60}")

        # 1. 加载元数据
        print("\n[步骤1] 加载元数据...")
        metadata = self.load_relation_metadata(rel_table_identify)
        print(f"  关系名称: {metadata['obj_rel_name']}")
        print(f"  起始类型: {metadata['start_obj_type_code']}")
        print(f"  目标类型: {metadata['end_obj_type_code']}")
        print(f"  源字段: {metadata['source_field']}")
        print(f"  目标字段: {metadata['target_field']}")

        # 2. 构建实体表名并加载
        print("\n[步骤2] 加载实体表...")
        start_table = self.get_entity_table_name(metadata['start_obj_type_code'])
        end_table = self.get_entity_table_name(metadata['end_obj_type_code'])
        start_pk = self.get_entity_pk_field(metadata['start_obj_type_code'])
        end_pk = self.get_entity_pk_field(metadata['end_obj_type_code'])

        print(f"  起始实体表: {start_table} (主键: {start_pk})")
        print(f"  目标实体表: {end_table} (主键: {end_pk})")

        start_entities = self.load_entity_table(start_table, start_pk)
        end_entities = self.load_entity_table(end_table, end_pk)

        # 3. 检查关系表是否存在
        print("\n[步骤3] 检查关系表...")
        tables = self.mysql.get_all_tables()
        if rel_table_identify not in tables:
            return {
                'success': False,
                'error': f"关系表 '{rel_table_identify}' 不存在"
            }
        print(f"  关系表存在: {rel_table_identify}")

        # 4. 创建 Neo4j 约束
        print("\n[步骤4] 初始化 Neo4j Schema...")
        self.neo4j.create_unique_constraint("Entity", "id")

        # 5. 流式处理关系表
        print(f"\n[步骤5] 迁移数据...")
        offset = 0
        total_read = 0
        total_filtered = 0
        total_nodes_merged = 0
        total_rels_created = 0
        error_records = []

        while True:
            sql = f"SELECT * FROM `{rel_table_identify}` LIMIT %s OFFSET %s"
            self.mysql.cursor.execute(sql, (batch_size, offset))
            rows = self.mysql.cursor.fetchall()

            if not rows:
                break

            total_read += len(rows)

            nodes_to_merge = {}
            edges_to_create = []

            for row in rows:
                source_code = row.get(metadata['source_field'])
                target_code = row.get(metadata['target_field'])

                if not source_code or not target_code:
                    total_filtered += 1
                    continue

                source_entity = start_entities.get(source_code)
                target_entity = end_entities.get(target_code)

                if not source_entity or not target_entity:
                    total_filtered += 1
                    error_records.append({
                        'source_code': source_code,
                        'target_code': target_code,
                        'error': '实体不存在'
                    })
                    continue

                nodes_to_merge[source_code] = {
                    'id': source_code,
                    'name': source_entity.get(f"{start_pk.replace('_CODE', '_NAME')}", source_code),
                    'type': metadata['start_obj_type_code'],
                    **source_entity
                }
                nodes_to_merge[target_code] = {
                    'id': target_code,
                    'name': target_entity.get(f"{end_pk.replace('_CODE', '_NAME')}", target_code),
                    'type': metadata['end_obj_type_code'],
                    **target_entity
                }

                edge_attrs = {
                    k: v for k, v in row.items()
                    if k not in [metadata['source_field'], metadata['target_field']]
                }
                edges_to_create.append({
                    'source_id': source_code,
                    'target_id': target_code,
                    'relation_type': metadata['obj_rel_name'],
                    'attributes': edge_attrs
                })

            if nodes_to_merge:
                node_list = list(nodes_to_merge.values())
                self.neo4j.batch_merge_nodes(node_list)
                total_nodes_merged += len(node_list)

            # 初始化批次统计变量
            success_count = 0
            failed = []

            if edges_to_create:
                success_count, failed = self.neo4j.batch_merge_relationships(edges_to_create)
                total_rels_created += success_count
                error_records.extend(failed)

            print(f"  批次 offset={offset}: 读取 {len(rows)} 条, "
                  f"节点 {len(nodes_to_merge)} 条, 关系 {success_count} 条, "
                  f"失败 {len(failed)} 条")

            offset += batch_size

        result = {
            'success': True,
            'relation_table': rel_table_identify,
            'metadata': {
                'relation_name': metadata['obj_rel_name'],
                'start_type': metadata['start_obj_type_code'],
                'end_type': metadata['end_obj_type_code'],
                'start_table': start_table,
                'end_table': end_table
            },
            'stats': {
                'total_read': total_read,
                'total_filtered': total_filtered,
                'nodes_merged': total_nodes_merged,
                'relationships_created': total_rels_created,
                'errors': len(error_records)
            }
        }

        print(f"\n{'='*60}")
        print(f"迁移完成: {rel_table_identify}")
        print(f"{'='*60}")
        print(f"  总读取: {total_read} 条")
        print(f"  已过滤: {total_filtered} 条")
        print(f"  节点写入: {total_nodes_merged} 条")
        print(f"  关系写入: {total_rels_created} 条")
        print(f"  错误数: {len(error_records)} 条")

        return result

    def close(self):
        """关闭数据库连接"""
        self.mysql.disconnect()
        self.neo4j.disconnect()


# ==================== Flask 路由 ====================

@app.route('/metadata/relations', methods=['GET'])
def get_relations_list():
    """获取所有可用的关系表列表"""
    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    try:
        # 检查元数据表是否存在
        tables = mysql.get_all_tables()
        if 'obj_rel_info' not in tables:
            return jsonify({
                'success': False,
                'error': '元数据表 obj_rel_info 不存在，请检查数据库连接配置'
            }), 400

        sql = "SELECT rel_table_identify, obj_rel_name, start_obj_type_code, end_obj_type_code FROM obj_rel_info"
        mysql.cursor.execute(sql)
        results = mysql.cursor.fetchall()
        return jsonify({
            'success': True,
            'relations': results,
            'count': len(results)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'查询失败: {str(e)}'
        }), 500
    finally:
        mysql.disconnect()


@app.route('/metadata/relation', methods=['GET'])
def get_relation_metadata():
    """获取指定关系表的元数据"""
    rel_table_identify = request.args.get('rel_table_identify')
    if not rel_table_identify:
        return jsonify({'success': False, 'error': 'rel_table_identify required'}), 400

    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    try:
        sql = """
        SELECT obj_rel_name, start_obj_type_code, end_obj_type_code, rel_table_fields
        FROM obj_rel_info
        WHERE rel_table_identify = %s
        """
        mysql.cursor.execute(sql, (rel_table_identify,))
        result = mysql.cursor.fetchone()

        if not result:
            return jsonify({'success': False, 'error': f'关系表 {rel_table_identify} 不存在'}), 404

        rel_table_fields = json.loads(result['rel_table_fields']) if result['rel_table_fields'] else []

        return jsonify({
            'success': True,
            'metadata': {
                'rel_table_identify': rel_table_identify,
                'obj_rel_name': result['obj_rel_name'],
                'start_obj_type_code': result['start_obj_type_code'],
                'end_obj_type_code': result['end_obj_type_code'],
                'rel_table_fields': rel_table_fields
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'查询失败: {str(e)}'
        }), 500
    finally:
        mysql.disconnect()


@app.route('/metadata/entity-table', methods=['GET'])
def get_entity_table_info():
    """根据对象类型代码获取实体表信息"""
    obj_type_code = request.args.get('obj_type_code')
    if not obj_type_code:
        return jsonify({'success': False, 'error': 'obj_type_code required'}), 400

    # 解析表名
    suffix = obj_type_code.split('_')[-1]
    table_name = f"ATT_{suffix}_BASE"
    pk_field = f"{suffix}_CODE"

    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    try:
        tables = mysql.get_all_tables()
        if table_name not in tables:
            return jsonify({
                'success': False,
                'error': f'实体表 {table_name} 不存在',
                'expected_table': table_name
            }), 404

        columns = mysql.get_table_columns(table_name)
        # 获取记录数
        mysql.cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
        count = mysql.cursor.fetchone()['cnt']

        return jsonify({
            'success': True,
            'table_name': table_name,
            'pk_field': pk_field,
            'columns': columns,
            'record_count': count
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'查询失败: {str(e)}'
        }), 500
    finally:
        mysql.disconnect()


@app.route('/migrate/run', methods=['POST'])
def run_migration():
    """执行元数据驱动的数据迁移

    支持单个关系表或批量迁移多个关系表

    单个迁移请求:
        {"rel_table_identify": "REL_ST_RV", "batch_size": 2000}

    批量迁移请求:
        {"rel_table_identify": ["REL_ST_RV", "REL_WRZ_AD"], "batch_size": 2000}
    """
    data = request.json
    rel_table_identify = data.get('rel_table_identify')
    batch_size = data.get('batch_size', 2000)

    if not rel_table_identify:
        return jsonify({'success': False, 'error': 'rel_table_identify required'}), 400

    # 判断是单个还是批量
    is_batch = isinstance(rel_table_identify, list)

    if is_batch:
        # 批量迁移
        results = []
        total_stats = {
            'total_tables': len(rel_table_identify),
            'success_count': 0,
            'failed_count': 0,
            'total_nodes': 0,
            'total_relationships': 0
        }

        migrator = MetadataDrivenMigration()
        try:
            for rel_table in rel_table_identify:
                try:
                    result = migrator.migrate(rel_table, batch_size)
                    results.append({
                        'rel_table': rel_table,
                        'status': 'success',
                        'result': result
                    })
                    total_stats['success_count'] += 1
                    if result.get('success'):
                        total_stats['total_nodes'] += result['stats']['nodes_merged']
                        total_stats['total_relationships'] += result['stats']['relationships_created']
                except Exception as e:
                    results.append({
                        'rel_table': rel_table,
                        'status': 'failed',
                        'error': str(e)
                    })
                    total_stats['failed_count'] += 1

            return jsonify({
                'success': True,
                'mode': 'batch',
                'summary': total_stats,
                'results': results
            })
        finally:
            migrator.close()
    else:
        # 单个迁移
        migrator = MetadataDrivenMigration()
        try:
            result = migrator.migrate(rel_table_identify, batch_size)
            return jsonify(result)
        except ValueError as e:
            return jsonify({'success': False, 'error': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            migrator.close()


@app.route('/migrate/validate', methods=['POST'])
def validate_migration():
    """验证迁移前置条件

    支持单个或批量验证

    单个验证请求:
        {"rel_table_identify": "REL_ST_RV"}

    批量验证请求:
        {"rel_table_identify": ["REL_ST_RV", "REL_WRZ_AD"]}
    """
    data = request.json
    rel_table_identify = data.get('rel_table_identify')

    if not rel_table_identify:
        return jsonify({'success': False, 'error': 'rel_table_identify required'}), 400

    # 判断是单个还是批量
    is_batch = isinstance(rel_table_identify, list)

    def validate_single(rel_table, mysql):
        """验证单个关系表"""
        # 1. 检查元数据表是否存在
        tables = mysql.get_all_tables()
        if 'obj_rel_info' not in tables:
            return {
                'success': False,
                'error': '元数据表 obj_rel_info 不存在，请检查数据库连接配置'
            }

        sql = """
        SELECT obj_rel_name, start_obj_type_code, end_obj_type_code, rel_table_fields
        FROM obj_rel_info
        WHERE rel_table_identify = %s
        """
        mysql.cursor.execute(sql, (rel_table,))
        metadata = mysql.cursor.fetchone()

        if not metadata:
            return {
                'success': False,
                'error': f'关系表 {rel_table} 在 obj_rel_info 中未找到'
            }

        rel_table_fields = json.loads(metadata['rel_table_fields']) if metadata['rel_table_fields'] else []

        # 解析字段
        source_field = None
        target_field = None
        for field in rel_table_fields:
            if field.get('fieldkey') == 1:
                source_field = field.get('fieldIdentify')
            elif field.get('fieldkey') == 2:
                target_field = field.get('fieldIdentify')

        # 2. 检查表是否存在
        tables = mysql.get_all_tables()
        issues = []

        if rel_table not in tables:
            issues.append(f"关系表 {rel_table} 不存在")

        start_type = metadata['start_obj_type_code']
        end_type = metadata['end_obj_type_code']

        start_suffix = start_type.split('_')[-1]
        end_suffix = end_type.split('_')[-1]
        start_table = f"ATT_{start_suffix}_BASE"
        end_table = f"ATT_{end_suffix}_BASE"

        if start_table not in tables:
            issues.append(f"起始实体表 {start_table} 不存在")

        if end_table not in tables:
            issues.append(f"目标实体表 {end_table} 不存在")

        # 3. 检查字段是否存在
        if rel_table in tables:
            start_columns = mysql.get_table_columns(rel_table)
            column_names = [c['field'] for c in start_columns]

            if source_field and source_field not in column_names:
                issues.append(f"关系表缺少源字段 {source_field}")

            if target_field and target_field not in column_names:
                issues.append(f"关系表缺少目标字段 {target_field}")

        if issues:
            return {
                'success': False,
                'error': '验证失败',
                'issues': issues
            }

        return {
            'success': True,
            'message': '验证通过',
            'metadata': {
                'rel_table_identify': rel_table,
                'relation_name': metadata['obj_rel_name'],
                'start_type': start_type,
                'end_type': end_type,
                'start_table': start_table,
                'end_table': end_table,
                'source_field': source_field,
                'target_field': target_field
            }
        }

    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    try:
        if is_batch:
            # 批量验证
            results = []
            summary = {
                'total': len(rel_table_identify),
                'passed': 0,
                'failed': 0
            }

            for rel_table in rel_table_identify:
                try:
                    result = validate_single(rel_table, mysql)
                    results.append({
                        'rel_table': rel_table,
                        **result
                    })
                    if result['success']:
                        summary['passed'] += 1
                    else:
                        summary['failed'] += 1
                except Exception as e:
                    results.append({
                        'rel_table': rel_table,
                        'success': False,
                        'error': f'验证出错: {str(e)}'
                    })
                    summary['failed'] += 1

            return jsonify({
                'success': summary['failed'] == 0,
                'mode': 'batch',
                'summary': summary,
                'results': results
            })
        else:
            # 单个验证
            result = validate_single(rel_table_identify, mysql)
            status_code = 200 if result['success'] else 400
            return jsonify(result), status_code
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'验证过程出错: {str(e)}'
        }), 500
    finally:
        mysql.disconnect()


@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    status = {'mysql': False, 'neo4j': False, 'errors': []}

    # MySQL 检查
    try:
        mysql = MySQLExtractor(**DB_CONFIG)
        mysql.connect()
        mysql.cursor.execute("SELECT 1")
        mysql.disconnect()
        status['mysql'] = True
    except Exception as e:
        status['errors'].append(f'MySQL: {str(e)}')

    # Neo4j 检查
    try:
        neo4j = Neo4jConnector(**NEO4J_CONFIG)
        neo4j.connect()
        with neo4j.driver.session(database=NEO4J_CONFIG['database']) as session:
            session.run("RETURN 1")
        neo4j.disconnect()
        status['neo4j'] = True
    except Exception as e:
        status['errors'].append(f'Neo4j: {str(e)}')

    return jsonify({
        'status': 'healthy' if all([status['mysql'], status['neo4j']]) else 'unhealthy',
        **status
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
