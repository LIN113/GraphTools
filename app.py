from flask import Flask, jsonify, request
from mysql_extractor.mysql_extractor import MySQLExtractor
from neo4j_module.neo4j_loader import Neo4jConnector
from dotenv import load_dotenv
import os

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

# MySQL 数据提取接口
@app.route('/mysql/tables', methods=['GET'])
def get_tables():
    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    tables = extractor.get_all_tables()
    extractor.disconnect()
    return jsonify({'tables': tables})

@app.route('/mysql/columns', methods=['GET'])
def get_columns():
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({'error': 'table_name required'}), 400

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    columns = extractor.get_table_columns(table_name)
    extractor.disconnect()
    return jsonify({'columns': columns})

@app.route('/mysql/entities', methods=['GET'])
def get_entities():
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({'error': 'table_name required'}), 400

    batch_size = request.args.get('batch_size', 100, type=int)
    offset = request.args.get('offset', 0, type=int)

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    entities = extractor.fetch_entities(table_name, batch_size, offset)
    extractor.disconnect()
    return jsonify({'entities': entities, 'count': len(entities)})

@app.route('/mysql/relations', methods=['GET'])
def get_relations():
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({'error': 'table_name required'}), 400

    batch_size = request.args.get('batch_size', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    relation_filter = {k: v for k, v in request.args.items()
                      if k not in ['table_name', 'batch_size', 'offset']}

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    relations = extractor.fetch_relations(table_name, relation_filter or None, batch_size, offset)
    extractor.disconnect()
    return jsonify({'relations': relations, 'count': len(relations)})

@app.route('/mysql/check', methods=['GET'])
def check_metadata():
    table_name = request.args.get('table_name')
    required_columns = request.args.get('required_columns')

    if not table_name or not required_columns:
        return jsonify({'error': 'table_name and required_columns required'}), 400

    columns_list = [col.strip() for col in required_columns.split(',')]

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    try:
        extractor.check_metadata(table_name, columns_list)
        extractor.disconnect()
        return jsonify({'valid': True})
    except ValueError as e:
        extractor.disconnect()
        return jsonify({'valid': False, 'error': str(e)}), 400

@app.route('/mysql/entity-types', methods=['GET'])
def get_entity_types():
    table_name = request.args.get('table_name')
    type_column = request.args.get('type_column')

    if not table_name or not type_column:
        return jsonify({'error': 'table_name and type_column required'}), 400

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    entity_types = extractor.get_distinct_entity_types(table_name, type_column)
    extractor.disconnect()
    return jsonify({'entity_types': entity_types})

# Neo4j 操作接口
@app.route('/neo4j/init-schema', methods=['POST'])
def init_schema():
    data = request.json
    entity_types = data.get('entity_types', [])
    unique_id_key = data.get('unique_id_key', 'id')
    index_keys = data.get('index_keys', [])

    neo4j = Neo4jConnector()
    neo4j.connect()
    neo4j.init_schema(entity_types, unique_id_key, index_keys)
    neo4j.disconnect()
    return jsonify({'success': True})

@app.route('/neo4j/load-nodes', methods=['POST'])
def load_nodes():
    nodes = request.json.get('nodes', [])
    if not nodes:
        return jsonify({'error': 'nodes required'}), 400

    neo4j = Neo4jConnector()
    neo4j.connect()
    neo4j.batch_merge_nodes(nodes)
    neo4j.disconnect()
    return jsonify({'success': True, 'count': len(nodes)})

@app.route('/neo4j/load-relationships', methods=['POST'])
def load_relationships():
    edges = request.json.get('edges', [])
    if not edges:
        return jsonify({'error': 'edges required'}), 400

    neo4j = Neo4jConnector()
    neo4j.connect()
    success_count, failed_edges = neo4j.batch_merge_relationships(edges)
    neo4j.disconnect()
    return jsonify({'success_count': success_count, 'failed_count': len(failed_edges)})

@app.route('/neo4j/schema', methods=['GET'])
def get_schema():
    neo4j = Neo4jConnector()
    neo4j.connect()
    schema = neo4j.get_schema()
    neo4j.disconnect()
    return jsonify(schema)

# ETL 迁移接口
@app.route('/etl/migrate-nodes', methods=['POST'])
def migrate_nodes():
    data = request.json
    entity_table = data.get('entity_table')
    batch_size = data.get('batch_size', 2000)

    if not entity_table:
        return jsonify({'error': 'entity_table required'}), 400

    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    neo4j = Neo4jConnector()
    neo4j.connect()

    try:
        neo4j.create_unique_constraint("Entity", "id")
        offset = 0
        total_read = 0
        total_filtered = 0
        total_written = 0

        print(f"开始迁移节点，表名: {entity_table}")

        while True:
            nodes = mysql.fetch_entities(entity_table, batch_size, offset)
            if not nodes:
                break

            total_read += len(nodes)
            # 过滤掉 id 为 null 的记录
            valid_nodes = [n for n in nodes if n.get('id') is not None]
            filtered_count = len(nodes) - len(valid_nodes)
            total_filtered += filtered_count

            if valid_nodes:
                neo4j.batch_merge_nodes(valid_nodes)
                total_written += len(valid_nodes)
                print(f"批次 offset={offset}: 读取 {len(nodes)} 条, 过滤 {filtered_count} 条, 写入 {len(valid_nodes)} 条")
            else:
                print(f"批次 offset={offset}: 读取 {len(nodes)} 条, 全部被过滤")

            offset += batch_size

        print(f"节点迁移完成: 总读取 {total_read} 条, 总过滤 {total_filtered} 条, 总写入 {total_written} 条")
        result = {
            'success': True,
            'total_read': total_read,
            'total_filtered': total_filtered,
            'total_written': total_written
        }
    except Exception as e:
        result = {'success': False, 'error': str(e)}
    finally:
        mysql.disconnect()
        neo4j.disconnect()

    return jsonify(result)

@app.route('/etl/migrate-edges', methods=['POST'])
def migrate_edges():
    data = request.json
    relation_table = data.get('relation_table')
    relation_filter = data.get('relation_filter')
    batch_size = data.get('batch_size', 2000)
    field_mapping = data.get('field_mapping', {
        'source_id': 'source_id',
        'target_id': 'target_id',
        'relation_type': 'relation_type'
    })

    if not relation_table:
        return jsonify({'error': 'relation_table required'}), 400

    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    neo4j = Neo4jConnector()
    neo4j.connect()

    try:
        error_records = []
        offset = 0
        while True:
            edges = mysql.fetch_relations(relation_table, relation_filter, batch_size, offset)
            if not edges:
                break
            # 映射字段名
            mapped_edges = []
            for edge in edges:
                mapped_edge = {
                    'source_id': edge.get(field_mapping['source_id']),
                    'target_id': edge.get(field_mapping['target_id']),
                    'relation_type': edge.get(field_mapping['relation_type'])
                }
                if mapped_edge['source_id'] and mapped_edge['target_id']:
                    mapped_edges.append(mapped_edge)

            if mapped_edges:
                success_count, failed_edges = neo4j.batch_merge_relationships(mapped_edges)
                error_records.extend(failed_edges)
            offset += batch_size

        if error_records:
            import csv
            with open('error_report.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=error_records[0].keys())
                writer.writeheader()
                writer.writerows(error_records)

        result = {'success': True, 'failed_edges': len(error_records)}
    except Exception as e:
        result = {'success': False, 'error': str(e)}
    finally:
        mysql.disconnect()
        neo4j.disconnect()

    return jsonify(result)

@app.route('/etl/migrate-all', methods=['POST'])
def migrate_all():
    data = request.json
    entity_table = data.get('entity_table')
    relation_table = data.get('relation_table')
    batch_size = data.get('batch_size', 2000)
    init_schema = data.get('init_schema', False)
    id_field = data.get('id_field', 'id')  # 实体表的ID字段名
    name_field = data.get('name_field', 'name')  # 实体表的名称字段名

    if not entity_table or not relation_table:
        return jsonify({'error': 'entity_table and relation_table required'}), 400

    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    neo4j = Neo4jConnector()
    neo4j.connect()

    try:
        # 可选：初始化 schema
        if init_schema:
            entity_types = data.get('entity_types', [])
            unique_id_key = data.get('unique_id_key', 'id')
            index_keys = data.get('index_keys', [])
            neo4j.init_schema(entity_types, unique_id_key, index_keys)

        # 迁移节点
        neo4j.create_unique_constraint("Entity", "id")
        offset = 0
        total_read = 0
        total_filtered = 0
        total_written = 0

        print(f"开始迁移节点，表名: {entity_table}")

        while True:
            nodes = mysql.fetch_entities(entity_table, batch_size, offset)
            if not nodes:
                break

            # 输出第一条数据的字段名用于调试
            if offset == 0 and nodes:
                print(f"实体表字段: {list(nodes[0].keys())}")
                print(f"第一条数据示例 (前3个字段): {dict(list(nodes[0].items())[:3])}")

            total_read += len(nodes)
            # 过滤掉 id 为 null 的记录
            valid_nodes = []
            for n in nodes:
                node_id = n.get(id_field)
                if node_id is not None:
                    # 重命名字段为 'id' 和 'name'
                    node_copy = dict(n)
                    node_copy['id'] = node_id
                    if name_field in n:
                        node_copy['name'] = n[name_field]
                    valid_nodes.append(node_copy)
            filtered_count = len(nodes) - len(valid_nodes)
            total_filtered += filtered_count

            if valid_nodes:
                neo4j.batch_merge_nodes(valid_nodes)
                total_written += len(valid_nodes)
                print(f"批次 offset={offset}: 读取 {len(nodes)} 条, 过滤 {filtered_count} 条, 写入 {len(valid_nodes)} 条")
            else:
                print(f"批次 offset={offset}: 读取 {len(nodes)} 条, 全部被过滤")

            offset += batch_size

        print(f"节点迁移完成: 总读取 {total_read} 条, 总过滤 {total_filtered} 条, 总写入 {total_written} 条")

        # 迁移关系
        relation_filter = data.get('relation_filter')
        field_mapping = data.get('field_mapping', {
            'source_id': 'source_id',
            'target_id': 'target_id',
            'relation_type': 'relation_type'
        })
        error_records = []
        offset = 0
        total_edges_read = 0
        total_edges_filtered = 0
        total_edges_written = 0

        print(f"开始迁移关系，表名: {relation_table}, 字段映射: {field_mapping}")

        while True:
            edges = mysql.fetch_relations(relation_table, relation_filter, batch_size, offset)
            if not edges:
                break

            # 输出第一条数据的字段名用于调试
            if offset == 0 and edges:
                print(f"关系表字段: {list(edges[0].keys())}")
                print(f"第一条数据示例 (前3个字段): {dict(list(edges[0].items())[:3])}")

            total_edges_read += len(edges)
            # 映射字段名
            mapped_edges = []
            for edge in edges:
                mapped_edge = {
                    'source_id': edge.get(field_mapping['source_id']),
                    'target_id': edge.get(field_mapping['target_id']),
                    'relation_type': edge.get(field_mapping['relation_type'])
                }
                if mapped_edge['source_id'] and mapped_edge['target_id']:
                    mapped_edges.append(mapped_edge)

            filtered_count = len(edges) - len(mapped_edges)
            total_edges_filtered += filtered_count

            if mapped_edges:
                _, failed_edges = neo4j.batch_merge_relationships(mapped_edges)
                error_records.extend(failed_edges)
                success_count = len(mapped_edges) - len(failed_edges)
                total_edges_written += success_count
                print(f"批次 offset={offset}: 读取 {len(edges)} 条, 过滤 {filtered_count} 条, 成功 {success_count} 条, 失败 {len(failed_edges)} 条")
            else:
                print(f"批次 offset={offset}: 读取 {len(edges)} 条, 全部被过滤")

            offset += batch_size

        print(f"关系迁移完成: 总读取 {total_edges_read} 条, 总过滤 {total_edges_filtered} 条, 总写入 {total_edges_written} 条, 总失败 {len(error_records)} 条")

        # 生成错误报告
        if error_records:
            import csv
            with open('error_report.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=error_records[0].keys())
                writer.writeheader()
                writer.writerows(error_records)

        result = {
            'success': True,
            'nodes': {
                'total_read': total_read,
                'total_filtered': total_filtered,
                'total_written': total_written
            },
            'edges': {
                'total_read': total_edges_read,
                'total_filtered': total_edges_filtered,
                'total_written': total_edges_written,
                'total_failed': len(error_records)
            },
            'error_report': 'error_report.csv' if error_records else None
        }

    except Exception as e:
        result = {'success': False, 'error': str(e)}

    finally:
        mysql.disconnect()
        neo4j.disconnect()

    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)