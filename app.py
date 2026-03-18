from flask import Flask, jsonify, request
from mysql_extractor.mysql_extractor import MySQLExtractor
from neo4j_module.neo4j_loader import Neo4jConnector
from etl_controller.etl_controller import GraphMigrationController
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

    controller = GraphMigrationController(mysql, neo4j)
    controller.run_node_migration(batch_size)

    mysql.disconnect()
    neo4j.disconnect()
    return jsonify({'success': True})

@app.route('/etl/migrate-edges', methods=['POST'])
def migrate_edges():
    data = request.json
    relation_table = data.get('relation_table')
    relation_filter = data.get('relation_filter')
    batch_size = data.get('batch_size', 2000)

    if not relation_table:
        return jsonify({'error': 'relation_table required'}), 400

    mysql = MySQLExtractor(**DB_CONFIG)
    mysql.connect()
    neo4j = Neo4jConnector()
    neo4j.connect()

    controller = GraphMigrationController(mysql, neo4j)
    controller.run_edge_migration(relation_filter, batch_size)

    if controller.error_records:
        controller.generate_error_report()

    mysql.disconnect()
    neo4j.disconnect()
    return jsonify({'success': True, 'errors': len(controller.error_records)})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
