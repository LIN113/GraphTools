from flask import Flask, jsonify, request
from mysql_extractor import MySQLExtractor
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

@app.route('/tables', methods=['GET'])
def get_tables():
    """获取所有表名"""
    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    tables = extractor.get_all_tables()
    extractor.disconnect()
    return jsonify({'tables': tables})

@app.route('/tables/columns', methods=['GET'])
def get_columns():
    """获取表的字段信息"""
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({'error': 'table_name parameter is required'}), 400

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    columns = extractor.get_table_columns(table_name)
    extractor.disconnect()
    return jsonify({'columns': columns})

@app.route('/tables/entities', methods=['GET'])
def get_entities():
    """获取实体数据"""
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({'error': 'table_name parameter is required'}), 400

    batch_size = request.args.get('batch_size', 100, type=int)
    offset = request.args.get('offset', 0, type=int)

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    entities = extractor.fetch_entities(table_name, batch_size, offset)
    extractor.disconnect()
    return jsonify({'entities': entities, 'count': len(entities)})

@app.route('/tables/relations', methods=['GET'])
def get_relations():
    """获取关系数据"""
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({'error': 'table_name parameter is required'}), 400

    batch_size = request.args.get('batch_size', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    relation_filter = {k: v for k, v in request.args.items() if k not in ['table_name', 'batch_size', 'offset']}

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    relations = extractor.fetch_relations(table_name, relation_filter or None, batch_size, offset)
    extractor.disconnect()
    return jsonify({'relations': relations, 'count': len(relations)})

@app.route('/tables/check', methods=['GET'])
def check_metadata():
    """健康检查：验证表和必需字段"""
    table_name = request.args.get('table_name')
    required_columns = request.args.get('required_columns')

    if not table_name or not required_columns:
        return jsonify({'error': 'table_name and required_columns are required'}), 400

    columns_list = [col.strip() for col in required_columns.split(',')]

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    try:
        extractor.check_metadata(table_name, columns_list)
        extractor.disconnect()
        return jsonify({'valid': True, 'message': '表结构验证通过'})
    except ValueError as e:
        extractor.disconnect()
        return jsonify({'valid': False, 'error': str(e)}), 400

@app.route('/tables/entity-types', methods=['GET'])
def get_entity_types():
    """获取所有实体类型"""
    table_name = request.args.get('table_name')
    type_column = request.args.get('type_column')

    if not table_name or not type_column:
        return jsonify({'error': 'table_name and type_column are required'}), 400

    extractor = MySQLExtractor(**DB_CONFIG)
    extractor.connect()
    entity_types = extractor.get_distinct_entity_types(table_name, type_column)
    extractor.disconnect()
    return jsonify({'entity_types': entity_types})

if __name__ == '__main__':
    app.run(debug=True, port=5001)
