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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
