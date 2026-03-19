from neo4j import GraphDatabase
from typing import List, Dict, Tuple
import os
from dotenv import load_dotenv


class Neo4jConnector:
    def __init__(self, uri: str = None, user: str = None, password: str = None, database: str = None):
        load_dotenv()
        self.uri = uri or os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.user = user or os.getenv('NEO4J_USER', 'neo4j')
        self.password = password or os.getenv('NEO4J_PASSWORD')
        self.database = database or os.getenv('NEO4J_DATABASE', 'neo4j')
        self.driver = None

    def connect(self):
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def disconnect(self):
        if self.driver:
            self.driver.close()

    def create_unique_constraint(self, label: str, property_name: str):
        with self.driver.session(database=self.database) as session:
            query = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property_name} IS UNIQUE"
            session.run(query)

    def create_index(self, label: str, property_name: str):
        with self.driver.session(database=self.database) as session:
            query = f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{property_name})"
            session.run(query)

    def init_schema(self, entity_types: list, unique_id_key: str, index_keys: list):
        for entity_type in entity_types:
            self.create_unique_constraint(entity_type, unique_id_key)
            for index_key in index_keys:
                self.create_index(entity_type, index_key)

    def batch_merge_nodes(self, node_list: List[Dict]):
        with self.driver.session(database=self.database) as session:
            query = """
            UNWIND $nodes AS node
            MERGE (n:Entity {id: node.id})
            SET n += node
            """
            session.run(query, nodes=node_list)

    def batch_merge_relationships(self, edge_list: List[Dict]) -> Tuple[int, List[Dict]]:
        success_count = 0
        failed_edges = []

        with self.driver.session(database=self.database) as session:
            for edge in edge_list:
                # 使用 apoc 创建动态关系类型
                query = """
                MATCH (a:Entity {id: $source_id})
                MATCH (b:Entity {id: $target_id})
                CALL apoc.merge.relationship(a, $rel_type, {}, {}, b, {}) YIELD rel
                RETURN rel
                """
                try:
                    result = session.run(query,
                                       source_id=edge['source_id'],
                                       target_id=edge['target_id'],
                                       rel_type=edge['relation_type'])

                    if result.single():
                        success_count += 1
                    else:
                        failed_edges.append(edge)
                except Exception:
                    failed_edges.append(edge)

        return success_count, failed_edges

    def get_schema(self) -> Dict:
        with self.driver.session(database=self.database) as session:
            labels = [record['label'] for record in session.run("CALL db.labels()")]
            rel_types = [record['relationshipType'] for record in session.run("CALL db.relationshipTypes()")]
            prop_keys = [record['propertyKey'] for record in session.run("CALL db.propertyKeys()")]
            return {'labels': labels, 'relationship_types': rel_types, 'property_keys': prop_keys}
