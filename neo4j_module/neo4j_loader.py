from neo4j import GraphDatabase
from typing import List, Dict, Tuple


class Neo4jConnector:
    def __init__(self, uri: str, user: str, password: str):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None

    def connect(self):
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def disconnect(self):
        if self.driver:
            self.driver.close()

    def create_unique_constraint(self, label: str, property_name: str):
        with self.driver.session() as session:
            query = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property_name} IS UNIQUE"
            session.run(query)

    def batch_merge_nodes(self, node_list: List[Dict]):
        with self.driver.session() as session:
            query = """
            UNWIND $nodes AS node
            MERGE (n:Entity {id: node.id})
            SET n += node
            """
            session.run(query, nodes=node_list)

    def batch_merge_relationships(self, edge_list: List[Dict]) -> Tuple[int, List[Dict]]:
        success_count = 0
        failed_edges = []

        with self.driver.session() as session:
            for edge in edge_list:
                query = """
                MATCH (a:Entity {id: $source_id})
                MATCH (b:Entity {id: $target_id})
                MERGE (a)-[r:RELATION {type: $rel_type}]->(b)
                RETURN r
                """
                result = session.run(query,
                                   source_id=edge['source_id'],
                                   target_id=edge['target_id'],
                                   rel_type=edge['relation_type'])

                if result.single():
                    success_count += 1
                else:
                    failed_edges.append(edge)

        return success_count, failed_edges
