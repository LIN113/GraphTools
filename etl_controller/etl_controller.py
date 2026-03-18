import csv
from typing import List, Dict
import sys
sys.path.append('..')


class GraphMigrationController:
    def __init__(self, mysql_connector, neo4j_connector):
        self.mysql = mysql_connector
        self.neo4j = neo4j_connector
        self.error_records = []

    def run_node_migration(self, batch_size: int = 2000):
        self.neo4j.create_unique_constraint("Entity", "id")

        offset = 0
        while True:
            nodes = self.mysql.fetch_entities(limit=batch_size, offset=offset)
            if not nodes:
                break

            self.neo4j.batch_merge_nodes(nodes)
            offset += batch_size

    def run_edge_migration(self, relation_filter: str = None, batch_size: int = 2000):
        offset = 0
        while True:
            edges = self.mysql.fetch_relations(
                relation_type=relation_filter,
                limit=batch_size,
                offset=offset
            )
            if not edges:
                break

            success_count, failed_edges = self.neo4j.batch_merge_relationships(edges)
            self.error_records.extend(failed_edges)
            offset += batch_size

    def generate_error_report(self, output_path: str = "error_report.csv"):
        if not self.error_records:
            return

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.error_records[0].keys())
            writer.writeheader()
            writer.writerows(self.error_records)
