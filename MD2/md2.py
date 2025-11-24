from typing import List, Dict, Any
import json
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase


def connect_to_neo4j():
    load_dotenv()

    neo4j_host = os.getenv("NEO4J_HOST")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    if not all([neo4j_host, neo4j_user, neo4j_password]):
        print("Error: Required environment variables are not set.")
        return None

    try:
        n = GraphDatabase.driver(neo4j_host, auth=(neo4j_user, neo4j_password))
        n.verify_connectivity()
        print("Connected to neo4j successfully!\n")
        return n

    except Exception as e:
        print(f"Failed to connect to neo4j: {e}")
        return None

def load_json(filename="data.json"):
    try:
        print(f"Loading JSON data from {filename}...")
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        print(f"File {filename} not found.")
        raise
    except json.decoder.JSONDecodeError:
        print(f"Error decoding JSON from file {filename}.")
        raise

def main():
    n = connect_to_neo4j()
    if not n:
        return

    with n.session() as session:
        summary = n.execute_query("""
            CREATE (a:Person {name: $name})
            CREATE (b:Person {name: $friendName})
            CREATE (a)-[:KNOWS]->(b)
            """, name="Alice", friendName="David", database_="neo4j", ).summary
        print("Created {nodes_created} nodes in {time} ms.".format(
            nodes_created=summary.counters.nodes_created,
            time=summary.result_available_after
        ))

        n.close()

    return

if __name__ == "__main__":
    main()
