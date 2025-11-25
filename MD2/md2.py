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

def delete_all_nodes(n):
    try:
        with n.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("All nodes and relationships deleted successfully.")
    except Exception as e:
        print(f"Error deleting nodes: {e}")
        raise

def import_insurance_company_data(n, json_data: List[Dict[str, Any]]) -> None:
    for insurance_company in json_data.get("insurance_companies", []):
        n.execute_query("""
        CREATE (c:InsuranceCompany {id: $id, name: $name, address: $address, contact_email: $contact_email})
        """, id=insurance_company.get("id"), name=insurance_company.get("name"), address=insurance_company.get("address"),
                        contact_email=insurance_company.get("contact_email"), database_="neo4j")
    print(f"Imported {len(json_data.get("insurance_companies", []))} insurance companies into Neo4j.")


def import_person_data(n, json_data: List[Dict[str, Any]]) -> None:
    for person in json_data.get("persons", []):
        n.execute_query("""
        CREATE (p:Person {social_security_number: $social_security_number, full_name: $full_name, date_of_birth: $date_of_birth, address: $address, phone_number: $phone_number, risk_level: $risk_level})
        """, social_security_number=person.get("social_security_number"), full_name=person.get("full_name"),
                        date_of_birth=person.get("date_of_birth"), address=person.get("address"),
                        phone_number=person.get("phone_number"), risk_level=person.get("risk_level"), database_="neo4j")
    print(f"Imported {len(json_data.get("persons", []))} persons into Neo4j.")

def main():
    n = connect_to_neo4j()
    if not n:
        return

    # insurance_companies -- DONE
    # persons -- DONE
    # policies
    # cars
    # accidents
    # claims

    json_data = load_json(filename="in_import_data.json")

    for i in json_data.get("persons", []):
        print(i)

    import_person_data(n, json_data)

    #import_insurance_company_data(n, json_data)

    #delete_all_nodes(n)
    n.close()

    return

if __name__ == "__main__":
    main()
