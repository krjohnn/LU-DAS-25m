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
    print(f"Imported {len(json_data.get("insurance_companies", []))} insurance companies into DB.")

def import_person_data(n, json_data: List[Dict[str, Any]]) -> None:
    for person in json_data.get("persons", []):
        n.execute_query("""
        CREATE (p:Person {social_security_number: $social_security_number, full_name: $full_name, date_of_birth: $date_of_birth, address: $address, phone_number: $phone_number, risk_level: $risk_level})
        """, social_security_number=person.get("social_security_number"), full_name=person.get("full_name"),
                        date_of_birth=person.get("date_of_birth"), address=person.get("address"),
                        phone_number=person.get("phone_number"), risk_level=person.get("risk_level"), database_="neo4j")
    print(f"Imported {len(json_data.get("persons", []))} persons into DB.")

def import_policy_data(n, json_data: List[Dict[str, Any]]) -> None:
    for policy in json_data.get("policies", []):
        n.execute_query("""
        CREATE (p:Policy {policy_id: $policy_id, policy_type: $policy_type, type_of_insurance: $type_of_insurance, start_date: $start_date, end_date: $end_date, insured_person: $insured_person, deductible_amount: $deductible_amount, coverage_amount: $coverage_amount})
        """, policy_id=policy.get("policy_id"), policy_type=policy.get("policy_type"), type_of_insurance=policy.get("type_of_insurance"),
                        start_date=policy.get("start_date"), end_date=policy.get("end_date"), insured_person=policy.get("insured_person"),
                        deductible_amount=policy.get("deductible_amount"), coverage_amount=policy.get("coverage_amount"), database_="neo4j")
    print(f"Imported {len(json_data.get("policies", []))} policies into DB.")

def import_car_data(n, json_data: List[Dict[str, Any]]) -> None:
    for car in json_data.get("cars", []):
        n.execute_query("""
        CREATE (c:Car {registration_number: $registration_number, make: $make, model: $model, year: $year, owner: $owner, vin: $vin, technical_inspection_date: $technical_inspection_date, technical_inspection_end_date: $technical_inspection_end_date, policy_number: $policy_number})
        """, registration_number=car.get("registration_number"), make=car.get("make"), model=car.get("model"),
                        year=car.get("year"), owner=car.get("owner"), vin=car.get("vin"), technical_inspection_date=car.get("technical_inspection_date"),
                        technical_inspection_end_date=car.get("technical_inspection_end_date"), policy_number=car.get("policy_number"), database_="neo4j")
    print(f"Imported {len(json_data.get("cars", []))} cars into DB.")

def import_accident_data(n, json_data: List[Dict[str, Any]]) -> None:
    print("Importing Accidents...")
    n.execute_query("""
            UNWIND $accidents AS acc

            // Create the Accident Node
            MERGE (a:Accident {accident_id: acc.accident_id})
            SET a.date = datetime(acc.date),
                a.weather = acc.weather_conditions,
                a.description = acc.description,
                a.severity = acc.severity_level,
                a.location = point({latitude: acc.location.lat, longitude: acc.location.lon}),
                a.location_desc = acc.location.description
            """, accidents=json_data.get("accidents", []), database_="neo4j")
    print(f"Imported {len(json_data.get("accidents", []))} accidents into DB.")


def main():
    n = connect_to_neo4j()
    if not n:
        return

    # insurance_companies -- DONE
    # persons -- DONE
    # policies -- DONE (relationships - TO DO)
    # cars -- DONE
    # accidents -- TO DO
    # claims -- TO DO

    json_data = load_json(filename="in_import_data.json")

    import_accident_data(n, json_data)
    return

    for i in json_data.get("accidents", []):
        print("\nAccident:")
        for k, v in i.items():
            if isinstance(v, list):
                for item in v:
                    print(k)
                    print(f"    - {item}")
            else:
                print(f"  {k}: {v}")

    return
    import_insurance_company_data(n, json_data)
    import_person_data(n, json_data)
    import_policy_data(n, json_data)
    import_car_data(n, json_data)


    #delete_all_nodes(n)
    n.close()

    return

if __name__ == "__main__":
    main()
