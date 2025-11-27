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
        MERGE (c:InsuranceCompany {id: $id, name: $name, address: $address, contact_email: $contact_email})
        """, id=insurance_company.get("id"), name=insurance_company.get("name"), address=insurance_company.get("address"),
                        contact_email=insurance_company.get("contact_email"), database_="neo4j")
    print(f"Imported {len(json_data.get("insurance_companies", []))} insurance companies into DB.")

def import_person_data(n, json_data: List[Dict[str, Any]]) -> None:
    for person in json_data.get("persons", []):
        n.execute_query("""
        MERGE (p:Person {social_security_number: $social_security_number, full_name: $full_name, date_of_birth: $date_of_birth, address: $address, phone_number: $phone_number, risk_level: $risk_level})
        """, social_security_number=person.get("social_security_number"), full_name=person.get("full_name"),
                        date_of_birth=person.get("date_of_birth"), address=person.get("address"),
                        phone_number=person.get("phone_number"), risk_level=person.get("risk_level"), database_="neo4j")
    print(f"Imported {len(json_data.get("persons", []))} persons into DB.")

def import_policy_data(n, json_data: List[Dict[str, Any]]) -> None:
    n.execute_query("""
    UNWIND $policies AS policy
    MERGE (pol:Policy {policy_id: policy.policy_id})
    SET pol.policy_type = policy.policy_type, 
        pol.type_of_insurance = policy.type_of_insurance, 
        pol.start_date = policy.start_date, 
        pol.end_date = policy.end_date, 
        pol.insured_person = policy.insured_person, 
        pol.deductible_amount = policy.deductible_amount, 
        pol.coverage_amount = policy.coverage_amount
    
    WITH pol
    MATCH (p:Person {social_security_number: pol.insured_person})
    MERGE (pol)-[:INSURES]->(p)
    """, policies=json_data.get("policies", []), database_="neo4j")
    print(f"Imported {len(json_data.get("policies", []))} policies into DB.")

def import_car_data(n, json_data: List[Dict[str, Any]]) -> None:
    n.execute_query("""
        UNWIND $cars AS car
        MERGE (c:Car {registration_number: car.registration_number, vin: car.vin})
        SET c.make = car.make,
            c.model = car.model,
            c.year = car.year,
            c.owner = car.owner,
            c.technical_inspection_date = car.technical_inspection_date,
            c.technical_inspection_end_date = car.technical_inspection_end_date,
            c.policy_number = car.policy_number
        
        WITH c        
        MATCH (p:Person {social_security_number: c.owner})
        MERGE (p)-[:OWNS]->(c)
        
        WITH c
        MATCH (c:Car)
        WHERE c.policy_number IS NOT NULL
        UNWIND c.policy_number AS policy_number
        MATCH (p:Policy {policy_id: policy_number})
        MERGE (p)-[:COVERS]->(c)
        """, cars=json_data.get("cars", []), database_="neo4j")
    print(f"Imported {len(json_data.get("cars", []))} cars into DB.")

def import_accident_data(n, json_data: List[Dict[str, Any]]) -> None:
    n.execute_query("""
            UNWIND $accidents AS acc

            // Create the Accident Node
            MERGE (a:Accident {accident_id: acc.accident_id})
            SET a.date = datetime(acc.date),
                a.weather = acc.weather_conditions,
                a.description = acc.description,
                a.severity = acc.severity_level,
                a.location = point({latitude: acc.location.lat, longitude: acc.location.lon}),
                a.location_desc = acc.location.desc,
                a.name = acc.accident_id
                
            // Link Involved Cars and their damage details
            WITH a, acc
            UNWIND acc.involved_cars AS car_data
            MATCH (c:Car {registration_number: car_data.registration_number})
            MERGE (c)-[r:INVOLVED_IN]->(a)
            SET r.damage_level = car_data.damage_level,
                r.damage_desc = car_data.damage_description

            // Link Involved People and their roles and injuries
            WITH a, acc, car_data
            UNWIND acc.involved_persons AS person_data
            MATCH (p:Person {social_security_number: person_data.ssn})
            MERGE (p)-[r:INVOLVED_IN]->(a)
            SET r.role = person_data.role,
                r.injuries = person_data.injuries

            // Check for "At Fault" party and create relationship
            WITH a, acc, car_data
            WHERE car_data.at_fault_party IS NOT NULL
            MATCH (p_fault:Person {social_security_number: car_data.at_fault_party})
            MERGE (p_fault)-[:CAUSED]->(a)

            """, accidents=json_data.get("accidents", []), database_="neo4j")
    print(f"Imported {len(json_data.get("accidents", []))} accidents into DB.")

def import_claim_data(n, json_data: List[Dict[str, Any]]) -> None:
    n.execute_query("""
            UNWIND $claims AS c
            MERGE (cl:Claim {claim_id: c.claim_id})
            SET cl.date_filed = datetime(c.date_filed),
                cl.claimant = c.claimant,
                cl.policy_number = c.policy_number,
                cl.accident_id = c.accident_id,
                cl.claim_amount = c.claim_amount,
                cl.status = c.status,
                cl.name = c.claim_id
                
            WITH c, cl
            MATCH (p:Person {social_security_number: cl.claimant})
            MERGE (cl)-[r:CLAIMANT]->(p)
            
            WITH c, cl
            MATCH (pol:Policy {policy_id: cl.policy_number})
            MERGE (cl)-[r:AGAINST_POLICY]->(pol)
    """, claims=json_data.get("claims", []), database_="neo4j")
    print(f"Imported {len(json_data.get("claims", []))} claims into DB.")

def main():
    n = connect_to_neo4j()
    if not n:
        return

    # insurance_companies -- DONE
    # persons -- DONE
    # policies -- DONE (relationships - TO DO)
    # cars -- DONE
    # accidents -- DONE
    # claims -- DONE

    json_data = load_json(filename="in_import_data.json")

    # for i in json_data.get("claims", []):
    #     print("\nClaim:")
    #     for k, v in i.items():
    #         if isinstance(v, list):
    #             for item in v:
    #                 print(k)
    #                 print(f"    - {item}")
    #         else:
    #             print(f"  {k}: {v}")


    import_insurance_company_data(n, json_data)
    import_person_data(n, json_data)
    import_policy_data(n, json_data)
    import_car_data(n, json_data)
    import_accident_data(n, json_data)
    import_claim_data(n, json_data)

    #delete_all_nodes(n)
    n.close()

    return

if __name__ == "__main__":
    main()
