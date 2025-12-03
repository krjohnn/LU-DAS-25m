import os
import json
import logging
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient, InsertOne
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure, ConfigurationError
from requests import session


def connect_to_mongodb():
    """
    Connects to MongoDB using environment variables.
    Returns: MongoClient instance or None if connection fails.
    """
    load_dotenv()

    mongo_host = os.getenv("MONGODB_HOST")
    mongo_user = os.getenv("MONGODB_USER")
    mongo_password = os.getenv("MONGODB_PASSWORD")

    if not all([mongo_host, mongo_user, mongo_password]):
        logging.error("Required environment variables (HOST, USER, PASSWORD) are not set.")
        return None

    uri = f"mongodb+srv://{mongo_user}:{mongo_password}@{mongo_host}"

    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        logging.info("Successfully connected to MongoDB!")

        return client

    except (ConnectionFailure, ConfigurationError) as e:
        logging.error(f"MongoDB Connection failed: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None

def load_json(filename="data.json"):
    try:
        logging.info(f"Loading JSON data from {filename}...")
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        logging.error(f"File {filename} not found.")
        raise
    except json.decoder.JSONDecodeError:
        logging.error(f"Error decoding JSON from file {filename}.")
        raise

def mongo_drop_database(mongo_client, db_name="EV_Monitoring"):
    try:
        mongo_client.drop_database(db_name)
        logging.info(f"Database '{db_name}' deleted successfully.")
    except Exception as e:
        logging.error(f"Failed to delete database '{db_name}': {e}")
        raise

def mongo_import_collection(db, collection_name="charging_stations", data=[]):
    try:
        collection = db[collection_name]
        if data:
            requests = [InsertOne(item) for item in data]
            result = collection.bulk_write(requests)
            logging.info(f"Imported {result.inserted_count} documents into '{collection_name}' collection.")
        else:
            logging.warning(f"No data provided to import into '{collection_name}' collection.")
    except Exception as e:
        logging.error(f"Failed to import data into collection '{collection_name}': {e}")
        raise

def mongo_create_database(mongo_client, db_name="EV_Monitoring"):
    try:
        db = mongo_client[db_name]
        logging.info(f"Database '{db_name}' created successfully.")
        return db
    except Exception as e:
        logging.error(f"Failed to create database '{db_name}': {e}")
        raise

def main():
    # logging.basicConfig(level=logging.INFO)
    logging.basicConfig(filename="log.log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    m = connect_to_mongodb()
    if not m:
        return

    mongodb_name = "EV_Monitoring"
    mongodb_exists = mongodb_name in m.list_database_names()

    perform_import = False

    if mongodb_exists:
        logging.info(f"Database {mongodb_name} already exists.")
        user_input = input("Delete existing database and re-import? (y/N): ").strip().lower()

        if user_input == 'y':
            logging.info("Deleting existing database and re-import.")
            mongo_drop_database(m, db_name="EV_Monitoring")
            perform_import = True
        else:
            logging.info("Using existing data. Skipping import.")
    else:
        perform_import = True
        logging.info(f"Creating new database {mongodb_name}. Performing import.")
        d = mongo_create_database(m, db_name=mongodb_name)

    if perform_import:
        logging.info("\n" + "=" * 40 + "\n      STARTING IMPORT PROCESS" + "\n" + "=" * 40)

        json_data = load_json(filename="stations.json")

        mongo_import_collection(d, collection_name="charging_stations", data=json_data)

        logging.info("\n" + "=" * 40 + "\n      IMPORT COMPLETE" + "\n" + "=" * 40)
        return

    logging.info("\n" + "=" * 40 + "\n      STARTING REPORTS" + "\n" + "=" * 40)
    reports = []
    logging.info("\n" + "=" * 40 + "\n      REPORTS ARE COMPLETE" + "\n" + "=" * 40)

    """
    print(m.get_database("sample_mflix"))
    db = m["EV_Monitoring"]
    collection = db["charging_stations"]
    requesting = []

    for item in json_data:
        requesting.append(InsertOne(item))

    result = collection.bulk_write(requesting)
    print(result)

    print(m.list_database_names())


    # collection_list = database.list_collections()
    # for c in collection_list:
    #     print(c)
    """

    m.close()
    logging.info("MongoDB connection closed.")
    return

if __name__ == "__main__":
    main()
