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
        print(f"Loading JSON data from {filename}...")
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        logging.error(f"File {filename} not found.")
        raise
    except json.decoder.JSONDecodeError:
        logging.error(f"Error decoding JSON from file {filename}.")
        raise

def main():
    logging.basicConfig(level=logging.INFO)

    m = connect_to_mongodb()
    if not m:
        return

    perform_import = False

    json_data = load_json(filename="stations.json")

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



    logging.info("\n" + "=" * 40+"\n      STARTING IMPORT PROCESS" + "\n" + "=" * 40)
    # do things

    m.close()
    return

if __name__ == "__main__":
    main()
