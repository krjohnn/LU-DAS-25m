import os
import json
import logging
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure, ConfigurationError


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

    # Use f-string for cleaner syntax
    uri = f"mongodb+srv://{mongo_user}:{mongo_password}@{mongo_host}"

    try:
        client = MongoClient(uri, server_api=ServerApi('1'))

        # Verify connection
        client.admin.command('ping')
        logging.info("Successfully connected to MongoDB!")

        return client  # Return the client so other functions can use it!

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
        print(f"File {filename} not found.")
        raise
    except json.decoder.JSONDecodeError:
        print(f"Error decoding JSON from file {filename}.")
        raise

def main():
    logging.basicConfig(level=logging.INFO)

    m = connect_to_mongodb()

    print(m.admin.command('ping'))
    m.close()

    try:
        print(m.admin.command('ping'))
    except (ConnectionFailure, ConfigurationError) as e:
        logging.error(f"MongoDB Connection failed: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None


    # do things

    return

if __name__ == "__main__":
    main()
