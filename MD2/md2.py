import redis
from typing import List, Dict, Any
import json
import os
from dotenv import load_dotenv

def connect_to_neo4j():
    """Connect to Redis (adjust host/port as needed)"""
    load_dotenv()

    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT")
    redis_user = os.getenv("REDIS_USER")
    redis_password = os.getenv("REDIS_PASSWORD")

    if not all([redis_host, redis_port, redis_user, redis_password]):
        print("Error: Required environment variables are not set.")
        return None

    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=True,
        username=redis_user,
        password=redis_password,
    )
    try:
        r.ping()
        print("Connected to Redis successfully!\n")
        return r
    except redis.ConnectionError:
        print("Could not connect to Redis")
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
    r = connect_to_redis()
    if not r:
        return

    r.close()
    return

if __name__ == "__main__":
    main()
