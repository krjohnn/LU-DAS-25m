"""Basic connection example.
"""

import redis
from typing import List, Dict, Any
import json
import os
import shlex
from dotenv import load_dotenv

# ============================================
# CONNECTION SETUP
# ============================================

def connect_to_redis():
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

def run_import_json(filename="data.json"):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"File {filename} not found.")
        return
    except json.decoder.JSONDecodeError:
        print(f"Error decoding JSON from file {filename}.")
        return

def main():
    r = connect_to_redis()
    if not r:
        return

    print("Importing core JSON data into Redis...")
    run_import_json(filename="llm_data_import.json")

    # Add 100 entries - Done
    if(r.dbsize() == 0):
        print("Redis database is empty. Inserting movie data...")
        #run_cli_script_seq(r, filename="create_movie_db.redis")

    else:
        print(f"Redis database is not empty: {r.dbsize()} entries found.")
        print(f"Flush the database to re-insert movie data.")
        r.flushall()

    # Edit 50 entires - To Do

    # Select 50 entries - To Do

    # Delete 50 entries - To Do

    r.close()
    return

if __name__ == "__main__":
    main()