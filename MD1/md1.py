import redis
from typing import List, Dict, Any
import json
import os
import shlex
from dotenv import load_dotenv

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

def import_movie_data(r: redis.Redis, json_data: List[Dict[str, Any]]) -> None:
    for movie in json_data.get("movies", []):
        movie_id = movie.get("id")
        if movie_id:
            r.hset(f"movie:{movie_id}", mapping=movie)
    print(f"Imported {len(json_data.get('movies', []))} movies into Redis.")

def import_director_data(r: redis.Redis, json_data: List[Dict[str, Any]]) -> None:
    for director in json_data.get("directors", []):
        director_id = director.get("id")
        if director.get("awards"):
            director["awards"] = ', '.join(director.get("awards"))
        if director_id:
            r.hset(f"director:{director_id}", mapping=director)
    print(f"Imported {len(json_data.get('directors', []))} directors into Redis.")

def import_actor_data(r: redis.Redis, json_data: List[Dict[str, Any]]) -> None:
    for actor in json_data.get("actors", []):
        actor_id = actor.get("id")
        if actor.get("awards"):
            actor["awards"] = ', '.join(actor.get("awards"))
        if actor_id:
            r.hset(f"actors:{actor_id}", mapping=actor)
    print(f"Imported {len(json_data.get('actors', []))} actors into Redis.")

def main():
    r = connect_to_redis()
    if not r:
        return

    json_data = load_json(filename="llm_data_import.json")

    # Add 100 entries - Done
    if(r.dbsize() == 0):
        print("Redis database is empty. Inserting movie data...")
        import_movie_data(r, json_data)
        import_director_data(r, json_data)
        import_actor_data(r, json_data)

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