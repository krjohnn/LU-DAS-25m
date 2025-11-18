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


def run_cli_script_seq(r, filename="data.redis"):
    if not r:
        return

    print(f"Opening script file: {filename}")
    command_count = 0
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                try:
                    parts = shlex.split(line)
                    r.execute_command(*parts)
                    command_count += 1

                except redis.exceptions.RedisError as e:
                    print(f"Error running command: {line}")
                    print(f"Redis error: {e}")

        print(f"\nSuccessfully executed {command_count} commands one by one.")

    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    """Run sample operations and reports"""
    r = connect_to_redis()
    if not r:
        return

    # run_cli_script_seq(r, filename="create_movie_db.redis")

    # Add 100 entries - Done
    # Edit 50 entires - To Do
    # Select 50 entries - To Do
    # Delete 50 entries - To Do

    # hset(movie_key, "genre", new_genre_string)

    r.hset("movie:1001", "year", "1994")

    movie_cnt = 0
    movies = (r.keys("movie:*"))
    for movie_key in movies:
        movie_cnt = movie_cnt+1
        print(movie_cnt)
        movie_data = r.hgetall(movie_key)
        print(f"{movie_key}: {movie_data}")

    r.close()
    return

if __name__ == "__main__":
    main()