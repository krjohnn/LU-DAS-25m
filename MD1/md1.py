import redis
from typing import List, Dict, Any
import json
import os
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
        if 'id' in movie:
            r.hset(f"movie:{movie_id}", mapping=movie)
        if 'revenue' in movie:
            r.zadd("boxoffice:revenue", {movie_id: movie['revenue']})
        if 'rating' in movie:
            r.zadd("top:rated", {movie_id: movie['rating']})
        if 'genre' in movie:
            r.sadd(f"genre:{movie['genre']}", movie_id)
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

def update_movie_data(r: redis.Redis, json_data: List[Dict[str, Any]]) -> None:
    for movie in json_data.get("movies", []):
        movie_id = movie.get("id")
        r.hset(f"movie:{movie_id}", mapping=movie)
        if 'revenue' in movie:
            r.zadd("boxoffice:revenue", {movie_id: movie['revenue']})
        if 'rating' in movie:
            r.zadd("top:rated", {movie_id: movie['rating']})
        if 'genre' in movie:
            r.sadd(f"genre:{movie['genre']}", movie_id)
        # print(f"Updated movie {movie_id} with data: {movie}")
    print(f"Updated {len(json_data.get('movies', []))} movies in Redis.")

def delete_movie_data(r: redis.Redis, json_data: List[Dict[str, Any]]) -> None:
    for movie in json_data.get("movies", []):
        movie_id = movie.get("id")
        r.zrem("boxoffice:revenue", movie_id)
        r.zrem("top:rated", movie_id)
        genre = r.hget(f"movie:{movie_id}", "genre")
        r.srem(f"genre:{genre}", movie_id)
        r.delete(f"movie:{movie_id}")
    print(f"Deleted {len(json_data.get('movies', []))} movies from Redis.")

def select_movie_data_by_name(r: redis.Redis, movie_name: List[str]) -> None:
    movie_id = r.scan_iter(f"movie:*")
    for mid in movie_id:
        movie = r.hgetall(mid)
        if movie.get("title") in movie_name:
            print(f"\nMovie Data for '{movie.get("title")}':")
            for(key, value) in movie.items():
                print(f"{key}: {value}")

def select_top_n_movies_by_revenue(r: redis.Redis, order: str , n: int) -> None:
    if order == "top":
        top_movies = r.zrevrange("boxoffice:revenue", 0, n-1, withscores=True)
    if order == "bottom":
        top_movies = r.zrange("boxoffice:revenue", 0, n-1, withscores=True)
    print(f"\nTop {n} Movies by Revenue:")
    for movie_id, revenue in top_movies:
        movie = r.hgetall(f"movie:{movie_id}")
        print(f"Title: {movie.get('title')}, Revenue: {revenue}")

def select_top_n_movies_by_rating(r: redis.Redis, n: int) -> None:
    top_movies = r.zrevrange("top:rated", 0, n-1, withscores=True)
    print(f"\nTop {n} Movies by Rating:")
    for movie_id, rating in top_movies:
        movie = r.hgetall(f"movie:{movie_id}")
        print(f"Title: {movie.get('title')}, Rating: {rating}")

def select_movies_by_genre(r: redis.Redis, genre: str) -> None:
    movie_ids = r.smembers(f"genre:{genre}")
    print(f"\nMovies in Genre '{genre}':")
    for movie_id in movie_ids:
        movie = r.hgetall(f"movie:{movie_id}")
        print(f"Title: {movie.get('title')}")

def main():
    r = connect_to_redis()
    if not r:
        return

    if(r.dbsize() != 0):
        print("Redis database is empty. Inserting movie data...")

        # Add 100 entries - Done
        json_data = load_json(filename="in_import_data.json")
        import_movie_data(r, json_data)
        import_director_data(r, json_data)
        import_actor_data(r, json_data)

        # Edit 50 entires - To D
        json_data = load_json(filename="in_update_data.json")
        update_movie_data(r, json_data)

        # Select 50 entries - To Do
        select_movie_data_by_name(r, movie_name=["Pulp Fiction", "Inception", "Interstellar", "The Dark Knight", "Forrest Gump", "The Matrix", "The Shawshank Redemption", "The Godfather", "The Lord of the Rings: The Return of the King", "Fight Club"])
        select_top_n_movies_by_revenue(r, "top",10)
        select_top_n_movies_by_revenue(r, "bottom", 10)
        select_movies_by_genre(r, "Sci-Fi")

        # Delete 50 entries - To Do
        json_data = load_json(filename="in_delete_data.json")
        delete_movie_data(r, json_data)

        select_movies_by_genre(r, "Sci-Fi")

    else:
        print(f"Redis database is not empty: {r.dbsize()} entries found.")
        print(f"Flush the database to re-insert movie data.")
        r.flushall()

    r.close()
    return

if __name__ == "__main__":
    main()