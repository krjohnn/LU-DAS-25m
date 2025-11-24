# LU-DAS-25m (MD1)

A small Python project demonstrating importing, updating, selecting, and deleting movie data in Redis using a simple script (`MD1/md1.py`). This repository stores example JSON payloads and uses environment variables for Redis connection.

## Features

- Load movie, director and actor data from JSON files into Redis
- Update and delete movie records
- Query top movies by revenue and rating
- Lookup movies by genre and find actors by award
- Uses dotenv to load Redis credentials from `.env`

## Requirements

- Python 3.8+
- Redis server accessible from your environment

Install Python dependencies:

```bash
python3 -m pip install -r MD1/requirements.txt
```

## Environment variables

Create a `.env` file in the project root (or set the variables in your environment) with the following keys:

```
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_USER=
REDIS_PASSWORD=
```

The script expects all four to be set (username may be blank depending on your Redis configuration).

## Project layout (relevant files)

- `MD1/md1.py` - main script that reads JSON files and interacts with Redis
- `MD1/in_import_data.json` - example input for initial import
- `MD1/in_update_data.json` - example input for updates
- `MD1/in_delete_data.json` - example input for deletions
- `MD1/requirements.txt` - Python dependencies

## How it works (high-level)

1. `MD1/md1.py` loads environment variables and connects to Redis.
2. If the Redis DB is empty, it:
   - imports movies, directors and actors from `in_import_data.json`
   - applies updates from `in_update_data.json`
   - runs several selection queries (top revenue, top rating, by genre, actors with awards)
   - deletes entries listed in `in_delete_data.json`
3. Otherwise it prints the DB size and flushes the database if desired.

The script stores:
- each movie as a Redis hash `movie:<id>`
- director and actor hashes `director:<id>` and `actors:<id>`
- revenue in a sorted set `boxoffice:revenue`
- ratings in a sorted set `top:rated`
- genre membership in a set `genre:<genre>`

## Usage

Run the script from the repository root:

```bash
python3 MD1/md1.py
```

You can inspect Redis while the script runs (for example with `redis-cli`) to see the keys and values created.

## Notes & recommended improvements

- Avoid using `KEYS` in production: `KEYS` blocks and can be slow on large datasets. The script uses `scan_iter` in some places (preferred). When iterating many keys use `SCAN`/`SCAN_ITER` to keep Redis responsive.

- Prefer using consistent key naming and types: the script already uses `movie:<id>` hashes and other typed structures which is good.

- Use pipelines/batches for bulk writes: when importing many records use `redis.pipeline()` to reduce round-trips and improve performance.

- Validate JSON and fields before writing: ensure numeric fields (like `revenue` and `rating`) are converted to numbers, and handle missing IDs gracefully.

- Use stronger typing and small helper functions for repeated operations (e.g., add_movie, remove_movie) to make the code more testable and readable.

- Connection handling: consider retry/backoff strategies for transient Redis connection failures.

## Why `KEYS` is discouraged and what to use instead

- `KEYS` scans the entire keyspace and can block Redis on large databases.
- Use `SCAN` (or client helper `scan_iter`) which is incremental and non-blocking. Example: `for k in r.scan_iter("movie:*"):`

## License & authors

This is a small demo project. Add your own license as needed.

---

If you'd like, I can also:
- Add a small example `.env.example` file
- Improve `MD1/md1.py` to use pipelines and better error handling (no behavioral changes)
- Add unit tests for the helper functions

Tell me which of the above you'd like next.
