# EV Monitoring & Data Modules

Minimal project to import, query and report data against Redis, Neo4j and MongoDB using the scripts in `MD1`, `MD2`, and `MD3`.

## Repository layout
- `MD1/md1.py` — Redis import/queries (movies example)
- `MD2/md2.py` — Neo4j import/queries (insurance/accident graph)
- `MD3/md3.py` — MongoDB import/reports (EV monitoring reports)
- `requirements.txt` — Python dependencies
- `.env` — Environment variables (not committed)

## Requirements
- Python 3.10+ (or compatible)
- Install deps:
  - pip install -r `requirements.txt`

## Environment
Create a `.env` file with the required variables per module:

- For `MD1` (Redis)
  - `REDIS_HOST`
  - `REDIS_PORT`
  - `REDIS_USER`
  - `REDIS_PASSWORD`

- For `MD2` (Neo4j)
  - `NEO4J_HOST`
  - `NEO4J_USER`
  - `NEO4J_PASSWORD`

- For `MD3` (MongoDB)
  - `MONGODB_HOST`
  - `MONGODB_USER`
  - `MONGODB_PASSWORD`

## Usage
1. Populate `.env`.
2. Install dependencies: `pip install -r requirements.txt`
3. Run a module:
   - Redis: `python MD1/md1.py`
   - Neo4j: `python MD2/md2.py`
   - MongoDB: `python MD3/md3.py`

Each script is interactive for import vs reuse of existing data and will prompt during import steps.

## Notes
- Sample JSON input files referenced within modules (e.g. `in_import_data.json`, `stations.json`, `sessions.json`) must be present where scripts expect them.
- Logs for `MD3` are written to `log.log` by default.