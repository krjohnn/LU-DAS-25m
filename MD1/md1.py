"""Basic connection example.
"""

import redis
from typing import List, Dict, Any
import json
import os
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

def main():
    """Run sample operations and reports"""
    r = connect_to_redis()
    if not r:
        return

    print("TEST AREA BEGINS --------------------")
    print("TEST AREA ENDS --------------------")
    return

if __name__ == "__main__":
    main()

# r.close()
