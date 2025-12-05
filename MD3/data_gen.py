import json
import numpy as np
import uuid
import random
from faker import Faker

fake = Faker()

NUM_STATIONS = 30
NUM_SESSIONS = 150

OPERATORS = ["Tesla Supercharger", "Elektrum Drive", "Eleport", "Ignitis ON", "Enefit", "e-mobi", "Virši", "Tesla", "Fiqsy", "Inbalance"]
VEHICLES = ["Tesla Model 3", "Nissan Leaf", "Porsche Taycan", "Ford Mustang Mach-E", "Audi e-tron"]
STATUS_STATION = ["Online", "Online", "Online", "Maintenance", "Offline"]  # Weighted to Online
STATUS_SESSION = ["Completed", "Completed", "Completed", "Interrupted", "Charging"]
CITIES = ["Rīga","Daugavpils","Liepāja","Jelgava","Jūrmala","Ventspils","Rēzekne","Jēkabpils","Valmiera","Ogre"]


def generate_stations(count):
    stations = []
    print(f"Generating {count} stations...")
    for i in range(count):
        station = {
            "station_id": str(uuid.uuid4()),  # Unique UUID
            "operator": str(np.random.choice(OPERATORS,p=[0.005,0.135,0.1,0.1,0.1,0.25,0.25,0.05,0.005,0.005])),
            "location_city": str(np.random.choice(CITIES,p=[0.55, 0.06, 0.07, 0.06, 0.08, 0.05, 0.03, 0.02, 0.04, 0.04])),
            "max_power_kw": int(np.random.choice([50, 150, 250, 350])),
            "status": str(np.random.choice(STATUS_STATION))
        }
        stations.append(station)
    return stations


def generate_sessions(count, stations):
    sessions = []
    station_ids = [s["station_id"] for s in stations] 

    print(f"Generating {count} sessions linked to stations...")
    for i in range(count):
        duration = np.random.randint(15, 90)
        kwh = round(np.random.uniform(10, 80), 2)
        price = round(np.random.uniform(0.22, 0.45), 2)
        cost = round(kwh * price, 2)

        session = {
            "session_id": i + 1,
            "station_id": random.choice(station_ids),  # LINK TO PARENT
            "vehicle_type": random.choice(VEHICLES),
            "kwh_consumed": kwh,
            "duration_minutes": duration,
            "price_per_kwh": price,
            "total_cost": cost,
            "status": random.choice(STATUS_SESSION),
            "timestamp": fake.date_time_between(start_date="-1y", end_date="now").isoformat()
        }
        sessions.append(session)
    return sessions


if __name__ == "__main__":
    stations_data = generate_stations(NUM_STATIONS)
    with open("stations.json", "w", encoding='utf-8') as f:
        json.dump(stations_data, f, indent=4, ensure_ascii=False)
    print("Created stations.json")

    sessions_data = generate_sessions(NUM_SESSIONS, stations_data)
    with open("sessions.json", "w", encoding='utf-8') as f:
        json.dump(sessions_data, f, indent=4, ensure_ascii=False)
    print("Created sessions.json")