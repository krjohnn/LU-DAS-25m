REPORTS = {
    "Complex logical filter": [
        {
            "$lookup": {
                "from": "stations",
                "localField": "station_id",
                "foreignField": "station_id",
                "as": "station_data"
            }
        },
        {
            "$match": {
                "station_data.location_city": "Rīga",
                "$or": [
                    {
                        "station_data.status": "Offline"
                    },
                    {
                        "station_data.status": "Maintenance"
                    }
                ],
                "price_per_kwh": {"$lte": 30.0},
                "status": "Completed",
                "total_cost": {"$gte": 20.0}
            }
        },
        {
            "$project": {
                "_id": 0,
                "session_id": 1,
                "station_id": 1,
                "vehicle_id": 1,
                "kwh_consumed": 1,
                "total_cost": 1,
                "price_per_kwh": 1,
                "status": 1
            }
        }
    ],
    "Revenue by Operator": [
        {
            "$lookup": {
                "from": "stations",
                "localField": "station_id",
                "foreignField": "station_id",
                "as": "station_details"
            }
        },
        {"$unwind": "$station_details"},
        {
            "$group": {
                "_id": "$station_details.operator",
                "totalRevenue": {"$sum": "$total_cost"},
                "sessionCount": {"$sum": 1}
            }
        },
        {"$sort": {"totalRevenue": -1}},
        {"$project": {
            "_id": 0,
            "operator": "$_id",
            "totalRevenue": 1,
            "sessionCount": 1
        }}
    ],
    "Average Charging Duration by Vehicle Type": [
        {
            "$lookup": {
                "from": "stations",
                "localField": "station_id",
                "foreignField": "station_id",
                "as": "station_details"}
        },
        {
            "$unwind": "$station_details"
        },
        {
            "$group": {
                "_id": "$vehicle_type",
                "avgDuration": {
                    "$avg": "$duration_minutes"
                }
            }
        },
        {
            "$sort":
                {
                    "avgDuration": -1
                }
        },
        {
            "$project": {
                "_id": 0,
                "carName": "$_id",
                "roundedAvgDuration": {
                    "$round": ["$avgDuration", 0]
                }
            }
        },
        {
            "$limit": 3
        }
    ],
    "City with highest recorded single charging sessions (kWh)": [
        {
            '$lookup': {
                'from': 'stations',
                'localField': 'station_id',
                'foreignField': 'station_id',
                'as': 'station_details'
            }
        }, {
            '$unwind': '$station_details'
        }, {
            '$group': {
                '_id': '$station_details.location_city',
                'maxKwhSession': {
                    '$max': '$kwh_consumed'
                }
            }
        }, {
            '$sort': {
                'maxKwhSession': -1
            }
        }, {
            '$project': {
                '_id': 0,
                'operator': '$_id',
                # 'maxKwhSession': 1,
                'maxKwhSession': {'$concat': [ {"$toString":'$maxKwhSession'}, ' kWh']},
                'sessionCount': 1
            }
        }, {
            '$limit': 3
        }
    ]
}
