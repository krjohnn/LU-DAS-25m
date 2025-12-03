REPORTS = {
    "1: Complex logical filter": [
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
                "vehicle_type": 1,
                "kwh_consumed": 1,
                "total_cost": 1,
                "price_per_kwh": 1,
                "status": 1
            }
        }
    ],
    "2: Revenue by Operator": [
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
    "3: Top 3 Average Charging Duration by Vehicle Type": [
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
    "4: Top 3 cities with highest recorded single charging sessions (kWh)": [
        {
            "$lookup": {
                "from": "stations",
                "localField": "station_id",
                "foreignField": "station_id",
                "as": "station_details"
            }
        }, {
            "$unwind": "$station_details"
        }, {
            "$group": {
                "_id": "$station_details.location_city",
                "topSession": {
                    "$top": {
                        "sortBy": {
                            "kwh_consumed": -1
                        },
                        "output": {
                            "maxKwhSession": "$kwh_consumed",
                            "duration_minutes": "$duration_minutes",
                            "vehicle_id": "$vehicle_id"
                        }
                    }
                }
            }
        }, {
            "$sort": {
                "topSession.maxKwhSession": -1
            }
        }, {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "maxKwhSession": {
                    "$concat": [
                        {
                            "$toString": "$topSession.maxKwhSession"
                        }, " kWh"
                    ]
                },
                "duration_minutes": "$topSession.duration_minutes",
                "vehicle_id": "$topSession.vehicle_id"
            }
        }, {
            "$limit": 3
        }
    ],
    "5: Interrupted vs Completed session for each Operator": [
        {
            "$lookup": {
                "from": "stations",
                "localField": "station_id",
                "foreignField": "station_id",
                "as": "station_details"
            }
        }, {
            "$unwind": "$station_details"
        }, {
            "$match": {
                "status": {
                    "$in": [
                        "Interrupted", "Completed"
                    ]
                }
            }
        }, {
            "$group": {
                "_id": {
                    "operator": "$station_details.operator",
                    "status": "$status"
                },
                "sessionCount": {
                    "$sum": 1
                },
                "kwhConsumed": {
                    "$sum": "$kwh_consumed"
                },
                "avgDuration": {
                    "$avg": "$duration_minutes"
                }
            }
        }, {
            "$project": {
                "_id": 0,
                "Operator": "$_id.operator",
                "Status": "$_id.status",
                "sessionCount": 1,
                "kwhConsumed": 1,
                "RoundavgDuration": {
                    "$round": [
                        "$avgDuration", 2
                    ]
                }
            }
        }, {
            "$sort": {
                "Operator": 1,
                "Status": 1
            }
        }
    ]
}
