#! /usr/bin/env python3
import json
from kafka import KafkaConsumer
import pandas as pd
from tabulate import tabulate
import time

empty_stations = {}
consumer = KafkaConsumer("empty-stations", bootstrap_servers='localhost:9092',
                         group_id="velib-monitor-stations")

for message in consumer:
    station = json.loads(message.value.decode())
    station_number = station["number"]
    contract = station["contract_name"]
    available_bikes = station["available_bikes"]

#   new empty station
    if available_bikes == 0 and station_number not in empty_stations.keys():
        empty_stations[station_number] = station
        
        number_stations_empty = 0
        for number, value in empty_stations.items():
            if value["contract_name"]==contract:
                number_stations_empty+=1
        print("#"*40)
        print("!!! EMPTY:\n\tAdresse {}\n\tCity: {}\n\tTotal empty stations : ({})".format(station["address"], contract, number_stations_empty))
        print("#"*40)
    
# Remove not empty stations from the list    
    elif available_bikes!=0 and station_number in empty_stations:
        del empty_stations[station_number]

