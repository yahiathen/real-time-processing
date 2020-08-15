#! /usr/bin/env python3
import json
import time
import urllib.request

# Run `pip install kafka-python` to install this package
from kafka import KafkaProducer

API_KEY = "16d93da7a901a61e4c1c55b8a1a27322d47a7519" # FIXME
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="localhost:9092")


empty_stations_memory = []
while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:

        # newly empty
        if station["available_bikes"] == 0 and station["number"] not in empty_stations_memory:
            empty_stations_memory.append(station["number"])
            producer.send("empty-stations", json.dumps(station).encode(),
                          key=str(station["contract_name"]).encode())
            # print("Produced {} station {} records EMPTY".format(len(stations), station['contract_name']))
        
        # became not empty
        elif station["available_bikes"] != 0 and station["number"] in empty_stations_memory:
            producer.send("empty-stations", json.dumps(station).encode(),
                          key=str(station["contract_name"]).encode())
            empty_stations_memory.remove(station["number"])
                # print("Produced {} station {} records NOT EMPTY".format(len(stations), station['contract_name']))
    time.sleep(5)
