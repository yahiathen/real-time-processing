import json
import time
import urllib.request

from kafka import KafkaProducer

API_KEY = "16d93da7a901a61e4c1c55b8a1a27322d47a7519" # FIXME Set your own API key here
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'])

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        producer.send("velib-stations", json.dumps(station).encode(), key=str(station["number"]).encode())
    print("{} Produced {} station records".format(time.time(), len(stations)))
    time.sleep(1)       