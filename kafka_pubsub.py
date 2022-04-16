from kafka import KafkaConsumer
from json import loads
import json


print("start:\n")

consumer = KafkaConsumer ('userdetails',bootstrap_servers = ['localhost:29092'],
value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
    print("user json :",message[6])
# Terminate the script
sys.exit()