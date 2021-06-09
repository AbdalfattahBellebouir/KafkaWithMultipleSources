from kafka import KafkaConsumer
from json import loads
import sys

broker = 'localhost:9092'
topic = 'yellow-tripdata'

print('Connection to broker started')

try:
    consumer = KafkaConsumer(topic,
     bootstrap_servers=[broker],
     value_deserializer=lambda x: loads(x.decode('utf-8')))
     
except Exception as e:
    print("ERROR -->"+str(e))
    sys.exit(1)

print('Succesfull connection to broker')

for message in consumer:
    print(message.value)
