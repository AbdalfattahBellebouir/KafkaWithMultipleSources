from kafka import KafkaConsumer
from json import loads
import sys

broker = '192.168.1.13:9092'
topic = 'yellow-tripdata'

print('Connection to broker on '+broker+' started')

try:
    consumer = KafkaConsumer(topic,
     bootstrap_servers=[broker],
     value_deserializer=lambda x: loads(x.decode('utf-8')))
     
except Exception as e:
    print("ERROR -->"+str(e))
    sys.exit(1)

print('Successfull connection to broker')

for message in consumer:
    print(message.value)
