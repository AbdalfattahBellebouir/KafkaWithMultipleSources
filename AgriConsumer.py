import pandas as pd
from kafka import KafkaConsumer
from random import randint
from time import sleep
import sys
from json import loads


broker = 'localhost:9092'
topic = 'agri-weather-data'

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
