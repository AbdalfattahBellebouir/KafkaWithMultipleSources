import pandas as pd
from kafka import KafkaProducer
from random import randint
from time import sleep
import sys
from json import dumps


df = pd.read_csv('yellow_tripdata_2020-06.csv')
df = df.rename({'Unnamed: 0': 'Index'}, axis='columns')


broker = 'localhost:9092'
topic = 'yellow-tripdata'

print('Connection to broker started')

try:
    p = KafkaProducer(bootstrap_servers=[broker],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
except Exception as e:
    print("ERROR -->"+str(e))
    sys.exit(1)

print('Succesfull connection to broker')

seq = 0
ln = len(df.index)
while True:
    dest = seq+randint(2, 7)
    message = df[seq:dest]
    for i in message.itertuples(index=False):
        p.send(topic,value=i)
        print(i)
    
    seq = dest
    if(dest >= ln-1):
        break
    sleep(randint(1, 5))
