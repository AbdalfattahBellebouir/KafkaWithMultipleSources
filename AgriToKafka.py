from kafka import KafkaProducer
from time import sleep
import sys
from json import dumps
import requests
import datetime
import time
import requests.exceptions
import urllib3.exceptions

broker = '160.176.224.87:9092'
topic = 'agri-weather-data'

print('Connection to broker on '+broker+' started')

try:
    p = KafkaProducer(bootstrap_servers=[broker],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
except Exception as e:
    print("ERROR -->"+str(e))
    sys.exit(1)

print('Successfull connection to broker')

URL="http://api.openweathermap.org/data/2.5/weather?q=Agadir&appid=93691540a79dc820dba68142a0279b2d&units=metric"

while(1):
    try:
        data = requests.get(url=URL).json()
        e = {
            "time":datetime.datetime.now().__str__(),
            "temperature":data['main']['temp'],                         # in °C
            "wind_speed":data['wind']['speed'],                         # in m/s
            "weather_description":str(data['weather'][0]['description']),   
            "humidity":data['main']['humidity']                         # in %
        }
        p.send(topic,value=e)
        print("["+str(datetime.datetime.now())+"]   Temperature: "+ str(data['main']['temp'])+" °C       Wind speed:"+ str(data['wind']['speed'])+" m/s"+"      Weather description: "+str(data['weather'][0]['description'])+ "        Humidity: "+str(data['main']['humidity'])+"%")
        time.sleep(1)
    except Exception as e:
        print("["+str(datetime.datetime.now())+"]" +'[' + str(e) + ']')
        continue
