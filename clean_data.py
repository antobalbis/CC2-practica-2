from pymongo import MongoClient
from bson.json_util import dumps, loads
import pandas as pd
import json

df1 = pd.read_csv('/tmp/datos/humidity.csv', header = 0)
df1 = df1.iloc[1:40]
df1 = df1.rename(columns={"San Francisco": "HUM"})
df0 = df1.datetime
df1 = df1.HUM

df2 = pd.read_csv('/tmp/datos/temperature.csv', header = 0)
df2 = df2.iloc[1:40]
df2 = df2.rename(columns={"San Francisco": "TEMP"})
df2 = df2.TEMP

merge = pd.concat([df0, df1, df2], axis = 1)
merge.to_csv('/tmp/datos/weather-data.csv')
merge.to_json('/tmp/datos/weather-data.json')

mongo_client = MongoClient('mongodb://%s:%s@localhost:27017' % ('admin', 'password'))
db = mongo_client.datos_tiempo
col = db.tiempo

json_ = open('/tmp/datos/weather-data.json').read()
data = loads(json_)

col.insert_one(data)
