from statsmodels.tsa.ar_model import AutoReg
from pymongo import MongoClient
from bson.json_util import dumps, loads
from flask import Flask
import pandas as pd
import numpy as np
import datetime

server = Flask(__name__)

def get_data_database(database):
    client = MongoClient('mongodb://%s:%s@%s:27017' % ('admin', 'password', database))
    db = client.datos_tiempo
    col = db.tiempo
    return dumps(col.find_one())

def define_dataframe(datos):
    df = pd.read_json(datos)
    df = df.iloc[1:40]
    return df

def predict(n_periods):
    datos = get_data_database('localhost')
    df = define_dataframe(datos)

    model = AutoReg(df.HUM, lags = 1)
    model2 = AutoReg(df.TEMP, lags = 1)

    model_fit = model.fit()
    model2_fit = model2.fit()

    fc = model.predict(np.ndarray(shape = (2, 1), dtype = float), start = 0, end = n_periods)
    fc2 = model2.predict(np.ndarray(shape = (2, 1), dtype = float), start = 0, end = n_periods)

    json_ = '{"predicciones": }'
    print(fc)
    for x in range(n_periods-1):
        json_ = json_ + '{hour: ' + str(datetime.time(x%24,0)) + ', temp: ' + str(fc2[x+1]) + ', hum: ' + str(fc[x+1]) + '},'

    return dumps(json_)

@server.route("/servicio/v2/prediccion/24horas")
def prediccion24():
    return predict(24)

@server.route("/servicio/v2/prediccion/48horas")
def prediccion48():
    return predict(48)

@server.route("/servicio/v2/prediccion/72horas")
def prediccion72():
    return predict(72)

if __name__ == "__main__":
    server.run(host='0.0.0.0')
