from statsmodels.tsa.ar_model import AutoReg
from pymongo import MongoClient
from bson.json_util import dumps, loads
from flask import Flask
import pandas as pd
import numpy as np
import datetime

server = Flask(__name__)

def predict(n_periods):
    client = MongoClient('mongodb://%s:%s@mongodb:27017' % ('admin', 'password'))
    db = client.datos_tiempo
    col = db.tiempo
    datos = dumps(col.find_one())

    df = pd.read_json(datos)
    df = df.iloc[1:40]

    model = AutoReg(df.sanfranciscohumidity, lags = 1)
    model2 = AutoReg(df.sanfranciscotemperature, lags = 1)

    model_fit = model.fit()
    model2_fit = model2.fit()

# Forecast
    fc = model.predict(np.ndarray(shape = (2, 1), dtype = float), start = 0, end = n_periods)
    fc2 = model2.predict(np.ndarray(shape = (2, 1), dtype = float), start = 0, end = n_periods)

    print(fc)
    print(fc2)
    json_ = '{"predicciones": }'

    for x in range(n_periods):
        json_ = json_ + '{hour: ' + str(datetime.time(x%24,0)) + ', temp: ' + str(fc2[x]) + ', hum: ' + str(fc[x]) + '},'

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
