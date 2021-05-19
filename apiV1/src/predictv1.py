from statsmodels.tsa.arima_model import ARIMA
from flask import Flask
from pymongo import MongoClient
from bson.json_util import dumps, loads
import pandas as pd
import pmdarima as pm
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
    datos = get_data_database('mongodb')

    df = define_dataframe(datos)
    print(df)

    model = pm.auto_arima(df.sanfranciscohumidity, start_p=1, start_q=1,
                      test='adf',       # use adftest to find optimal 'd'
                      max_p=3, max_q=3, # maximum p and q
                      m=1,              # frequency of series
                      d=None,           # let model determine 'd'
                      seasonal=False,   # No Seasonality
                      start_P=0,
                      D=0,
                      trace=True,
                      error_action='ignore',
                      suppress_warnings=True,
                      stepwise=True)

    model2 = pm.auto_arima(df.sanfranciscotemperature, start_p=1, start_q=1,
                      test='adf',       # use adftest to find optimal 'd'
                      max_p=3, max_q=3, # maximum p and q
                      m=1,              # frequency of series
                      d=None,           # let model determine 'd'
                      seasonal=False,   # No Seasonality
                      start_P=0,
                      D=0,
                      trace=True,
                      error_action='ignore',
                      suppress_warnings=True,
                      stepwise=True)

# Forecast
    fc, confint = model.predict(n_periods=n_periods, return_conf_int=True)
    fc2, confint2 = model2.predict(n_periods = n_periods, return_conf_int=True)

    json_ = '{predicciones: }'

    for x in range(n_periods):
        json_ = json_ + '{hour: ' + str(datetime.time(x%24,0)) + ', temp: ' + str(fc2[x]) + ', hum: ' + str(fc[x]) + '},'

    return dumps(json_)


@server.route("/servicio/v1/prediccion/24horas")
def prediccion24():
    return predict(24)

@server.route("/servicio/v1/prediccion/48horas")
def prediccion48():
    return predict(48)

@server.route("/servicio/v1/prediccion/72horas")
def prediccion72():
    return predict(72)

if __name__ == "__main__":
    server.run(host='0.0.0.0')
