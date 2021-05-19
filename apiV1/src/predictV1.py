from statsmodels.tsa.arima_model import ARIMA
from flask import Flask
import pandas as pd
import pmdarima as pm

server = Flask(__name__)

@server.route("/v1/run")
def readHumidity():
    df = pd.read_csv('humidity.csv', header=0)

    df = df.iloc[1:40]
    print(df.sanfrancisco)

    model = pm.auto_arima(df.sanfrancisco, start_p=1, start_q=1,
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
    n_periods = 24 # One day
    fc, confint = model.predict(n_periods=n_periods, return_conf_int=True)

    print(fc)

if __name__ == "__main__":
    server.run(host='0.0.0.0')
