
FROM python:3.8

WORKDIR /code

COPY requirements.txt .
COPY weather-data.csv .

RUN pip install -r requirements.txt

COPY src/ .

CMD ["python3", "./predictv1.py"]
