from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pymongo import MongoClient
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    dag_id='Weather_to_mongo',
    start_date=datetime(2022, 8, 24),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 */4 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args=default_args
)


def get_weather_data(**context):
    url = context["params"]["url"]

    User = context["params"]["user"]
    Password = context["params"]["password"]
    api_key = context["params"]["apikey"]
    mongo_path = f"mongodb+srv://{User}:{Password}@nestcluster.6afp4lo.mongodb.net/?retryWrites=true&w=majority"

    lat = context["params"]["lat"]
    lon = context["params"]["lon"]

    url = f"{url}?lat={lat}&lon={lon}&exclude=current,minutely,hourly,alerts&appid={api_key}&units=metric"
    response = requests.get(url)
    data = json.loads(response.text)

    client = MongoClient(mongo_path)
    db = client.Cluster0

    insert_list = []

    for d in data["daily"]:
        temp_dict = dict()
        date = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        temp = d["temp"]["day"]
        min_temp = d["temp"]["min"]
        max_temp = d["temp"]["max"]
        humidity = d["humidity"]
        weather_state = d['weather'][0]['main']
        wind_speed = d['wind_speed']

        existing_doc = db.weather.find_one({"date": date})
        if existing_doc:
            print(f"Document with date {date} already exists. Skipping...")
            continue
        else:
            temp_dict["date"] = date
            temp_dict["temp"] = temp
            temp_dict["min_temp"] = min_temp
            temp_dict["max_temp"] = max_temp
            temp_dict["humidity"] = humidity
            temp_dict["weather_state"] = weather_state
            temp_dict["wind_speed"] = wind_speed

            insert_list.append(temp_dict)
            print(f"date {date} not exists, can insert")

    if not insert_list:
        print("No new data to insert. Exiting...")
        return

    try:
        with client.start_session() as s:
            def cb(s):
                db.weather.insert_many(insert_list, session=s)

            s.with_transaction(cb)
        print('Insert Success')

    except Exception as e:
        raise


get_weather_data = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    params={
        "url": "https://api.openweathermap.org/data/2.5/onecall",
        "lat": 37.5665,
        "lon": 126.9780,
        "user": "hajun",
        "password": Variable.get('mongo_password'),
        "apikey": Variable.get('open_weather_api_key')
    },

    provide_context=True,
    dag=dag,
)

get_weather_data