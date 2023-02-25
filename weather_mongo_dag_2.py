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
    dag_id='Weather_to_mongo_v2',
    start_date=datetime(2022, 8, 24),  # Date in the future won't trigger the DAG
    schedule_interval='0 0 */3 * *',  # Adjust to desired interval
    max_active_runs=1,
    catchup=False,
    default_args=default_args
)

def extract_weather_data(**context):
    url = context["params"]["url"]
    api_key = context["params"]["apikey"]
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]

    url = f"{url}?lat={lat}&lon={lon}&exclude=current,minutely,hourly,alerts&appid={api_key}&units=metric"
    response = requests.get(url)
    data = json.loads(response.text)

    context["ti"].xcom_push(key="data", value=data["daily"])

    print(data["daily"])
    # return data["daily"]


def transform_weather_data(**context):
    daily_data = context["ti"].xcom_pull(key="data", task_ids="extract_weather_data")

    transformed_data = []
    for d in daily_data:
        temp_dict = dict()
        date = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        temp = d["temp"]["day"]
        min_temp = d["temp"]["min"]
        max_temp = d["temp"]["max"]
        humidity = d["humidity"]
        weather_state = d['weather'][0]['main']
        wind_speed = d['wind_speed']

        temp_dict["date"] = date
        temp_dict["temp"] = temp
        temp_dict["min_temp"] = min_temp
        temp_dict["max_temp"] = max_temp
        temp_dict["humidity"] = humidity
        temp_dict["weather_state"] = weather_state
        temp_dict["wind_speed"] = wind_speed

        transformed_data.append(temp_dict)

    context["ti"].xcom_push(key="data", value=transformed_data)

    print(transformed_data)
    # return transformed_data


def load_weather_data(**context):
    transformed_data = context["ti"].xcom_pull(key="data", task_ids="transform_weather_data")

    User = context["params"]["user"]
    Password = context["params"]["password"]
    mongo_path = f"mongodb+srv://{User}:{Password}@nestcluster.6afp4lo.mongodb.net/?retryWrites=true&w=majority"

    client = MongoClient(mongo_path)
    db = client.Cluster0

    insert_list = []

    for d in transformed_data:
        existing_doc = db.weather.find_one({"date": d["date"]})
        if existing_doc:
            print(f"Document with date {d['date']} already exists. Skipping...")
        else:
            insert_list.append(d)

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


extract_weather_data = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    params={
        "url": "https://api.openweathermap.org/data/2.5/onecall",
        "lat": 37.5665,
        "lon": 126.9780,
        "apikey": Variable.get('open_weather_api_key')
    },
    provide_context=True,
    dag=dag
)

transform_weather_data = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    provide_context=True,
    dag=dag
)

load_weather_data = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    params={
        "user": 'hajun',
        "password": Variable.get('mongo_password')
    },
    provide_context=True,
    dag=dag
)

extract_weather_data >> transform_weather_data >> load_weather_data
