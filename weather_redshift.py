from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def etl(**context):
    url = context["params"]["url"]
    # 서울의 위도 경도
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    api_key = Variable.get("open_weather_api_key")

    # https://openweathermap.org/api/one-call-api
    url = f"{url}?lat={lat}&lon={lon}&exclude=current,minutely,hourly,alerts&appid={api_key}&units=metric"
    response = requests.get(url)
    data = json.loads(response.text)
    """
    {'dt': 1622948400, 'sunrise': 1622923873, 'sunset': 1622976631, 'moonrise': 1622915520, 'moonset': 1622962620, 'moon_phase': 0.87, 'temp': {'day': 26.59, 'min': 15.67, 'max': 28.11, 'night': 22.68, 'eve': 26.29, 'morn': 15.67}, 'feels_like': {'day': 26.59, 'night': 22.2, 'eve': 26.29, 'morn': 15.36}, 'pressure': 1003, 'humidity': 30, 'dew_point': 7.56, 'wind_speed': 4.05, 'wind_deg': 250, 'wind_gust': 9.2, 'weather': [{'id': 802, 'main': 'Clouds', 'description': 'scattered clouds', 'icon': '03d'}], 'clouds': 44, 'pop': 0, 'uvi': 3}
    """
    cur = get_redshift_connection()

    insert_vals = []
    # record 삭제 -> full refresh
    insert_sql = f"""BEGIN;DELETE FROM {schema}.{table};"""
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        insert_vals.append(f"""('{day}','{d["temp"]["day"]}','{d["temp"]["min"]}','{d["temp"]["max"]}')""")

    insert_sql += f"INSERT INTO {schema}.{table} VALUES " + ",".join(insert_vals)

    logging.info(insert_sql)
    try:
        cur.execute(sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise


"""
CREATE TABLE hajuny129.weather_forecast (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    updated_date timestamp default GETDATE()
);
"""

dag = DAG(
    dag_id='Weather_to_Redshift',
    start_date=datetime(2022, 8, 24),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 */2 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

etl = PythonOperator(
    task_id='etl',
    python_callable=etl,
    params={
        "url": "https://api.openweathermap.org/data/2.5/onecall",
        "lat": 37.5665,
        "lon": 126.9780,
        "schema": "hajuny129",
        "table": "weather_forecast"
    },
    provide_context=True,
    dag=dag
)

etl


