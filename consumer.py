from airflow.decorators import dag, task
from airflow import Dataset
from datetime import datetime

data = Dataset('/Users/yoohajun/Airflow/sample.txt')

@dag(default_args={
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 5)},
    schedule=[data],
    tags=['trigger', 'dataset'],
    catchup=False)
def consumer():
    @task()
    def run():
        print("run")

    run()

consumer()
