from airflow.decorators import dag, task
from airflow import Dataset
from datetime import datetime

data = Dataset('/Users/yoohajun/Airflow/sample.txt')


@dag(default_args={
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 5)},
    schedule='@once',
    tags=['trigger', 'dataset'],
    catchup=False)
def producer():
    @task(outlets=[data])
    def update_a():
        with open('/Users/yoohajun/Airflow/sample.txt', 'a') as f:
            f.write('Updated content of sample.txt')
        print("sample.txt has been updated")

    update_a()


producer()
