import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'backup_files_to_s3',
    default_args=default_args,
    schedule_interval='@daily'
)

# Define the local directory to backup
local_dir = '/Users/yoohajun/Desktop/grad_audio/output'

# Define the S3 bucket and path to backup to
s3_bucket = 'your-s3-bucket'
s3_path = 'grad_audio/output'

# Define the S3Hook to interact with S3
s3_hook = S3Hook()

# Define the function to copy all files from the local directory to S3
def copy_files_to_s3():
    # Loop over all files in the local directory
    for filename in os.listdir(local_dir):
        filepath = os.path.join(local_dir, filename)
        # Check if the file is not a directory
        if os.path.isfile(filepath):
            # Define the key name for the file in S3
            s3_key = os.path.join(s3_path, filename)
            # Check if the file already exists in S3
            if not s3_hook.check_for_key(s3_key, bucket_name=s3_bucket):
                # Upload the file to S3
                s3_hook.load_file(filepath=filepath, key=s3_key, bucket_name=s3_bucket)
                print(f'Uploaded file {filename} to S3 bucket {s3_bucket} at path {s3_key}')
            else:
                print(f'File {filename} already exists in S3 bucket {s3_bucket} at path {s3_key}')

# Define the task to copy files from the local directory to S3
backup_files_to_s3 = PythonOperator(
    task_id='backup_files_to_s3',
    python_callable=copy_files_to_s3,
    dag=dag
)

# Define the BashOperator to create the output directory if it doesn't exist
create_output_dir = BashOperator(
    task_id='create_output_dir',
    bash_command=f'mkdir -p {local_dir}',
    dag=dag
)

# Set the dependencies between tasks
create_output_dir >> backup_files_to_s3