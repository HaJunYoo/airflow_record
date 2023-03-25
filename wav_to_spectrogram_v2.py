import os
import numpy as np
import librosa
import librosa.display
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

## 발전 사항
##  로컬에 저장한 후
## mongoDB에 저장하게 하기
## 이 때 사진에 pk값을 넣어서 저장하게 하기
# mongdo db에 사진이 이미 존재할 때 업로드하지 않고
# 존재하지 않을 때만 업로드하게 하기

## 로컬에 저장한 후
## 해당 내용을 s3에 업로드하게 하기

# train data가 저장되어 있는 장소
path = '/Users/yoohajun/Desktop/grad_audio/source'

# Define the output directory for the spectrogram images
spec_dir = os.path.join(path, 'spectrogram_fixed')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    # 'schedule_interval': '* */4 * * *',
    'schedule_interval': '@daily',
    'start_date': datetime(2023, 2, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('audio_to_spectrogram_v2', default_args=default_args)


# Load the audio files
def generate_spectrogram(subdir, audio_dir, img_height, img_width):
    # Create the output directory for the spectrogram images under the subdir/data directory
    spec_dir_2 = os.path.join(spec_dir, subdir)
    if not os.path.exists(spec_dir_2):
        os.makedirs(spec_dir_2)
        print(f"{spec_dir_2} : vacant created")


    # Loop over all the audio files in the input directory
    for idx, filename in enumerate(os.listdir(audio_dir)):
        if filename.endswith('.wav'):
            # Load the audio file
            filepath = os.path.join(audio_dir, filename)
            y, sr = librosa.load(filepath, sr=44100)

            # Generate the spectrogram
            S = librosa.feature.melspectrogram(y=y, sr=sr)
            S_db = librosa.power_to_db(S, ref=np.max)

            # Resize the spectrogram to the fixed shape
            S_resized = librosa.util.fix_length(S_db, size=img_width, axis=1, mode='constant')
            S_resized = S_resized[:img_height, :]

            # Save the spectrogram as an image file
            spec_filename = filename[:-4] + '.png'
            spec_filepath = os.path.join(spec_dir_2, spec_filename)
            plt.imsave(spec_filepath, S_resized)

            print(f'{subdir} : {idx + 1}, {spec_filename} -> saved')

    print(f'Finished processing audio files in directory {audio_dir}')


def create_data_dirs():
    # Loop over subdirectories under the path directory - categories
    folders = ['robbery', 'theft', 'exterior', 'interior', 'sexual', 'violence', 'help']

    for subdir in os.listdir(path):
        if subdir in folders:
            # Create the data directory if it doesn't exist
            data_dir = os.path.join(path, subdir, 'train')
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)
                print(f"{data_dir} : vacant created")
            # Run the generate_spectrogram function for the data directory
            generate_spectrogram(subdir, data_dir, 128, 256)


# Define the BashOperator that creates the output directory
create_spec_dir = BashOperator(
    task_id='create_spec_dir',
    bash_command=f'mkdir -p {spec_dir}',
    dag=dag
)

# Define the PythonOperator that creates the data directories and runs the generate_spectrogram function
transform_audio = PythonOperator(
    task_id='transform_audio',
    python_callable=create_data_dirs,
    dag=dag
)

# Set the dependencies between tasks
create_spec_dir >> transform_audio