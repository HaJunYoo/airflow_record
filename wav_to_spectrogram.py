import os
import numpy as np
import librosa
import librosa.display
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

path = '/Users/yoohajun/Desktop/grad_audio/crime_theft'

# Define the input directory for the audio files
# audio_dir = os.path.join(path, 'data')
# Define the output directory for the spectrogram images
# spec_dir = os.path.join(path, 'spectrogram_fixed')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'schedule_interval': '* */1 * * *',
    'start_date': datetime(2023, 2, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('audio_to_spectrogram', default_args=default_args)


# Load the audio files
def generate_spectrogram(**context):
    audio_dir = context["params"]['audio_dir']
    spec_dir = context["params"]['spec_dir']
    img_height = context["params"]['img_height']
    img_width = context["params"]['img_width']
    # Loop over all the audio files in the input directory
    for idx, filename in enumerate(os.listdir(audio_dir)):
        if filename.endswith('.wav'):
            # Load the audio file
            filepath = os.path.join(audio_dir, filename)
            y, sr = librosa.load(filepath, sr=22050)

            # Generate the spectrogram
            S = librosa.feature.melspectrogram(y=y, sr=sr)
            S_db = librosa.power_to_db(S, ref=np.max)

            # Resize the spectrogram to the fixed shape
            S_resized = librosa.util.fix_length(S_db, size=img_width, axis=1, mode='constant')
            S_resized = S_resized[:img_height, :]
            print('spectrogram size : ', S_resized.shape)
            # Save the spectrogram as an image file
            spec_filename = filename[:-4] + '.png'
            spec_filepath = os.path.join(spec_dir, spec_filename)
            plt.imsave(spec_filepath, S_resized)

            print(f'{idx + 1}, {spec_filename} -> saved')

    print('finished')


def create_spec_dir(**context):
    spec_dir = context["params"]['spec_dir']
    if not os.path.exists(spec_dir):
        os.makedirs(spec_dir)
        print(f"{spec_dir} : created")


# Define the BashOperator that creates the output directory
create_spec_dir = PythonOperator(
    task_id='create_spec_dir',
    python_callable=create_spec_dir,
    params={
        'spec_dir': os.path.join(path, 'spectrogram_fixed')
    },
    provide_context=True,
    dag=dag
)

# Define the PythonOperator that loads the audio files
transform_specs = PythonOperator(
    task_id='transform_specs',
    python_callable=generate_spectrogram,
    params={
        'audio_dir': os.path.join(path, 'train'),
        'spec_dir': os.path.join(path, 'spectrogram_fixed'),
        # Define the fixed shape of the spectrograms
        'img_height': 128,
        'img_width': 256
    },
    provide_context=True,
    dag=dag
)

# Set the dependencies between tasks
create_spec_dir >> transform_specs
