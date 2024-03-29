import os
import numpy as np
import librosa
import librosa.display
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from skimage.transform import resize

from airflow import Dataset

import zipfile
import math

## 로컬에 저장한 후
# spectrogram 폴더를 압축한 뒤 s3에 업로드
## 향후 s3에 저장된 압축파일을 다운받아서 모델 학습할 수 있게 설정


saved_data = Dataset('/Users/yoohajun/Desktop/grad_audio/output/spectrogram_fixed.zip')

# train data가 저장되어 있는 장소
source_path = '/Users/yoohajun/Desktop/grad_audio/source'
# output mfcc,spectrogram이 저장될 장소
output_path = '/Users/yoohajun/Desktop/grad_audio/output'

# Define the output directory for the spectrogram, mfcc images
spec_dir = os.path.join(output_path, 'spectrogram_fixed')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': True,
    # 'schedule_interval': '* */1 * * *',
    'schedule_interval': '@once',
    'start_date': datetime(2023, 4, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG('audio_to_spectrogram_inex', default_args=default_args)


def random_pad(mels, pad_size, mfcc=True):
    pad_width = pad_size - mels.shape[1]
    rand = np.random.rand()
    left = int(pad_width * rand)
    right = pad_width - left

    if left < 0:
        right += abs(left)
        left = 0

    if right < 0:
        left += abs(right)
        right = 0

    if mfcc:
        mels = np.pad(mels, pad_width=((0, 0), (left, right)), mode='constant')
        local_max, local_min = mels.max(), mels.min()
        mels = (mels - local_min) / (local_max - local_min)

    else:
        local_max, local_min = mels.max(), mels.min()
        mels = (mels - local_min) / (local_max - local_min)
        mels = np.pad(mels, pad_width=((0, 0), (left, right)), mode='constant')

    return mels


# Load the audio files
def generate_spectrogram(subdir, audio_dir, img_height, img_width, seconds_per_spec):
    # Create the output directory for the spectrogram images under the subdir/data directory
    spec_dir_2 = os.path.join(spec_dir, subdir)
    if not os.path.exists(spec_dir_2):
        os.makedirs(spec_dir_2)
        print(f"{spec_dir_2} : vacant created")

    target_shape = (img_height, img_width)
    size = 128
    pad_size = 128
    # repeat_size = 2

    # Loop over all the audio files in the input directory
    for idx, filename in enumerate(os.listdir(audio_dir)):
        if filename.endswith('.wav'):

            # Load the audio file
            filepath = os.path.join(audio_dir, filename)
            y, sr = librosa.load(filepath, sr=44100)

            # Calculate the number of spectrograms to generate based on seconds_per_spec
            num_specs = math.ceil(len(y) / (sr * seconds_per_spec))

            for i in range(num_specs):
                start = int(i * sr * seconds_per_spec)
                end = int(min(start + sr * seconds_per_spec, len(y)))
                y_slice = y[start:end]

                # Generate the spectrogram
                mels = librosa.feature.melspectrogram(y=y_slice, sr=sr)
                mels_db = librosa.power_to_db(mels, ref=np.max)

                # for the incremental update

                spec_filename = f'{filename[:-4]}_{i + 1}.png'

                if spec_filename in os.listdir(spec_dir_2):
                    print(f'{subdir} : {idx + 1}, {spec_filename} -> exists')
                    continue
                else:
                    ## padding
                    # mels_db = random_pad(mels_db, pad_size=pad_size, mfcc=False)
                    # Resize the spectrogram to the fixed shape
                    mels_resized = resize(mels_db, target_shape)

                    # Save the spectrogram as an image file
                    spec_filepath = os.path.join(spec_dir_2, spec_filename)
                    plt.imsave(spec_filepath, mels_resized)

                    print(f'{subdir} : {idx + 1}, {spec_filename} -> saved')

    print(f'Finished processing audio files in directory {audio_dir}')


# Define the BashOperator that creates the output directory
create_spec_dir = BashOperator(
    task_id='create_spec_dir',
    bash_command=f'mkdir -p {spec_dir}',
    dag=dag
)

def print_finish():
    print("finish!")
    return "finish!"

finish_task = PythonOperator(
    task_id='finish_task',
    python_callable=print_finish,
    dag=dag
)

# Define the BashOperator to create a compressed zip file
# create_zip_file = BashOperator(
#     task_id='create_zip_file',
#     bash_command=f'cd {output_path} && zip -r spectrogram_fixed.zip spectrogram_fixed',
#     dag=dag
# )

# def create_zip(output_path: str):
#     with zipfile.ZipFile(output_path + '/spectrogram_fixed.zip', 'w', zipfile.ZIP_DEFLATED) as zip_file:
#         zip_file.write(output_path + '/spectrogram_fixed')
#
#
# create_zip_file = PythonOperator(
#     task_id='create_zip_file',
#     python_callable=create_zip,
#     outlets=[saved_data],
#     op_kwargs={'output_path': output_path},
#     dag=dag
# )

# Define the PythonOperator that creates the data directories and runs the generate_spectrogram function
folders = ['exterior', 'interior']

for folder in folders:
    data_dir = os.path.join(source_path, folder, 'train')

    create_source_dir = BashOperator(
        task_id=f'create_source_dir_{folder}',
        bash_command=f'mkdir -p {data_dir} && echo "vacant created"',
        dag=dag
    )

    # Define the directory for the audio files
    audio_dir = os.path.join(source_path, folder, 'train')
    # Define the task ID for the PythonOperator
    task_id = f'generate_spectrogram_{folder}'

    # Define the PythonOperator
    generate_spectrogram_task = PythonOperator(
        task_id=task_id,
        python_callable=generate_spectrogram,
        op_kwargs={
            'subdir': folder,
            'audio_dir': audio_dir,
            'img_height': 128,
            'img_width': 128,
            'seconds_per_spec': 5
        },
        dag=dag
    )

    # Set the dependencies between tasks
    create_spec_dir >> create_source_dir >> generate_spectrogram_task >> finish_task
