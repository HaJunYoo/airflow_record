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
import boto3

## 발전 사항
##  로컬에 저장한 후
## mongoDB에 저장하게 하기
## 이 때 사진에 pk값을 넣어서 저장하게 하기
# mongdo db에 사진이 이미 존재할 때 업로드하지 않고
# 존재하지 않을 때만 업로드하게 하기

## 로컬에 저장한 후
## 해당 내용을 s3에 업로드하게 하기

# train data가 저장되어 있는 장소
source_path = '/Users/yoohajun/Desktop/grad_audio/source'
# output mfcc,spectrogram이 저장될 장소
output_path = '/Users/yoohajun/Desktop/grad_audio/output'

# Define the output directory for the spectrogram, mfcc images
# spec_dir = os.path.join(output_path, 'spectrogram_fixed')
mfcc_dir = os.path.join(output_path, 'mfcc_fixed')

# # Define the S3 bucket name and region
# S3_BUCKET_NAME = 'your-bucket-name'
# S3_BUCKET_REGION = 'your-bucket-region'
#
# # Create an S3 client
# s3 = boto3.client('s3', region_name=S3_BUCKET_REGION)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'schedule_interval': '* */2 * * *',
    # 'schedule_interval': '@daily',
    'start_date': datetime(2023, 3, 5),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('audio_to_mfcc_v1', default_args=default_args)


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
def generate_spectrogram(subdir, audio_dir, img_height, img_width):
    # Create the output directory for the spectrogram images under the subdir/data directory

    mfcc_dir_2 = os.path.join(mfcc_dir, subdir)
    if not os.path.exists(mfcc_dir_2):
        os.makedirs(mfcc_dir_2)
        print(f"{mfcc_dir_2} : vacant created")

    target_shape = (img_height, img_width)
    size = 256
    pad_size = 256
    repeat_size = 5

    # Loop over all the audio files in the input directory
    for idx, filename in enumerate(os.listdir(audio_dir)):
        if filename.endswith('.wav'):
            # Load the audio file
            filepath = os.path.join(audio_dir, filename)
            y, sr = librosa.load(filepath, sr=44100)

            mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=size)

            for i in range(repeat_size):
                mfcc_filename = f'{filename[:-4]}_{i + 1}.png'

                if mfcc_filename in os.listdir(mfcc_dir_2):
                    print(f'{subdir} : {idx + 1}, {mfcc_filename} -> exists')
                    continue
                else:
                    ## padding
                    mfcc = random_pad(mfcc, pad_size=pad_size, mfcc=True)

                    # Resize the mfcc to the fixed shape
                    mfcc_resized = resize(mfcc, target_shape)

                    # Save the MFCC as an image file
                    mfcc_filepath = os.path.join(mfcc_dir_2, mfcc_filename)
                    plt.imsave(mfcc_filepath, mfcc_resized)

                    print(f'{subdir} : {idx + 1}, {mfcc_filename} -> saved')

    print(f'Finished processing audio files in directory {audio_dir}')


def create_data_dirs():
    # Loop over subdirectories under the path directory - categories
    folders = ['robbery', 'theft', 'exterior', 'interior', 'sexual', 'violence', 'help']

    for subdir in os.listdir(source_path):
        if subdir in folders:
            # Create the data directory if it doesn't exist
            data_dir = os.path.join(source_path, subdir, 'train')
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)
                print(f"{data_dir} : vacant created")
            # Run the generate_spectrogram function for the data directory
            generate_spectrogram(subdir, data_dir, 256, 256)


# Define the BashOperator that creates the output directory
create_mfcc_dir = BashOperator(
    task_id='create_mfcc_dir',
    bash_command=f'mkdir -p {mfcc_dir}',
    dag=dag
)

# Define the PythonOperator that creates the data directories and runs the generate_spectrogram function
transform_audio = PythonOperator(
    task_id='transform_audio',
    python_callable=create_data_dirs,
    dag=dag
)

# Set the dependencies between tasks
create_mfcc_dir >> transform_audio