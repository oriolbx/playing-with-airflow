import requests
import time
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

############# ARGUMENTS ##################

address = 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-01.csv'
output = 'fast_download.txt'

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

############# INSTANTIATE A DAG ##################

dag = DAG(
    'download_fast',
    default_args=default_args
)

############# DEFINE PYTHON FUNCTIONS ##################

# download file using requests stream=True
def download(url):
    start_time = time.time()
    r = requests.get(url, stream= True)
    print(r.status_code)
    print('loading...')
    with open(output, 'wb') as filename:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                filename.write(chunk)
                filename.flush()
    print('total time download','----->',time.time() - start_time)

# count number of lines reading the data in line by line(only a single line is stored in the RAM at any given time)
def count_lines(filename):
    start_time_count = time.time()
    counter = 0
    with open(filename, 'r') as f:
        for line in f:
            counter += 1
    print('total time count lines','----->',time.time() - start_time_count)
    return counter

############# TASKS ##################

download_file = PythonOperator(
    task_id='download_file',
    python_callable=download,
    op_kwargs={'url': address},
    dag=dag
)

count_lines = PythonOperator(
    task_id='count_lines',
    python_callable=count_lines,
    op_kwargs={'filename': output},
    dag=dag
)

############# TASKS WORFLOW ##################

# download_file runs first and count_lines runs second 
# once download_file is completed
download_file >> count_lines
