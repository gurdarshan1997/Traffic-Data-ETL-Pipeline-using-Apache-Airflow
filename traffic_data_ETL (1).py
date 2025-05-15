from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import tarfile
import requests
import pandas as pd

# Define constants
URL = 'https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/trafficdata.tgz'
DATA_DIR = '/tmp/traffic_data'
EXTRACT_DIR = f'{DATA_DIR}/extracted'
ZIP_FILE = f'{DATA_DIR}/trafficdata.tgz'

# Default args
default_args = {
    'owner': 'Gurdarshan_Singh',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='traffic_data_ETL',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Task 1: Create directories
    create_directories = BashOperator(
        task_id='create_directories',
        bash_command=f'mkdir -p {EXTRACT_DIR}'
    )
    
    # Task 2: Download the file
    def download_data():
        response = requests.get(URL, stream=True)
        with open(ZIP_FILE, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
    
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data
    )
    
    # Task 3: Unzip the file
    def extract_data():
        with tarfile.open(ZIP_FILE, 'r:gz') as tar:
            tar.extractall(EXTRACT_DIR)
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    
    # Task 4: Extract data from CSV
    def process_csv():
        df = pd.read_csv(f'{EXTRACT_DIR}/vehicle-data.csv')
        df.iloc[:, [0, 1, 2, 3]].to_csv(f'{EXTRACT_DIR}/csv_d.csv', index=False)
    
    extract_csv_task = PythonOperator(
        task_id='extract_csv',
        python_callable=process_csv
    )
    
    # Task 5: Extract data from TSV
    def process_tsv():
        df = pd.read_csv(f'{EXTRACT_DIR}/tollplaza-data.tsv', sep='\t')
        df.iloc[:, [4, 5, 6]].to_csv(f'{EXTRACT_DIR}/tsv_d.csv', index=False)
    
    extract_tsv_task = PythonOperator(
        task_id='extract_tsv',
        python_callable=process_tsv
    )
    
    # Task 6: Extract data from fixed-width file
    def process_fixed_width():
        colspecs = [(0, 2), (3, 5)]  # Adjust based on file format
        df = pd.read_fwf(f'{EXTRACT_DIR}/payment-data.txt', colspecs=colspecs)
        df.to_csv(f'{EXTRACT_DIR}/fixed_width_d.csv', index=False)
    
    extract_fixed_width_task = PythonOperator(
        task_id='extract_fixed_width',
        python_callable=process_fixed_width
    )
    
    # Task 7: Combine extracted files
    combine_data = BashOperator(
        task_id='combine_data',
        bash_command=f'paste -d "," {EXTRACT_DIR}/csv_d.csv {EXTRACT_DIR}/tsv_d.csv {EXTRACT_DIR}/fixed_width_d.csv > {EXTRACT_DIR}/combined_data.csv'
    )
    
    # Task 8: Transform data
    def transform_data():
        df = pd.read_csv(f'{EXTRACT_DIR}/combined_data.csv')
        df.iloc[:, 3] = df.iloc[:, 3].str.upper()
        df.to_csv(f'{EXTRACT_DIR}/transformed_data.csv', index=False)
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    # Set task dependencies
    create_directories >> download_task >> extract_task
    extract_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> combine_data >> transform_task
