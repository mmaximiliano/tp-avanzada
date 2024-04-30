from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'read_s3_csv_to_pandas',
    default_args=default_args,
    description='A DAG to read CSV files from S3 to Pandas DataFrame',
    schedule='@daily',
)

def read_csv_from_s3(**kwargs):
    s3_conn_id = 'aws_default'
    bucket_name = 'tpudesa'
    key = 'ads_views.csv'
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    print(f"Reading CSV file from S3 bucket: {bucket_name}, key: {key}")
    s3_conn = s3_hook.get_conn()
    print(f"Connection to S3 established: {s3_conn}")
    csv_object = s3_hook.get_key(key, bucket_name)
    print(f"CSV object: {csv_object}")
    csv_content = csv_object.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    print(df.head())

read_csv_task = PythonOperator(
    task_id='read_csv_from_s3_task',
    python_callable=read_csv_from_s3,
    dag=dag,
)

read_csv_task
