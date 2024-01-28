from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import io
import pandas as pd
import requests
import boto3
import os


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 27),
    'email': ['nishajagdish99@gmail.com'],
    'email_on_failure':False
}

dag = DAG(
    'airbnb_dag',
    default_args = default_args,
    description = 'my first dag'
)

def load_data_from_s3(**kwargs):
    # Specify your AWS credentials and region
    aws_access_key_id = 'YOUR_ACCESS_KEY_ID'
    aws_secret_access_key = 'YOUR_SECRET_ACCESS_KEY'
    region_name = 'YOUR_REGION'

    # Specify the S3 bucket and file/key
    bucket_name = 'your-s3-bucket'
    file_key = 'path/to/your/file.csv'

    # Create an S3 client
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

    # Download the file from S3 to a Pandas DataFrame
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        data = pd.read_csv(response['Body'])
        print("Data loaded successfully!")
        print(data.head())
    except Exception as e:
        print(f"Error: {e}")

load_data_task = PythonOperator(
    task_id = 'load_data_task',
    python_callable = load_data_from_s3,
    dag = dag
)

