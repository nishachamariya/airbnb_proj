from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import io
import pandas as pd
import requests
import boto3
import os
from dotenv import load_dotenv
import logging

load_dotenv()

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 29),
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
    aws_access_key_id = os.getenv('API_KEY')
    aws_secret_access_key = os.getenv('SECRET_KEY')
    region_name = 'ap-south-1'

    # Specify the S3 bucket and file/key
    bucket_name = 'airbnb-bigdata-nisha'
    file_key = 'new_york_listings_2024.csv'

    # Create an S3 client
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

    # Download the file from S3 to a Pandas DataFrame
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        data = pd.read_csv(response['Body'])
        print("Data loaded successfully!")
        logging.info("Data loaded successfully!")
        logging.info(data.head())
    except Exception as e:
        logging.error(f"Error: {e}")

load_data_task = PythonOperator(
    task_id = 'load_data_task',
    python_callable = load_data_from_s3,
    dag = dag
)



load_data_task