from airflow import DAG
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import io
import pandas as pd
import requests
import boto3
import os
import logging
from dotenv import load_dotenv
load_dotenv('.env')

aws_access_key_id = os.getenv('API_KEY')
aws_secret_access_key = os.getenv('SECRET_KEY')
region_name = 'ap-south-1'
# Specify the S3 bucket and file/key
bucket_name = 'airbnb-bigdata-nisha'
file_key = 'new_york_listings_2024.csv'

# Create an S3 client
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

# Download the file from S3 to a Pandas DataFrame
response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
data = pd.read_csv(response['Body'])
print("Data loaded successfully!")
print("Data loaded successfully!")
print(data.head())