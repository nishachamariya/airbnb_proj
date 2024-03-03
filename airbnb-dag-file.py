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
from airflow.operators.postgres_operator import PostgresOperator
import redshift_connector


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
    aws_access_key_id = 'xxxxx'
    aws_secret_access_key ='xxxx'
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
        return data
    except Exception as e:
        logging.error(f"Error: {e}")

    



# Task 2: Transform data
def transform_data(**kwargs):
    ti = kwargs['ti']
    
    # Directly access the DataFrame from the 'extract_data_task'
    df = ti.xcom_pull(task_ids='load_data_task')


    #cleaning the data
    df['baths'] = df['baths'].replace('Not specified', 1)
    df['baths'] = df['baths'].astype(float)

    df = df.drop_duplicates().reset_index(drop=True)
    df['airbnb_id'] = df.index
    df = df[['airbnb_id','id', 'name', 'host_id', 'host_name', 'neighbourhood_group',
        'neighbourhood', 'latitude', 'longitude', 'room_type', 'price',
        'minimum_nights', 'number_of_reviews', 'last_review',
        'reviews_per_month', 'calculated_host_listings_count',
        'availability_365', 'number_of_reviews_ltm', 'license', 'rating',
        'bedrooms', 'beds', 'baths']]

    #creating the reqired dimension table
    property_details= df[['name','last_review','room_type','minimum_nights','availability_365','license','rating','bedrooms','beds','baths']].reset_index(drop=True)
    property_details['property_id'] = property_details.index
    property_details = property_details[['property_id','name','last_review','room_type','minimum_nights','availability_365','license','rating','bedrooms','beds','baths']]

    host_dim = df[['host_id', 'host_name', 'calculated_host_listings_count']]
    host_dim = host_dim.drop_duplicates(subset=['host_id']).reset_index(drop=True)


    location_dim= df[['id','neighbourhood_group','neighbourhood','latitude','longitude']].reset_index(drop=True)
    location_dim['location_id'] = location_dim.index


    reviews = df[['number_of_reviews','reviews_per_month','number_of_reviews_ltm']].reset_index(drop=True)
    reviews['reviews_id'] = reviews.index
    reviews= reviews[['reviews_id','number_of_reviews', 'reviews_per_month', 'number_of_reviews_ltm']]


    #creating the fact table
    fact_table = df.merge(property_details, left_on='airbnb_id', right_on='property_id') \
                .merge(host_dim, left_on='host_id', right_on='host_id') \
                .merge(location_dim, left_on='airbnb_id', right_on='location_id') \
                .merge(reviews, left_on='airbnb_id', right_on='reviews_id') \
                    [['airbnb_id','property_id', 'host_id', 'location_id',
                'reviews_id','price']]

    # data_values = ",".join([str(tuple(row)) for row in fact_table.values])
    # print(data_values)
    return fact_table


# task:3
def load_data_into_redshift(**kwargs):
    ti = kwargs['ti']

    # Directly access the DataFrame from the 'transform_data_task'
    fact_table = ti.xcom_pull(task_ids='transform_data_task')

    redshift_conn_id = 'redshift_default'
    schema_name = 'airbnb'
    table_name = 'fact_table'

    conn = redshift_connector.connect(
        host="default-workgroup.533267136373.us-east-1.redshift-serverless.amazonaws.com",
        port=5439,
        database='dev',
        user='xxx',
        password='xxx'
    )

    cursor = conn.cursor()

    # Prepare data values for the SQL query
    data_values = ', '.join([f"({row['airbnb_id']}, {row['property_id']}, {row['host_id']}, {row['location_id']}, {row['reviews_id']}, {row['price']})" for _, row in fact_table.iterrows()])

    # SQL query to insert data into Redshift
    sql_query = f"""
        INSERT INTO {schema_name}.{table_name}
        (airbnb_id, property_id, host_id, location_id, reviews_id, price)
        VALUES
        {data_values};
    """

     # Execute the SQL query on Redshift
    cursor.execute(sql_query)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    


load_data_task = PythonOperator(
    task_id = 'load_data_task',
    python_callable = load_data_from_s3,
    dag = dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)


load_into_redshift_task = PythonOperator(
    task_id='load_into_redshift_task',
    python_callable=load_data_into_redshift,  # Assuming load_data_into_redshift is your function
    provide_context=True,
    dag=dag,
)


   
    

load_data_task >> transform_data_task >> load_into_redshift_task
