import psycopg2
import boto3
import glob
import os
from typing import List
import pandas as pd
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator

host = "pizzasaleclus.crjjtklftimj.us-east-1.redshift.amazonaws.com"
dbname = "dev"
user = "awsuser"
password = "Wer121137"
port = "5439"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

def _get_files():
    df = pd.read_csv("/opt/airflow/dags/data/Pizza.csv")
    df['order_date'] = pd.to_datetime(df.order_date)
    df.to_csv("/opt/airflow/dags/data/output.csv", sep=',', index=False)

def _upload_files():
    aws_access_key_id = "ASIAW5OU4XNT2CEQJUKP"
    aws_secret_access_key = "FRyKna6gMUkIdnIk4bBJQya3Q3iK622TeWJEw7uL"
    aws_session_token = "FwoGZXIvYXdzEEQaDLBty3MVKWj7OA1MMSLKAdy2p1K6TGuCwLh+K/qvlLpnAr9uKViW5I+sBd2FjXTkvbTD+Lmsr/3uMn/Dn8v+AhycNc+DFDEXuQg0pupOENtZhVz1a2K9+eUPkoD/HlA+hgFHPmwsnvZ5zMskt+39M6H01NDRtB78oHyBkAxeSQcIgRAHhZNF7HnRQumebAB1+Ltd1aAhYad/SqIPpv92gh6tRVe6dgicxJpVgCElumTHZWlxZEkMLUwJuUozIRJEF+dAyg1iKxvAOUxm89TvBd9r2fXoIncDkV0ox7yEnQYyLa6tXAcp2QiILclghGFz6hL+D5ZV3uJObRokU60j3ZzPKtKU/1w3K7ssh1jx2Q=="

    s3 = boto3.resource(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token

    )
    s3.meta.client.upload_file("/opt/airflow/dags/data/output.csv", 'pizzasaleproject', 'output.csv')



def _create_tables():


    drop_table_query = "DROP TABLE IF EXISTS pizzasale"
    cur.execute(drop_table_query)
    conn.commit()

    create_table_queries = """
    CREATE TABLE IF NOT EXISTS pizzasale (
        
        order_details_id int,
        order_id int,
        pizza_id text,
        quantity int,
        order_date DATE,
        order_time TIME,
        unit_price float,
        total_price float,
        pizza_size text,
        pizza_category text,
        pizza_ingredients text,
        pizza_name text
        
    )
    """
    cur.execute(create_table_queries)
    conn.commit()

def _insert_data():
    copy_table_queries = """
    COPY pizzasale FROM 's3://pizzasaleproject/output.csv'
    ACCESS_KEY_ID 'ASIAW5OU4XNT2CEQJUKP'
    SECRET_ACCESS_KEY 'FRyKna6gMUkIdnIk4bBJQya3Q3iK622TeWJEw7uL'
    SESSION_TOKEN 'FwoGZXIvYXdzEEQaDLBty3MVKWj7OA1MMSLKAdy2p1K6TGuCwLh+K/qvlLpnAr9uKViW5I+sBd2FjXTkvbTD+Lmsr/3uMn/Dn8v+AhycNc+DFDEXuQg0pupOENtZhVz1a2K9+eUPkoD/HlA+hgFHPmwsnvZ5zMskt+39M6H01NDRtB78oHyBkAxeSQcIgRAHhZNF7HnRQumebAB1+Ltd1aAhYad/SqIPpv92gh6tRVe6dgicxJpVgCElumTHZWlxZEkMLUwJuUozIRJEF+dAyg1iKxvAOUxm89TvBd9r2fXoIncDkV0ox7yEnQYyLa6tXAcp2QiILclghGFz6hL+D5ZV3uJObRokU60j3ZzPKtKU/1w3K7ssh1jx2Q=='
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
    cur.execute(copy_table_queries)
    conn.commit()


    
with DAG(
    "etl",
    start_date=timezone.datetime(2022, 12, 19),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        
    )


    upload_files = PythonOperator(
        task_id="upload_files",
        python_callable=_upload_files,
    )
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )
    insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=_insert_data,
    )
    

    # [get_files, create_tables] >> process
    get_files >> upload_files >> create_tables >> insert_data


#if __name__ == "__main__":
#    main()