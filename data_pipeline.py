from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from data_ingestion import ingest_data
from schema_creation import *

import pandas as pd
import psycopg2 as pg


def transform_data():
    # read raw data from the s3 bucket for transformation
    dev_dataframe = pd.read_csv("s3://datapipeline-raw/weather_raw_info.csv")

    # convert timestamp data into time
    dev_dataframe['sunrise'] = pd.to_datetime(dev_dataframe['sunrise'], unit = 's').dt.time
    dev_dataframe['sunset'] = pd.to_datetime(dev_dataframe['sunset'], unit = 's').dt.time
    dev_dataframe['timezone'] = pd.to_datetime(dev_dataframe['timezone'], unit = 's').dt.time

    # Kelvin to celsius with double precision
    dev_dataframe['temperature'] = dev_dataframe['temperature'].apply(lambda x: round(x - 273.15, 2))


    #load transformed data into s3 bucket
    dev_dataframe.to_csv("s3://datapipeline-prod/weather_prod_data.csv", index = False)

# load data into the table that reside in redshift data-warehouse
def load_data_to_redshift():
    # read transformed data from the s3 bucket for Loading
    dev_dataframe = pd.read_csv("s3://datapipeline-prod/weather_prod_data.csv")

    conn, cur = establish_connection()
    insert_query = "INSERT INTO weather_table (country, city_name, temperature, sunrise, sunset, timezone) VALUES (%s, %s, %s, %s, %s, %s)"
    data = [(row['country'], row['city_name'], row['temperature'], row['sunrise'], row['sunset'], row['timezone'])for i, row in dev_dataframe.iterrows()]
    
    cur.executemany(insert_query, data)
    conn.commit()
    close_connection(conn, cur)


# dag function that manage all task
with DAG(dag_id = 'data_pipeline', start_date=datetime(2023,1,1), schedule_interval = '@daily', catchup = False) as dag:
    
    transform_task = PythonOperator(
        task_id = 'transform_task',
        python_callable = transform_data,
    )

    load_task = PythonOperator(
        task_id = 'load_task',
        python_callable = load_data_to_redshift,
    )

    get_dataFrame = PythonOperator(
        task_id = 'get_dataFrame',
        python_callable = ingest_data,
    )

    schema_creation = PythonOperator(
        task_id = 'schema_creation',
        python_callable = create_schema,

    )

get_dataFrame >> schema_creation >> transform_task >> load_task
