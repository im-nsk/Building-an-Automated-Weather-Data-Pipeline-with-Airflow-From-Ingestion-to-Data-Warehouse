from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from data_ingestion import ingest_data
from schema_creation import *

import pandas as pd
import psycopg2 as pg


def transform_data(**context):
    # get dataFrame
    dev_dataframe = context["task_instance"].xcom_pull(task_ids="get_dataFrame")

    # convert timestamp data into time
    dev_dataframe['sunrise'] = pd.to_datetime(dev_dataframe['sunrise'], unit = 's').dt.time
    dev_dataframe['sunset'] = pd.to_datetime(dev_dataframe['sunset'], unit = 's').dt.time
    dev_dataframe['timezone'] = pd.to_datetime(dev_dataframe['timezone'], unit = 's').dt.time
    dev_dataframe['temperature'] = dev_dataframe['temperature'] - 273.15


# load data into the table that reside in redshift data-warehouse
def load_data_to_redshift(**context):
    dev_dataframe = context["task_instance"].xcom_pull(task_ids="get_dataFrame")

    # load data to the s3 bucket
    dev_dataframe.to_csv("s3://datapipeline-raw/weather_raw_data.csv")

    conn, cur = establish_connection()

    insert_query = "INSERT INTO weather_table (country, city_name, temperature, sunrise, sunset, timezone) VALUES (%s, %s, %s, %s, %s, %s)"
    data = [(row['country'], row['city_name'], row['sunrise'], row['sunset'], row['timezone'], row['temperature'])for _, row in dev_dataframe.iterrows()]
    
    cur.executemany(insert_query, data)
    conn.commit()
    
    close_connection(conn, cur)


# dag function that manage all task
with DAG(dag_id = 'etl_pipeline', start_date=datetime(2023,1,1), schedule_interval = '@daily', catchup = False) as dag:
    
    transform_task = PythonOperator(
        task_id = 'transform_task',
        python_callable = transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id = 'load_task',
        python_callable = load_data_to_redshift,
        provide_context=True
    )

    get_dataFrame = PythonOperator(
        task_id = 'get_dataFrame',
        python_callable = ingest_data,
        do_xcom_push = True,
    )

    schema_creation = PythonOperator(
        task_id = 'schema_creation',
        python_callable = create_schema,

    )

get_dataFrame >> schema_creation >> transform_task >> load_task
# create_schema()