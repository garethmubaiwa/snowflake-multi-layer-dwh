'''
DAG Definition for Data Warehouse Pipeline
This DAG orchestrates the ETL process for a data warehouse, including loading raw data into the
bronze layer, processing it into the silver layer, and finally transforming it into the gold layer for analytics.
- load_bronze: Executes a stored procedure to load raw data into the bronze layer.
- process_silver: Executes a stored procedure to process data from the bronze layer into the silver layer.
- process_gold: Executes a stored procedure to transform data from the silver layer into the gold layer for analytics.
'''

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'dwh_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    load_bronze = SnowflakeOperator(
        task_id='load_raw_data',
        snowflake_conn_id='snowflake_default',
        sql='CALL raw.load_raw_data();',
        warehouse='dev_wh',
        database='dev_db',
        schema='RAW'
    )
    
    process_silver = SnowflakeOperator(
        task_id='process_silver_layer',
        snowflake_conn_id='snowflake_default',
        sql='CALL staging.process_silver_layer();',
        warehouse='dev_wh',
        database='dev_db',
        schema='STAGING'
    )

    process_gold = SnowflakeOperator(
        task_id='process_gold_layer',
        snowflake_conn_id='snowflake_default',
        sql='CALL core.process_gold_layer();',
        warehouse='dev_wh',
        database='dev_db',
        schema='CORE'
    )

    load_bronze >> process_silver >> process_gold

