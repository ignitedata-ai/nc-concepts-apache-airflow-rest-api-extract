"""Airflow DAG: extract NC Concepts data from Toast 
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from nc_concepts_toast_rest_extract_dly import config

# def log_execution_date(**kwargs: Dict[str, Any]) -> None:
#     execution_date = kwargs['execution_date']
#     logging.info(f'Execution date is: {execution_date}')

with DAG(
    dag_id='nc_concepts_toast_rest_extract_dly',
    # Only allow one task to run at a time to avoid overloading the API
    max_active_runs=1,
    concurrency=2,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    },
    description='Extract NC Concepts Toast Data and load into MySQL daily',
    schedule_interval='0 7 * * *', # Daily, midnight MST assuming Airflow is set to UTC
    start_date=datetime(2025, 11, 3),
    catchup=True,
) as dag:

    # PythonOperator(
    #     task_id='list_execution_date',
    #     python_callable=log_execution_date
    # )

    #Ingest Tasks
    
    ingest_complete = EmptyOperator(
        task_id='ingest_complete'
    )

    for restaurant in config.CONFIG_RESTAURANT_LIST:
        restaurant_external_id = restaurant["external_id"]
        restaurant_name_simple = restaurant["name_simple"]
        restaurant_name = restaurant["name"]
        restaurant_address = restaurant["address"]

        task_dict = {}

        restaurant_ingest_complete = EmptyOperator(
            task_id=f'{restaurant_name_simple}_ingest_complete'
        )

        restaurant_ingest_complete >> ingest_complete
    
        for ingest_task in config.CONFIG_INGEST_TASKS:
            task_id = ingest_task["task_id"]

            if ingest_task["task_type"] == "bearer_token":
                task_dict[task_id] = PythonOperator(
                    task_id=f'{restaurant_name_simple}_{task_id}',
                    python_callable=ingest_task["python_callable"],
                    op_kwargs={
                        'toast_conn_id': config.TOAST_CONN_ID,
                        'restaurant_external_id': restaurant_external_id,
                    }
                )

            if ingest_task["task_type"] == "get_and_load":
                task_dict[task_id] = PythonOperator(
                    task_id=f'{restaurant_name_simple}_{task_id}',
                    python_callable=ingest_task["python_callable"],
                    op_kwargs={
                        'mysql_conn_id': config.MYSQL_INGEST_DB_CONN_ID,
                        'api_url': ingest_task["api_url"],
                        'restaurant_external_id': restaurant_external_id,
                        'restaurant_name_simple': restaurant_name_simple,
                        'restaurant_name': restaurant_name,
                        'restaurant_address': restaurant_address,
                        'table_load_info': ingest_task["table_load_info"],
                        'api_caller_db_loader_class': ingest_task["api_caller_db_loader_class"]
                    }
                )

                task_dict[task_id] >> restaurant_ingest_complete

            for dep in ingest_task.get("depends_on", []):
                task_dict[dep] >> task_dict[task_id]

    # Processed Tasks
    processed_complete = EmptyOperator(
        task_id='processed_complete'
    )

    task_dict = {}
    for process_task in config.CONFIG_PROCESSED_TASKS:
        task_id = process_task["task_id"]

        task_dict[task_id] = SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id=process_task["conn_id"],
            sql=process_task["sql_file"]
        )

        for dep in process_task.get("depends_on", []):
            task_dict[dep] >> task_dict[task_id]
        
        ingest_complete >> task_dict[task_id] >> processed_complete
