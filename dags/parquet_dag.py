from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator

from datetime import datetime

from includes.parquet_dag.main import main
from includes.parquet_dag.functions import get_mapping, get_partition, \
    get_list_partitions, find_old_partition, drop_partition_from_db
from includes.parquet_dag.telegram import send_telegram_message


with DAG('parquet_dag', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
  
    get_list_partitions = PythonOperator(
        task_id='get_list_partitions',
        python_callable=get_list_partitions,
        provide_context=True
    )

    find_old_partition = ShortCircuitOperator(
        task_id='find_old_partition',
        python_callable=find_old_partition,
        provide_context=True
    )

    get_mapping = PythonOperator(
        task_id='get_mapping',
        python_callable=get_mapping,
        provide_context=True
    )

    get_partition = PythonOperator(
        task_id='get_partition',
        python_callable=get_partition,
        provide_context=True
    )

    check_parquet_partition = BranchPythonOperator(
        task_id='check_parquet_partition',
        python_callable=main,
        provide_context=True
    )

    drop_partition_from_db = PythonOperator(
        task_id='drop_partition_from_db',
        python_callable=drop_partition_from_db,
        provide_context=True
    )

    send_telegram_message = PythonOperator(
        task_id='send_telegram_message',
        python_callable=send_telegram_message,
        provide_context=True
    )

    get_list_partitions >> find_old_partition >> get_mapping
    get_mapping >> get_partition >> check_parquet_partition >> [drop_partition_from_db, send_telegram_message]
