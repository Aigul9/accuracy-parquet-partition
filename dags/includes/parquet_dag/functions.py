from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

from includes.parquet_dag.config import original_table_name
from includes.parquet_dag.utils import stringify


def get_mapping(ti):
    partition_name = ti.xcom_pull(key='old_partition', task_ids='find_old_partition')
    request = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name='{partition_name}'
        ORDER BY ordinal_position;
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    columns_map = cursor.fetchall()
    ti.xcom_push(key='columns_map', value=columns_map)


def get_partition(ti):
    partition_name = ti.xcom_pull(key='old_partition', task_ids='find_old_partition')
    request = f'SELECT * FROM {partition_name} ORDER BY id;'
    pg_hook = PostgresHook(postgres_conn_id='postgres_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    records = cursor.fetchall()
    # convert to 2d list, since tuples are immutable
    records = [list(row) for row in records]
    for j in range(len(records[0])):
        # convert datetime to string, since datetime is not JSON serializable
        if isinstance(records[0][j], datetime):
            for i in range(len(records)):
                records[i][j] = stringify(records[i][j])
    ti.xcom_push(key='records', value=records)


def get_list_partitions(ti):
    request = 'SELECT relname FROM pg_class WHERE oid IN (SELECT inhrelid FROM pg_inherits);'
    pg_hook = PostgresHook(postgres_conn_id='postgres_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    list_partitions = cursor.fetchall()
    ti.xcom_push(key='list_partitions', value=list_partitions)


def find_old_partition(ti):
    list_partitions = ti.xcom_pull(key='list_partitions', task_ids='get_list_partitions')
    request = f"SELECT days FROM metadata WHERE table_name = '{original_table_name}';"
    pg_hook = PostgresHook(postgres_conn_id='postgres_mdm_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    retention_days = cursor.fetchall()[0][0]
    old_partition = None

    pg_hook = PostgresHook(postgres_conn_id='postgres_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for table_name in list_partitions:
        table_name = table_name[0]
        request = f'SELECT max(timestamp) FROM {table_name};'
        cursor.execute(request)
        max_date = cursor.fetchall()
        diff = get_diff_in_days(datetime.now(), max_date[0][0])
        
        if diff > retention_days:
            old_partition = table_name
            break
        
    ti.xcom_push(key='old_partition', value=old_partition)

    if old_partition == None:
        return False
    return True


def get_diff_in_days(d1, d2):
    return (d1 - d2).days


def get_diff_in_months(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def drop_partition_from_db(ti):
    partition_name = ti.xcom_pull(key='old_partition', task_ids='find_old_partition')
    request = f'ALTER TABLE {original_table_name} DETACH PARTITION {partition_name};'
    pg_hook = PostgresHook(postgres_conn_id='postgres_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    request = f'DROP TABLE {partition_name};'
    cursor.execute(request)
    connection.commit()
