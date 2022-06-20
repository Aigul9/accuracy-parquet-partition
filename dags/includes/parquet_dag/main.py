from includes.parquet_dag.cbf import CBF
from includes.parquet_dag.config import false_positive, host, port, webhdfs_prefix, hdfs_path
from includes.parquet_dag.utils import get_pandas_data_frame, read, modify_df, concat


def main(ti):
    columns_map = ti.xcom_pull(key='columns_map', task_ids='get_mapping')
    partition = ti.xcom_pull(key='records', task_ids='get_partition')
    partition_name = ti.xcom_pull(key='old_partition', task_ids='find_old_partition')

    df_db = get_pandas_data_frame(columns_map, partition)
    read_result = read(ti, host, port, webhdfs_prefix, hdfs_path, partition_name)

    if read_result is False:
        return 'send_telegram_message'

    df_parquet = modify_df(read_result)

    n = len(df_db)
    bloom_filter = CBF(n, false_positive)

    for row in df_db.iterrows():
        bloom_filter.add(concat(row))

    state = True
    for row in df_parquet.iterrows():
        row = concat(row)
        if not bloom_filter.check(row):
            state = False
            break

    state = False
    ti.xcom_push(key='partition_state', value=state)

    if state:
        return 'drop_partition_from_db'
    return 'send_telegram_message'
