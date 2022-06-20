import pandas as pd


def get_pandas_data_frame(col_name_data_type_map, records):
    table_columns = [item[0] for item in col_name_data_type_map]
    df = pd.DataFrame(records, columns=table_columns)
    return df


def read(ti, host, port, webhdfs_prefix, hdfs_path, partition_name):
    try:
        return pd.read_parquet(f'http://{host}:{port}/{webhdfs_prefix}/{hdfs_path}/{partition_name}.parquet?op=OPEN')
    except Exception as ex:
        ti.xcom_push(key='exception', value=type(ex).__name__)
        return False
    

def modify_df(df):
    df = df.sort_values(by=['id'])
    df = df.reset_index(drop=True)
    df['timestamp'] = df['timestamp'].apply(lambda x: stringify(x))
    return df


def concat(r):
    return f"{r[1]['id']} {r[1]['name']} {r[1]['timestamp']}"


def stringify(value):
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")
  