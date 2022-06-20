from airflow.providers.telegram.hooks.telegram import TelegramHook
from includes.parquet_dag.config import hdfs_path


def send_telegram_message(ti):
    telegram_hook = TelegramHook(telegram_conn_id='tg_conn_id')
    exception = ti.xcom_pull(key='exception', task_ids='check_parquet_partition')
    old_partition = ti.xcom_pull(key='old_partition', task_ids='find_old_partition')

    if exception is not None:
        exception_case = {
            'HTTPError': 'File not found',
            'OSError': 'File corrupted'
        }

        telegram_hook.send_message({
            'text': f"<b>{exception_case[exception]}</b>\n\n<b>Path:</b> <code>'{hdfs_path}/{old_partition}.parquet'</code>"
        })
    else:
        telegram_hook.send_message({
            'text': f"<b>Failure of parquet check</b>\n\n<b>Partition:</b> <code>'{old_partition}'</code>\
            \n<b>Parquet:</b> <code>'{old_partition}.parquet'</code>"
        })
