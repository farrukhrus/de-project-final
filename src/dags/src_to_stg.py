import json

import boto3
import pendulum
import vertica_python
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator

HOST = 'vertica.tgcloudenv.ru'
PORT = '5433'
USER = 'farruhrusyandexru'
PASSWORD = 'JLlMNB9gxWc5A0h'

conn_info = {
    "host": HOST, "port": PORT,
    "user": USER, "password": PASSWORD,
    "autocommit": True,
}


def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id='YCAJEWXOyY8Bmyk2eJL-hlt2K',
        aws_secret_access_key='YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA',
    )

    s3_client.download_file(Bucket=bucket, Key=key,
                            Filename=f"/data/{key}")


def load_currencies_staging(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            """copy FARRUHRUSYANDEXRU__STAGING.currencies
            (currency_code,currency_code_with,date_update,currency_with_div)
            from local"""
            "'/data/currencies_history.csv' delimiter ','",
            buffer_size=65536,
        )
        return cur.fetchall()


def load_transactions_staging(file_num, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            """copy FARRUHRUSYANDEXRU__STAGING.transactions
            (operation_id,account_number_from,account_number_to,currency_code,
            country,status,transaction_type,amount,transaction_dt)
            from local"""
            f"'/data/transactions_batch_{file_num}.csv' delimiter ','",
            buffer_size=65536,
        )
        return cur.fetchall()


@dag(
    schedule_interval="0 12 1 * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=False,
)
def stg_dag():
    fetch_currencies_task = PythonOperator(
        task_id="fetch_currencies",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "final-project", "key": "currencies_history.csv"},
    )

    load_currencies_task = PythonOperator(
        task_id="load_currencies_staging",
        python_callable=load_currencies_staging,
    )

    fetch_transactions_task = []
    load_transactions_task = []
    for i in range(1, 2):
        fetch_transactions_task.append(
            PythonOperator(
                task_id=f"fetch_transactions_{i}",
                python_callable=fetch_s3_file,
                op_kwargs={
                    "bucket": "final-project",
                    "key": f"transactions_batch_{i}.csv",
                },
            )
        )

        load_transactions_task.append(
            PythonOperator(
                task_id=f"load_transactins_staging_{i}",
                python_callable=load_transactions_staging,
                op_kwargs={"file_num": i},
            )
        )

    (
        fetch_currencies_task
        >> fetch_transactions_task
        >> load_currencies_task
        >> load_transactions_task
    )


_ = stg_dag()
