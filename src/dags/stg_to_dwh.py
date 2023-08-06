import json

import pendulum
import vertica_python
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

HOST = 'vertica.tgcloudenv.ru'
PORT = '5433'
USER = 'farruhrusyandexru'
PASSWORD = 'JLlMNB9gxWc5A0h'

conn_info = {
    "host": HOST, "port": PORT,
    "user": USER, "password": PASSWORD,
    "autocommit": True,
}


def load_global_metrics(snap_date, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            insert into FARRUHRUSYANDEXRU__DWH.global_metrics
            with t0 as (
                select * from
                    FARRUHRUSYANDEXRU__STAGING.currencies c
                where 1=1
                    and currency_code_with = 420 -- USD
                    and date_update = '{snap_date}'::date-1
                ),
            t1 as (
                select
                    t0.date_update,
                    t.currency_code as currency_from,
                    t.account_number_from,
                    (t.amount * t0.currency_with_div) as amount
                from FARRUHRUSYANDEXRU__STAGING.transactions t
                join t0 on t.transaction_dt::date = t0.date_update
                    and t.currency_code = t0.currency_code
                where 1=1
                    and t.status = 'done' and t.account_number_from>0
                    and t.transaction_dt::date = '{snap_date}'::date-1
                union all
                select
                    transaction_dt::date as date_update,
                    currency_code as currency_from,
                    account_number_from,
                    amount
                from FARRUHRUSYANDEXRU__STAGING.transactions
                where 1=1
                    and currency_code = 420 -- USD
                    and status = 'done'
                    and account_number_from>0
                    and transaction_dt::date = '{snap_date}'::date-1
            )
            select
                date_update,
                currency_from,
                sum(amount) as amount_total,
                count(*) as cnt_transactions,
                round(sum(amount) / count(distinct account_number_from), 2) as avg_transactions_per_account,
                count(distinct account_number_from) as cnt_accounts_make_transactions
            from t1
            group by date_update,currency_from;
            """
        )
        return cur.fetchall()


@dag(
    schedule_interval="0 12 1 * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=True,
)
def dwh_dag():
    # load_global_metrics_task = PythonOperator(
    #     task_id="load_global_metrics",
    #     python_callable=load_global_metrics,
    #     op_kwargs={"snap_date": "2022-10-02"},
    # )

    load_global_metrics_task = PythonOperator(
        task_id="load_global_metrics",
        python_callable=load_global_metrics,
        op_kwargs={"snap_date": "{{ ds }}"},
    )

    load_global_metrics_task


_ = dwh_dag()
