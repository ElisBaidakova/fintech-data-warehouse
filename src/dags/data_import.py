from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import boto3
import pendulum
import os
import vertica_python

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    VERTICA_HOST,
    VERTICA_PORT,
    VERTICA_USER,
    VERTICA_PASSWORD,
    VERTICA_DATABASE,
    S3_BUCKET,
    LOCAL_DATA_DIR,
    STAGING_SCHEMA
)


def ensure_dir_exists():
    if not os.path.exists(LOCAL_DATA_DIR):
        os.makedirs(LOCAL_DATA_DIR)


def fetch_s3_file(key: str, **context) -> str:
    ensure_dir_exists()

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    local_filepath = os.path.join(LOCAL_DATA_DIR, key.replace('/', '_'))

    s3_client.download_file(
        Bucket=S3_BUCKET,
        Key=key,
        Filename=local_filepath
    )

    return local_filepath


def load_currencies_to_vertica(**context):
    local_filepath = context['ti'].xcom_pull(task_ids='fetch_currencies_history_csv')

    conn_info = {
        'host': VERTICA_HOST,
        'port': VERTICA_PORT,
        'user': VERTICA_USER,
        'password': VERTICA_PASSWORD,
        'database': VERTICA_DATABASE,
        'read_timeout': 600
    }

    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()

        table_name = STAGING_SCHEMA + ".currencies"
        rejected_table = STAGING_SCHEMA + ".currencies_rejected"

        copy_query = (
            "COPY " + table_name + " (\n"
            "    currency_code, currency_code_with, date_update, currency_with_div\n"
            ") FROM LOCAL '" + local_filepath + "'\n"
            "DELIMITER ','\n"
            "SKIP 1\n"
            "REJECTED DATA AS TABLE " + rejected_table + ";"
        )

        cur.execute(copy_query)
        connection.commit()


def load_transactions_to_vertica(batch_number: int, **context):
    task_id_fetch = 'fetch_transactions_batch_' + str(batch_number) + '_csv'
    local_filepath = context['ti'].xcom_pull(task_ids=task_id_fetch)

    conn_info = {
        'host': VERTICA_HOST,
        'port': VERTICA_PORT,
        'user': VERTICA_USER,
        'password': VERTICA_PASSWORD,
        'database': VERTICA_DATABASE,
        'read_timeout': 600
    }

    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()

        table_name = STAGING_SCHEMA + ".transactions"
        rejected_table = STAGING_SCHEMA + ".transactions_rejected"

        copy_query = (
            "COPY " + table_name + " (\n"
            "    operation_id, account_number_from, account_number_to,\n"
            "    currency_code, country, status, transaction_type, amount, transaction_dt\n"
            ") FROM LOCAL '" + local_filepath + "'\n"
            "DELIMITER ','\n"
            "SKIP 1\n"
            "REJECTED DATA AS TABLE " + rejected_table + ";"
        )

        cur.execute(copy_query)
        connection.commit()


@dag(
    schedule_interval='@daily',
    start_date=pendulum.parse('2022-10-01'),
    catchup=True,
    max_active_runs=1,
    tags=['staging', 'final', 'project', 'vertica', 'iterative']
)
def dwh_staging_load_dag():
    begin = DummyOperator(task_id="begin")

    # Задача для currencies
    fetch_currencies = PythonOperator(
        task_id='fetch_currencies_history_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'key': 'currencies_history.csv'},
    )

    load_currencies = PythonOperator(
        task_id='load_currencies_history',
        python_callable=load_currencies_to_vertica,
    )

    # Задачи для transactions (итеративно по 10 батчам)
    fetch_transactions_tasks = []
    load_transactions_tasks = []

    for i in range(1, 11):
        fetch_task = PythonOperator(
            task_id='fetch_transactions_batch_' + str(i) + '_csv',
            python_callable=fetch_s3_file,
            op_kwargs={'key': 'transactions_batch_' + str(i) + '.csv'},
        )

        load_task = PythonOperator(
            task_id='load_transactions_batch_' + str(i),
            python_callable=load_transactions_to_vertica,
            op_kwargs={'batch_number': i},
        )

        fetch_transactions_tasks.append(fetch_task)
        load_transactions_tasks.append(load_task)

    begin >> fetch_currencies >> load_currencies

    for fetch_task, load_task in zip(fetch_transactions_tasks, load_transactions_tasks):
        begin >> fetch_task >> load_task


dag_1 = dwh_staging_load_dag()
