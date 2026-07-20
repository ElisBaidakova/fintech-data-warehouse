from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pendulum
import os
import vertica_python

from config import (
    VERTICA_HOST,
    VERTICA_PORT,
    VERTICA_USER,
    VERTICA_PASSWORD,
    VERTICA_DATABASE
)


# Обновление витрины global_metrics за дату ds (вчерашняя дата)
def update_global_metrics(**context):
    # Получаем дату на день раньше execution_date
    ds = context['ds']
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

        # Читаем SQL-файл
        sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'update_mart.sql')
        with open(sql_file_path, 'r') as f:
            sql = f.read()

        # Выполняем MERGE с передачей параметра ds
        cur.execute(sql, (ds,))

        # Получаем количество обработанных строк
        rows_affected = cur.rowcount
        connection.commit()

        print(f"Дата: {ds} Обработано строк: {rows_affected}")

        if rows_affected == 0:
            print("Нет данных для обновления за эту дату.")
        else:
            print(f"Витрина global_metrics успешно обновлена за дату: {ds}")


@dag(
    schedule_interval='@daily',
    start_date=pendulum.parse('2022-10-01'),
    catchup=True,
    max_active_runs=1,
    tags=['datamart', 'global_metrics', 'incremental']
)
def datamart_update_dag():
    begin = DummyOperator(task_id="begin")

    update_metrics = PythonOperator(
        task_id='update_global_metrics',
        python_callable=update_global_metrics,
        provide_context=True,
    )

    begin >> update_metrics


dag_2 = datamart_update_dag()
