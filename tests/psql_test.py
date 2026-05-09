from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'Loginov Petr',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'psql_test',
    default_args=default_args,
    description='Test PostgreSQL connection',
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
)

# Способ 1:直接用 PostgresOperator
test_query = PostgresOperator(
    task_id='test_query',
    postgres_conn_id='postgres_default',  # ID подключения в Airflow
    sql='SELECT version();',
    dag=dag,
)

# Способ 2: Через хук для более сложной логики
def test_connection():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT current_database(), current_user;")
        result = cursor.fetchone()
        logging.info(f"Connected to DB: {result[0]}, User: {result[1]}")
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        raise

test_hook = PythonOperator(
    task_id='test_hook_connection',
    python_callable=test_connection,
    dag=dag,
)

test_query >> test_hook 