import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine

from airflow.utils.dates import days_ago
from Spotify_etl import spotify_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023, 1, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag  = DAG (
    'spotify_final_dag',
    default_args = default_args,
    description = 'Spotify ETL process 1-min',
    Schedule_interval = dt.timedelta(minutes = 50),
)

def ETL():
    print('started')
    df = spotify_etl()
    #print(df)
    conn = BaseHook.get_connection('mysql_conn')
    engine = create_engine(f'mysql+mysqlconnector://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df.to_sql('songs', engine, if_exists = 'replace', index = False)

with dag:
    create_table = PythonOperator(
        tast_id = 'create_table',
        python_callable = ETL,
        dag = dag 
    )
    run_etl = PythonOperator(
        task_id = 'spotify_etl_final',
        python_callable = ETL,
        dag =dag,
    )

    create_table >> run_etl