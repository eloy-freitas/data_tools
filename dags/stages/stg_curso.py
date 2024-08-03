from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from src.utils.connection.airflow_connection_factory import PostgresHook
from templates.stages.template_stage_multithread import StageMultiThread


with DAG(
    'dag_test',
    start_date=datetime(2024,8,3),
    catchup=False
):
    conn = PostgresHook().create_postgres_engine('conn_dbdw')

    query = "select * from dw.d_curso"
    table_name = "dw.d_curso_copia"
    stg = StageMultiThread(
        query=query,
        table_name_taget=table_name,
        conn_input=conn,
        conn_output=conn,
        consumers=5,
        max_rows_buffer=300000,
        yield_per=40000
    )
    task = PythonOperator(
        task_id='stg_curso',
        python_callable=stg.start
    )
