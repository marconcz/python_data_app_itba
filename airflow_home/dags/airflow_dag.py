from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os
sys.path.append('/usr/local/airflow')
import cheapshark  # Importar función del primer archivo

# Definir argumentos por defecto del DAG
default_args = {
    'owner': '2024_patricio_nahuel_perrone',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 17),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'cheapshark_to_redshift_dag',
    default_args=default_args,
    description='Carga de ofertas de CheapShark y carga en Redshift',
    schedule_interval=timedelta(days=1),  # Corre diariamente
)

# Tarea 1: Ejecutar la función que obtiene las ofertas de CheapShark
fetch_deals_task = PythonOperator(
    task_id='api_to_parquet',
    python_callable = cheapshark.api_to_parquet,
    op_kwargs={'start_date': '{{ ds }}'},  # Airflow pasará la fecha de ejecución
    dag=dag,
)

# Definir el orden de ejecución de las tareas
fetch_deals_task
