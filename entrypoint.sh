#!/bin/bash

set -e  # Detener el script si un comando falla

export AIRFLOW_HOME=/usr/local/airflow

airflow db init

sleep 5

# Crea el usuario admin
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin_password

# Inicia el servidor web y el programador de Airflow
airflow webserver &  # Ejecuta el servidor web en segundo plano
airflow scheduler     # Inicia el scheduler
