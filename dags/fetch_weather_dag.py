"""
## Enhanced Weather Data Fetch DAG

This DAG fetches comprehensive weather data from Open-Meteo API using the openmeteo_requests client.
It extracts current weather conditions, hourly temperature forecasts, and daily UV index data.
The data is saved in both raw and formatted structures using multiple formats (JSON, CSV, Parquet).

This is the first stage of the enhanced weather processing pipeline.
"""

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from typing import Dict


@dag(
    dag_id="fetch_weather_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "nutriweather", "retries": 3},
    tags=["weather", "fetch", "raw"],
    max_active_runs=1,
)
def fetch_weather_dag():    # Run the fetch_weather script with auto-dependency installation
    extract_weather_task = BashOperator(
        task_id="extract_weather_from_api",
        bash_command="cd /usr/local/airflow && python include/scripts/fetch_weather.py",
        cwd="/usr/local/airflow",
    )
    
    trigger_formatting = TriggerDagRunOperator(
        task_id="trigger_formatting",
        trigger_dag_id="format_weather_dag",
        wait_for_completion=False,
        conf={"triggered_by": "fetch_weather_dag"}
    )
    
    extract_weather_task >> trigger_formatting


fetch_weather_dag()
