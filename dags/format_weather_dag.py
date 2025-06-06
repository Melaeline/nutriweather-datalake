"""
## Weather Data Format DAG

This DAG formats raw weather data from Open-Meteo fetch into structured Parquet files.
It reads the latest raw CSV file and saves it in Parquet format for efficient querying.

This is the second stage of the weather processing pipeline.
"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="format_weather_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "nutriweather", "retries": 3},
    tags=["weather", "format", "parquet"],
    max_active_runs=1,
)
def format_weather_dag():
    
    # Format weather using Python script
    format_weather_task = BashOperator(
        task_id="format_weather",
        bash_command="cd /usr/local/airflow && python include/scripts/format_weather.py",
        cwd="/usr/local/airflow"
    )
    
    format_weather_task


# Instantiate the DAG
format_weather_dag()
