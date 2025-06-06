"""
## Weather Data Format DAG

This DAG formats comprehensive raw weather data from Open-Meteo fetch into structured Parquet files.
It handles multiple data types: hourly temperature data, daily UV index data, current conditions,
and legacy format data. The processed data is optimized for analytics and time-series analysis.

Supports multiple input formats:
- CSV files (hourly and daily separately)
- JSON files (comprehensive response with metadata)
- Legacy CSV format (backward compatibility)

This is the second stage of the enhanced weather processing pipeline.
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
def format_weather_dag():      # Format weather using enhanced Python script with auto-dependency installation
    format_weather_task = BashOperator(
        task_id="format_weather",
        bash_command="cd /usr/local/airflow && python include/scripts/format_weather.py",
        cwd="/usr/local/airflow"
    )
    
    format_weather_task


# Instantiate the DAG
format_weather_dag()
