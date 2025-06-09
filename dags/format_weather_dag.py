"""
Weather Data Format DAG - Formats raw weather data into structured Parquet files.
"""

from airflow import DAG
from config.dag_config import COMMON_DAG_CONFIG
from utils.task_utils import create_script_task

dag = DAG(
    dag_id="format_weather_dag",
    description="Format raw weather data to Parquet",
    tags=["format", "weather", "parquet"],
    **COMMON_DAG_CONFIG
)

# Tasks
format_task = create_script_task(
    task_id="format_weather",
    script_name="format_weather.py",
    dag=dag
)
