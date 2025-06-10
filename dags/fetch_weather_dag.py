"""
Weather Data Fetch DAG - Fetches weather data from Open-Meteo API.
Can run frequently without API rate limiting issues.
"""
from airflow import DAG
from config.dag_config import SCHEDULED_DAG_CONFIG
from utils.task_utils import create_script_task, create_trigger_task

dag = DAG(
    dag_id="fetch_weather_dag",
    description="Fetch weather data from Open-Meteo API - supports frequent execution",
    tags=["fetch", "weather", "raw", "scheduled"],
    **SCHEDULED_DAG_CONFIG
)

# Tasks
fetch_task = create_script_task(
    task_id="fetch_weather_data",
    script_name="fetch_weather.py",
    dag=dag
)

trigger_task = create_trigger_task(
    task_id="trigger_format_weather",
    target_dag_id="format_weather_dag",
    dag=dag
)

# Dependencies
fetch_task >> trigger_task
