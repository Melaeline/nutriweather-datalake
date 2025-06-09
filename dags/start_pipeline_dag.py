"""
Complete NutriWeather Data Pipeline DAG - Orchestrates the entire data processing pipeline.
"""
from airflow import DAG
from config.dag_config import COMMON_DAG_CONFIG
from utils.task_utils import create_script_task, create_trigger_task

dag = DAG(
    dag_id="start_pipeline_dag",
    description="Complete NutriWeather data processing pipeline",
    tags=["pipeline", "complete"],
    **COMMON_DAG_CONFIG
)

# Tasks - using consistent script task approach
meals_fetch = create_script_task(
    task_id="fetch_meals_data",
    script_name="fetch_meals.py",
    dag=dag
)

weather_fetch = create_script_task(
    task_id="fetch_weather_data",
    script_name="fetch_weather.py",
    dag=dag
)

format_meals = create_script_task(
    task_id="format_meals_data",
    script_name="format_meals.py",
    dag=dag
)

format_weather = create_script_task(
    task_id="format_weather_data",
    script_name="format_weather.py",
    dag=dag
)

merge_data = create_script_task(
    task_id="merge_formatted_data",
    script_name="merge_formatted.py",
    dag=dag
)

trigger_indexing = create_trigger_task(
    task_id="trigger_elasticsearch_indexing",
    target_dag_id="index_elasticsearch_dag",
    wait_for_completion=True,
    dag=dag
)

# Dependencies
meals_fetch >> format_meals
weather_fetch >> format_weather
[format_meals, format_weather] >> merge_data >> trigger_indexing
