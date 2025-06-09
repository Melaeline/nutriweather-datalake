"""
## Formatted Data Merge DAG

This DAG merges formatted meal and weather data to create analytical datasets.
It combines the latest formatted meals (Parquet) with weather data (JSON) to enable
cross-domain analysis for agricultural and nutritional insights.

This is the third stage of the complete data processing pipeline, executed after
both format_meals_dag and format_weather_dag have completed successfully.
"""

from airflow import DAG
from config.dag_config import COMMON_DAG_CONFIG
from utils.task_utils import create_script_task

dag = DAG(
    dag_id="merge_formatted_dag",
    description="Merge formatted meal and weather data",
    tags=["merge", "analytics"],
    **COMMON_DAG_CONFIG
)

# Tasks
merge_task = create_script_task(
    task_id="merge_formatted_data",
    script_name="merge_formatted.py",
    dag=dag
)
