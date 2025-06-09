"""
MealDB Data Format DAG - Formats raw meal data into structured Parquet files.
"""

from airflow import DAG
from config.dag_config import COMMON_DAG_CONFIG
from utils.task_utils import create_script_task

dag = DAG(
    dag_id="format_meals_dag",
    description="Format raw meal data to Parquet",
    tags=["format", "meals", "parquet"],
    **COMMON_DAG_CONFIG
)

# Tasks
format_task = create_script_task(
    task_id="format_meals",
    script_name="format_meals.py",
    dag=dag
)
