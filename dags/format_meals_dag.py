"""
## MealDB Data Format DAG

This DAG formats raw meal data from TheMealDB fetch into structured Parquet files.
It reads the latest raw JSON file, enhances it with estimated preparation time and 
temperature data, and saves it in Parquet format for efficient querying.

This is the second stage of the meal processing pipeline.
"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="format_meals_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "nutriweather", "retries": 3},
    tags=["mealdb", "format", "parquet"],
    max_active_runs=1,
)
def format_meals_dag():
    
    # Format meals using PySpark script
    format_meals_task = BashOperator(
        task_id="format_meals",
        bash_command="cd /usr/local/airflow && python include/scripts/format_meals.py",
        cwd="/usr/local/airflow"
    )
    
    format_meals_task


# Instantiate the DAG
format_meals_dag()
