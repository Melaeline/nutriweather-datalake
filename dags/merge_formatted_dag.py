"""
## Formatted Data Merge DAG

This DAG merges formatted meal and weather data to create analytical datasets.
It combines the latest formatted meals (Parquet) with weather data (JSON) to enable
cross-domain analysis for agricultural and nutritional insights.

This is the third stage of the complete data processing pipeline, executed after
both format_meals_dag and format_weather_dag have completed successfully.
"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="merge_formatted_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "nutriweather", "retries": 3},
    tags=["merge", "analytics", "parquet", "json"],
    max_active_runs=1,
)
def merge_formatted_dag():
    
    # Merge formatted data using enhanced Python script
    merge_formatted_task = BashOperator(
        task_id="merge_formatted",
        bash_command="cd /usr/local/airflow && python include/scripts/merge_formatted.py",
        cwd="/usr/local/airflow"
    )

    return merge_formatted_task


# Instantiate the DAG
merge_formatted_dag()
