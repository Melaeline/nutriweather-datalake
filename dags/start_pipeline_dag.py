"""
## Complete NutriWeather Data Pipeline DAG

This DAG orchestrates the complete data processing pipeline:
1. Fetch meals and weather data in parallel
2. Format both datasets after successful fetch
3. Merge formatted data
4. Index to Elasticsearch

The pipeline ensures proper sequencing and error handling across all stages.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import json
import string
import os
from typing import List, Dict
from elasticsearch import Elasticsearch
import glob


@dag(
    dag_id="start_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "nutriweather", "retries": 2},
    tags=["pipeline", "complete", "parallel"],
    max_active_runs=1,
)
def start_pipeline_dag():
    
    # STAGE 1: FETCH DATA IN PARALLEL
    @task
    def fetch_meals_data() -> str:
        """Fetch all meals from TheMealDB API."""
        BASE_URL = "https://www.themealdb.com/api/json/v1/1"
        all_meals = []
        meal_ids = set()
        
        print("Starting meals data fetch...")
        for letter in string.ascii_lowercase:
            url = f"{BASE_URL}/search.php?f={letter}"
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                meals = response.json().get('meals', []) or []
                
                for meal in meals:
                    meal_id = meal.get('idMeal')
                    if meal_id and meal_id not in meal_ids:
                        all_meals.append(meal)
                        meal_ids.add(meal_id)
                        
            except requests.RequestException as e:
                print(f"Error fetching meals for letter '{letter}': {e}")
        
        # Save raw meal data
        raw_dir = "/usr/local/airflow/include/raw/meals"
        os.makedirs(raw_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_meals_{timestamp}.json"
        file_path = os.path.join(raw_dir, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({"total_meals": len(all_meals), "meals": all_meals}, f, indent=2)
        
        print(f"Saved {len(all_meals)} meals to {file_path}")
        return file_path

    fetch_weather_task = BashOperator(
        task_id="fetch_weather_data",
        bash_command="cd /usr/local/airflow && python include/scripts/fetch_weather.py",
        cwd="/usr/local/airflow"
    )

    # STAGE 2: FORMAT DATA AFTER FETCH COMPLETES
    format_meals_task = BashOperator(
        task_id="format_meals_data",
        bash_command="cd /usr/local/airflow && python include/scripts/format_meals.py",
        cwd="/usr/local/airflow"
    )

    format_weather_task = BashOperator(
        task_id="format_weather_data",
        bash_command="cd /usr/local/airflow && python include/scripts/format_weather.py",
        cwd="/usr/local/airflow"
    )

    # STAGE 3: MERGE FORMATTED DATA
    merge_data_task = BashOperator(
        task_id="merge_formatted_data",
        bash_command="cd /usr/local/airflow && python include/scripts/merge_formatted.py",
        cwd="/usr/local/airflow"
    )

    # STAGE 4: TRIGGER ELASTICSEARCH INDEXING DAG
    trigger_indexing_task = TriggerDagRunOperator(
        task_id="trigger_elasticsearch_indexing",
        trigger_dag_id="index_elasticsearch_dag",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    # PIPELINE DEPENDENCIES
    meals_fetch = fetch_meals_data()
    
    # Parallel fetch operations complete before formatting
    meals_fetch >> format_meals_task
    fetch_weather_task >> format_weather_task
    
    # Sequential processing after formatting
    [format_meals_task, format_weather_task] >> merge_data_task >> trigger_indexing_task


start_pipeline_dag()
