"""
Complete NutriWeather Data Pipeline DAG - Orchestrates the entire data processing pipeline.
"""
from airflow import DAG
from airflow.decorators import task
from config.dag_config import COMMON_DAG_CONFIG, MEALDB_BASE_URL, RAW_DIR, WEATHER_API_TIMEOUT
from utils.task_utils import create_script_task, create_trigger_task
import requests
import json
import string
import os
from datetime import datetime

dag = DAG(
    dag_id="start_pipeline_dag",
    description="Complete NutriWeather data processing pipeline",
    tags=["pipeline", "complete"],
    **COMMON_DAG_CONFIG
)

@task(dag=dag)
def fetch_meals_data() -> str:
    """Fetch meals data inline for the complete pipeline."""
    all_meals = []
    meal_ids = set()
    
    for letter in string.ascii_lowercase:
        url = f"{MEALDB_BASE_URL}/search.php?f={letter}"
        try:
            response = requests.get(url, timeout=WEATHER_API_TIMEOUT)
            response.raise_for_status()
            meals = response.json().get('meals', []) or []
            
            for meal in meals:
                meal_id = meal.get('idMeal')
                if meal_id and meal_id not in meal_ids:
                    all_meals.append(meal)
                    meal_ids.add(meal_id)
                    
        except requests.RequestException as e:
            print(f"Error fetching meals for letter '{letter}': {e}")
    
    # Save raw data
    meals_dir = f"{RAW_DIR}/meals"
    os.makedirs(meals_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(meals_dir, f"raw_meals_{timestamp}.json")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({"total_meals": len(all_meals), "meals": all_meals}, f, indent=2)
    
    print(f"Saved {len(all_meals)} meals to {filepath}")
    return filepath

# Tasks
meals_fetch = fetch_meals_data()
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
