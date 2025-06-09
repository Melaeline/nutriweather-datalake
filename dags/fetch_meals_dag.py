"""
MealDB Data Fetch DAG - Fetches all meals from TheMealDB API.
"""

from airflow import DAG
from airflow.decorators import task
from config.dag_config import COMMON_DAG_CONFIG, MEALDB_BASE_URL, RAW_DIR, WEATHER_API_TIMEOUT
from utils.task_utils import create_trigger_task
import requests
import json
import string
import os
from datetime import datetime

dag = DAG(
    dag_id="fetch_meals_dag",
    description="Fetch meals data from TheMealDB API",
    tags=["fetch", "meals", "raw"],
    **COMMON_DAG_CONFIG
)

@task(dag=dag)
def fetch_meals_data() -> str:
    """Fetch all meals from TheMealDB API with HDFS backup."""
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
    
    # Save raw data with HDFS backup
    meals_dir = f"{RAW_DIR}/meals"
    os.makedirs(meals_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(meals_dir, f"raw_meals_{timestamp}.json")
    
    # Import HDFS backup function
    import sys
    sys.path.append('/usr/local/airflow/include/scripts')
    from spark_utils import save_with_hdfs_backup
    
    # Save with HDFS backup
    save_with_hdfs_backup(
        filepath,
        {
            "total_meals": len(all_meals),
            "fetch_timestamp": timestamp,
            "meals": all_meals
        },
        "json"
    )
    
    print(f"Saved {len(all_meals)} meals locally and backed up to HDFS: {filepath}")
    return filepath

# Tasks
fetch_task = fetch_meals_data()
trigger_task = create_trigger_task(
    task_id="trigger_format_meals",
    target_dag_id="format_meals_dag",
    dag=dag
)

# Dependencies
fetch_task >> trigger_task
