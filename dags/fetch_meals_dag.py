"""
## MealDB Data Fetch DAG

This DAG fetches all meals from TheMealDB API and saves them as raw JSON data.
It fetches meals by searching through each letter of the alphabet and saves
the results to the include/raw directory for further processing.

This is the first stage of the meal processing pipeline.
"""

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import json
import string
import os
from typing import List, Dict


@dag(
    dag_id="fetch_meals_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "nutriweather", "retries": 3},
    tags=["mealdb", "fetch", "raw"],
    max_active_runs=1,
)
def fetch_meals_dag():
    
    @task
    def extract_meals_from_api() -> List[Dict]:
        """Extract all meals from TheMealDB API by searching through each letter."""
        BASE_URL = "https://www.themealdb.com/api/json/v1/1"
        all_meals = []
        meal_ids = set()
        
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
        
        print(f"Total unique meals found: {len(all_meals)}")
        return all_meals
    
    @task
    def save_raw_meals(meals: List[Dict]) -> None:
        """Save raw meal data to JSON file."""
        raw_dir = "/usr/local/airflow/include/raw/meals"
        os.makedirs(raw_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_meals_{timestamp}.json"
        file_path = os.path.join(raw_dir, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({"total_meals": len(meals), "meals": meals}, f, indent=2)
        
        print(f"Saved {len(meals)} meals to {file_path}")
    
    trigger_formatting = TriggerDagRunOperator(
        task_id="trigger_formatting",
        trigger_dag_id="format_meals_dag",
        wait_for_completion=False,
        conf={"triggered_by": "fetch_meals_dag"}
    )
    
    raw_meals = extract_meals_from_api()
    save_raw_meals(raw_meals) >> trigger_formatting


fetch_meals_dag()
