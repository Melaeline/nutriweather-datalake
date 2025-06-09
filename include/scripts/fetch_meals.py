"""
Simplified Meals Data Fetching Module
"""

import requests
import json
import string
from datetime import datetime
from spark_utils import get_spark_session, ensure_directory
from pyspark.sql.functions import explode, col


def fetch_all_meals():
    """Fetch all meals from TheMealDB API."""
    base_url = "https://www.themealdb.com/api/json/v1/1"
    all_meals = []
    seen_ids = set()
    
    print("Fetching meals from TheMealDB...")
    
    for letter in string.ascii_lowercase:
        try:
            response = requests.get(f"{base_url}/search.php?f={letter}", timeout=30)
            response.raise_for_status()
            
            meals = response.json().get('meals', []) or []
            for meal in meals:
                meal_id = meal.get('idMeal')
                if meal_id and meal_id not in seen_ids:
                    all_meals.append(meal)
                    seen_ids.add(meal_id)
                    
        except Exception as e:
            print(f"Error fetching meals for '{letter}': {e}")
            continue
    
    return all_meals


def main():
    spark = get_spark_session("FetchMeals")
    
    try:
        # Fetch meals data
        meals_data = fetch_all_meals()
        print(f"Fetched {len(meals_data)} unique meals")
        
        # Setup output directory
        output_dir = "/usr/local/airflow/include/raw/meals"
        ensure_directory(output_dir)
        
        # Save raw meals data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{output_dir}/raw_meals_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                "total_meals": len(meals_data),
                "fetch_timestamp": timestamp,
                "meals": meals_data
            }, f, indent=2)
        
        print(f"Raw meals saved to: {output_file}")
        
        # Create Spark DataFrame for validation
        df = spark.createDataFrame([{"meals": meals_data}])
        df_exploded = df.select(explode(col("meals")).alias("meal"))
        
        print(f"Spark validation: {df_exploded.count()} meals processed")
        
    except Exception as e:
        print(f"Error in meals fetch: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
