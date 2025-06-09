"""
Simplified Data Merge Module - Elasticsearch Compatible
"""

import glob
import json
import os
import pandas as pd
from datetime import datetime
from spark_utils import get_spark_session, ensure_directory
import random


def load_latest_meals():
    """Load latest formatted meals data."""
    meals_dir = "/usr/local/airflow/include/formatted/meals"
    pattern = f"{meals_dir}/formatted_meals_*.parquet"
    files = glob.glob(pattern)
    
    if not files:
        return []
    
    # Filter out any directories or artifacts, get only .parquet files
    parquet_files = [f for f in files if os.path.isfile(f) and f.endswith('.parquet')]
    
    if not parquet_files:
        return []
    
    latest_file = max(parquet_files, key=os.path.getctime)
    print(f"Loading meals from: {latest_file}")
    df = pd.read_parquet(latest_file)
    return df.to_dict('records')


def load_latest_weather():
    """Load latest formatted weather data."""
    weather_dir = "/usr/local/airflow/include/formatted/weather"
    pattern = f"{weather_dir}/formatted_weather_*.json"
    files = glob.glob(pattern)
    
    if not files:
        return {}
    
    latest_file = max(files, key=os.path.getctime)
    with open(latest_file, 'r') as f:
        return json.load(f)


def load_advice_dataset():
    """Load advice dataset for temperature-based recommendations."""
    advice_file = "/usr/local/airflow/include/advice_dataset.csv"
    try:
        df = pd.read_csv(advice_file, sep=';')
        return df.to_dict('records')
    except:
        return []


def get_meal_advice(temperature, advice_data):
    """Get advice based on temperature from advice dataset."""
    if not advice_data:
        return get_simple_meal_advice(temperature)
    
    # Find closest temperature match
    closest_advice = min(advice_data, key=lambda x: abs(x['temperature'] - temperature))
    return closest_advice['advice']


def get_simple_meal_advice(temperature):
    """Fallback simple meal advice based on temperature."""
    if temperature < 10:
        return "Perfect weather for hot soups and warm comfort food!"
    elif temperature < 20:
        return "Great weather for hearty meals and moderate cooking!"
    elif temperature < 30:
        return "Pleasant weather for fresh dishes and light cooking!"
    else:
        return "Hot weather - perfect for cold dishes and minimal cooking!"


def get_temperature_category(temperature):
    """Categorize temperature."""
    if temperature < 5:
        return "cold"
    elif temperature < 15:
        return "cool"
    elif temperature < 25:
        return "moderate"
    elif temperature < 35:
        return "warm"
    else:
        return "hot"


def select_random_meal(meals_data):
    """Select a random meal for recommendation."""
    if not meals_data:
        return None
    return random.choice(meals_data)


def create_temperature_records(weather_data):
    """Create simple temperature time-series records."""
    records = []
    
    if not weather_data.get('hourly'):
        return records
    
    location = weather_data.get('location', {})
    
    for hour_data in weather_data['hourly']:
        records.append({
            "@timestamp": hour_data['time'].replace('T', ' ').replace('+00:00', ''),
            "temperature": hour_data['temperature_2m'],
            "timezone": location.get('timezone', 'UTC'),
            "location": location.get('name', 'Unknown'),
            "latitude": location.get('latitude'),
            "longitude": location.get('longitude')
        })
    
    return records


def create_enhanced_records(weather_data, meals_data, advice_data):
    """Create enhanced records with meal recommendations."""
    records = []
    
    current = weather_data.get('current', {})
    location = weather_data.get('location', {})
    daily = weather_data.get('daily', [])
    
    if not current.get('temperature_2m'):
        return records
    
    temp = current['temperature_2m']
    recommended_meal = select_random_meal(meals_data)
    advice = get_meal_advice(temp, advice_data)
    temp_category = get_temperature_category(temp)
    
    # Get UV index from daily data
    uv_index = daily[0]['uv_index_max'] if daily else None
    
    record = {
        "@timestamp": current['time'].replace('T', ' ').replace('+00:00', ''),
        "timezone": location.get('timezone', 'UTC'),
        "current_temperature": temp,
        "relative_humidity": current.get('relative_humidity_2m'),
        "wind_speed": current.get('wind_speed_10m'),
        "location": location.get('name', 'Unknown'),
        "latitude": location.get('latitude'),
        "longitude": location.get('longitude'),
        "recommended_advice": advice,
        "temperature_category": temp_category,
        "uv_index_max": uv_index
    }
    
    if recommended_meal:
        record.update({
            "suggested_meal": recommended_meal.get('meal_name'),
            "preparation_time": f"{recommended_meal.get('preparation_time', 30)} minutes",
            "instructions": recommended_meal.get('instructions', ''),
            "ingredients": recommended_meal.get('ingredients', ''),
            "meal_category": recommended_meal.get('category'),
            "meal_region": recommended_meal.get('region')
        })
    
    records.append(record)
    return records


def save_records(temp_records, enhanced_records):
    """Save records in JSONL format for Elasticsearch."""
    output_dir = "/usr/local/airflow/include/usage"
    ensure_directory(output_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save temperature records
    temp_file = f"{output_dir}/temperature_timeseries_{timestamp}.jsonl"
    with open(temp_file, 'w') as f:
        for record in temp_records:
            f.write(json.dumps(record) + '\n')
    
    # Save enhanced records
    enhanced_file = f"{output_dir}/enhanced_recommendations_{timestamp}.jsonl"
    with open(enhanced_file, 'w') as f:
        for record in enhanced_records:
            f.write(json.dumps(record) + '\n')
    
    return temp_file, enhanced_file


def main():
    spark = get_spark_session("MergeData")
    
    try:
        print("Starting data merge process...")
        
        # Load data
        meals_data = load_latest_meals()
        weather_data = load_latest_weather()
        advice_data = load_advice_dataset()
        
        print(f"Loaded {len(meals_data)} meals")
        print(f"Weather data available: {bool(weather_data)}")
        print(f"Loaded {len(advice_data)} advice entries")
        
        # Create records
        temp_records = create_temperature_records(weather_data)
        enhanced_records = create_enhanced_records(weather_data, meals_data, advice_data)
        
        # Save records
        temp_file, enhanced_file = save_records(temp_records, enhanced_records)
        
        print(f"Temperature records: {len(temp_records)} -> {temp_file}")
        print(f"Enhanced records: {len(enhanced_records)} -> {enhanced_file}")
        print("Data merge completed successfully!")
        
    except Exception as e:
        print(f"Error in merge process: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
