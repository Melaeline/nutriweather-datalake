"""
Merge Formatted Data Module - Elasticsearch Compatible

This module merges formatted meal and weather data to create clean, flat records
optimized for Elasticsearch indexing and Kibana dashboarding.
"""

import os
import json
import glob
import pandas as pd
from datetime import datetime, timedelta
import traceback
import hashlib


def get_latest_file(directory, pattern):
    """Get the latest file matching pattern in directory."""
    full_pattern = os.path.join(directory, pattern)
    files = glob.glob(full_pattern)
    if not files:
        raise FileNotFoundError(f"No files found matching {full_pattern}")
    return max(files, key=os.path.getctime)


def load_latest_meals_data():
    """Load the latest formatted meals data from Parquet."""
    meals_dir = "/usr/local/airflow/include/formatted/meals"
    try:
        latest_meals_file = get_latest_file(meals_dir, "formatted_meals_*.parquet")
        print(f"Loading meals data from: {latest_meals_file}")
        df = pd.read_parquet(latest_meals_file)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error loading meals data: {e}")
        return []


def load_latest_weather_data():
    """Load the latest formatted weather data from JSON."""
    weather_dir = "/usr/local/airflow/include/formatted/weather"
    try:
        latest_weather_file = get_latest_file(weather_dir, "formatted_weather_*.json")
        print(f"Loading weather data from: {latest_weather_file}")
        with open(latest_weather_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading weather data: {e}")
        return {}


def generate_temperature_records(weather_data):
    """Generate simple temperature records for time-series analysis."""
    records = []
    
    if not weather_data.get('hourly'):
        return records
    
    location = weather_data.get('location', {})
    timezone = location.get('timezone', 'UTC')
    
    for hourly_record in weather_data['hourly']:
        if hourly_record.get('temperature_2m') is not None:
            records.append({
                "@timestamp": hourly_record['date'].replace(' UTC', ''),
                "temperature": round(hourly_record['temperature_2m'], 1),
                "timezone": timezone,
                "location": location.get('name', 'Unknown'),
                "latitude": location.get('coordinates', {}).get('latitude'),
                "longitude": location.get('coordinates', {}).get('longitude')
            })
    
    return records


def get_meal_advice(temperature):
    """Get meal advice based on temperature - deterministic."""
    if temperature < 5:
        return "Extremely cold weather calls for hearty stews and hot beverages. Stay warm and nourished!"
    elif temperature < 10:
        return "Perfect weather for hot soups and warm comfort food. Stay cozy!"
    elif temperature < 15:
        return "Cool weather ideal for warm meals and moderate cooking. Enjoy balanced nutrition!"
    elif temperature < 20:
        return "Great weather for hearty meals and moderate cooking. Enjoy balanced nutrition!"
    elif temperature < 25:
        return "Pleasant weather for fresh salads and light cooking. Stay hydrated!"
    elif temperature < 30:
        return "Warm weather perfect for fresh dishes and minimal cooking. Keep cool!"
    else:
        return "Hot weather calls for cold dishes and minimal cooking. Stay refreshed!"


def get_temperature_category(temperature):
    """Categorize temperature for meal selection."""
    if temperature < 10:
        return "cold"
    elif temperature < 20:
        return "moderate"
    elif temperature < 30:
        return "warm"
    else:
        return "hot"


def select_recommended_meal(meals_data, temperature, date_str):
    """Select a meal recommendation deterministically based on temperature and date."""
    if not meals_data:
        return None
    
    # Get temperature category
    temp_category = get_temperature_category(temperature)
    
    # Filter meals based on temperature preference
    if temp_category == "cold":
        # Cold weather - prefer hot meals (lower temperature meals)
        preferred = [m for m in meals_data if m.get('temperature', 15) <= 15]
    elif temp_category == "hot":
        # Hot weather - prefer cold meals (higher temperature meals)
        preferred = [m for m in meals_data if m.get('temperature', 15) >= 20]
    else:
        # Moderate/warm weather - prefer moderate temperature meals
        preferred = [m for m in meals_data if 15 <= m.get('temperature', 15) <= 25]
    
    # If no preferred meals found, use all meals
    if not preferred:
        preferred = meals_data
    
    # Create deterministic selection using hash of date and temperature
    selection_seed = f"{date_str}_{temp_category}_{int(temperature)}"
    hash_value = int(hashlib.md5(selection_seed.encode()).hexdigest(), 16)
    
    # Select meal based on hash
    selected_index = hash_value % len(preferred)
    return preferred[selected_index]


def generate_enhanced_records(weather_data, meals_data):
    """Generate enhanced records with meal recommendations."""
    records = []
    
    if not weather_data.get('current'):
        return records
    
    current = weather_data['current']
    location = weather_data.get('location', {})
    daily = weather_data.get('daily', [])
    
    current_temp = current.get('temperature_2m')
    if current_temp is None:
        return records
    
    # Get UV index from daily data
    uv_index = None
    if daily:
        uv_index = daily[0].get('uv_index_max')
    
    # Get current date for deterministic selection
    current_time = current.get('time', datetime.now().isoformat())
    date_str = current_time.split('T')[0] if 'T' in current_time else current_time.split(' ')[0]
    
    # Select recommended meal deterministically
    recommended_meal = select_recommended_meal(meals_data, current_temp, date_str)
    
    # Create enhanced record
    record = {
        "@timestamp": current_time.replace(' UTC', ''),
        "timezone": location.get('timezone', 'UTC'),
        "current_temperature": round(current_temp, 1),
        "relative_humidity": current.get('relative_humidity_2m'),
        "wind_speed": current.get('wind_speed_10m'),
        "location": location.get('name', 'Unknown'),
        "latitude": location.get('coordinates', {}).get('latitude'),
        "longitude": location.get('coordinates', {}).get('longitude'),
        "recommended_advice": get_meal_advice(current_temp),
        "temperature_category": get_temperature_category(current_temp)
    }
    
    # Add UV index if available
    if uv_index is not None:
        record["uv_index_max"] = round(uv_index, 1)
    
    # Add meal recommendation if available
    if recommended_meal:
        record.update({
            "suggested_meal": recommended_meal.get('meal_name'),
            "preparation_time": f"{recommended_meal.get('preparation_time', 30)} minutes",
            "instructions": recommended_meal.get('instructions', '').replace(';', '. ')[:200] + "...",
            "ingredients": recommended_meal.get('ingredients', '').replace(';', ', ')[:150] + "...",
            "meal_category": recommended_meal.get('category'),
            "meal_region": recommended_meal.get('region')
        })
    
    records.append(record)
    return records


def save_elasticsearch_records(temperature_records, enhanced_records):
    """Save records in Elasticsearch-compatible format."""
    usage_dir = "/usr/local/airflow/include/usage"
    os.makedirs(usage_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save temperature records (for time-series analysis)
    temp_filename = f"temperature_timeseries_{timestamp}.jsonl"
    temp_filepath = os.path.join(usage_dir, temp_filename)
    
    with open(temp_filepath, 'w') as f:
        for record in temperature_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"Temperature records saved to: {temp_filepath}")
    print(f"Total temperature records: {len(temperature_records)}")
    
    # Save enhanced records (for dashboarding)
    enhanced_filename = f"enhanced_recommendations_{timestamp}.jsonl"
    enhanced_filepath = os.path.join(usage_dir, enhanced_filename)
    
    with open(enhanced_filepath, 'w') as f:
        for record in enhanced_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"Enhanced records saved to: {enhanced_filepath}")
    print(f"Total enhanced records: {len(enhanced_records)}")
    
    return temp_filepath, enhanced_filepath


def main():
    """Main execution function."""
    print("=== Starting Elasticsearch-Compatible Data Merge ===")
    
    try:
        # Load data
        meals_data = load_latest_meals_data()
        weather_data = load_latest_weather_data()
        
        print(f"Loaded {len(meals_data)} meals")
        print(f"Weather data available: {bool(weather_data)}")
        
        # Generate records
        temperature_records = generate_temperature_records(weather_data)
        enhanced_records = generate_enhanced_records(weather_data, meals_data)
        
        # Save records
        temp_file, enhanced_file = save_elasticsearch_records(temperature_records, enhanced_records)
        
        print("=== Data Merge Completed Successfully ===")
        print(f"Temperature file: {temp_file}")
        print(f"Enhanced file: {enhanced_file}")
        
    except Exception as e:
        print(f"Error in data merge process: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
