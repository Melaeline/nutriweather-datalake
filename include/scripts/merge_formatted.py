"""
Formatted Data Merge Module

This module merges formatted meal and weather data to create analytical datasets.
It selects one meal and one advice based on temperature matching with randomness.
"""

import os
import re
import json
import glob
import traceback
import random
import csv
from datetime import datetime
from pyspark.sql import SparkSession


def get_latest_file(directory, prefix, extension):
    """Get the latest file based on timestamp in filename."""
    pattern = os.path.join(directory, f"{prefix}*{extension}")
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No files found in {directory} with prefix {prefix}")
    
    # Extract timestamp and sort by most recent
    def extract_timestamp(filepath):
        filename = os.path.basename(filepath)
        timestamps = re.findall(r'\d{8}_\d{6}', filename)
        return timestamps[0] if timestamps else "00000000_000000"
    
    latest_file = max(files, key=extract_timestamp)
    return latest_file, extract_timestamp(latest_file)


def load_weather_data(weather_file):
    """Load weather JSON data as Python dict."""
    with open(weather_file, 'r') as f:
        return json.load(f)


def load_advice_data():
    """Load advice data from CSV."""
    advice_file = "/usr/local/airflow/include/advice_dataset.csv"
    advice_list = []
    
    with open(advice_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            advice_list.append({
                'temperature': int(row['temperature']),
                'advice': row['advice']
            })
    
    return advice_list


def select_meal_by_temperature(meals_data, target_temp):
    """Select one meal based on temperature matching with randomness."""
    # Calculate temperature differences for all meals
    meal_scores = []
    
    for meal_row in meals_data:
        meal_dict = meal_row.asDict()
        meal_temp = meal_dict.get('temperature', 20)
        temp_diff = abs(meal_temp - target_temp)
        
        # Score: lower temperature difference = higher score
        # Add some randomness to the scoring
        score = max(0, 100 - temp_diff) + random.uniform(-10, 10)
        meal_scores.append((score, meal_dict))
    
    # Sort by score (highest first) and select from top candidates
    meal_scores.sort(key=lambda x: x[0], reverse=True)
    
    # Choose randomly from top 5 candidates to add variability
    top_candidates = meal_scores[:min(5, len(meal_scores))]
    selected_score, selected_meal = random.choice(top_candidates)
    
    return selected_meal


def select_advice_by_temperature(advice_data, target_temp):
    """Select one advice based on temperature matching with randomness."""
    # Calculate temperature differences for all advice entries
    advice_scores = []
    
    for advice in advice_data:
        advice_temp = advice['temperature']
        temp_diff = abs(advice_temp - target_temp)
        
        # Score: lower temperature difference = higher score
        # Add some randomness to the scoring
        score = max(0, 100 - temp_diff) + random.uniform(-5, 5)
        advice_scores.append((score, advice))
    
    # Sort by score (highest first) and select from top candidates
    advice_scores.sort(key=lambda x: x[0], reverse=True)
    
    # Choose randomly from top 3 candidates to add variability
    top_candidates = advice_scores[:min(3, len(advice_scores))]
    selected_score, selected_advice = random.choice(top_candidates)
    
    return selected_advice


def create_merged_recommendation(weather_data, selected_meal, selected_advice):
    """Create the final merged recommendation."""
    # Extract weather components
    location_info = weather_data.get('location', {})
    current_weather = weather_data.get('current', {})
    hourly_data = weather_data.get('hourly', [])
    daily_data = weather_data.get('daily', [])
    
    current_temp = current_weather.get('temperature_2m', 0)
    current_timestamp = datetime.now().isoformat() + 'Z'
    
    # Create the merged recommendation
    merged_record = {
        # Weather context
        'weather_location': {
            'name': location_info.get('name', 'Unknown'),
            'latitude': location_info.get('coordinates', {}).get('latitude'),
            'longitude': location_info.get('coordinates', {}).get('longitude'),
            'timezone': location_info.get('timezone')
        },
        
        # Current weather snapshot
        'current_weather': {
            'temperature': current_temp,
            'humidity': current_weather.get('relative_humidity_2m', 0),
            'wind_speed': current_weather.get('wind_speed_10m', 0),
            'observation_time': current_weather.get('time', '')
        },
        
        # Complete hourly weather data
        'hourly_weather_data': hourly_data,
        
        # Daily weather summary
        'daily_weather_summary': daily_data,
        
        # Selected meal recommendation
        'recommended_meal': {
            'meal_id': selected_meal.get('meal_id'),
            'meal_name': selected_meal.get('meal_name'),
            'category': selected_meal.get('category'),
            'region': selected_meal.get('region'),
            'ingredients': selected_meal.get('ingredients'),
            'instructions': selected_meal.get('instructions'),
            'preparation_time': selected_meal.get('preparation_time'),
            'temperature': selected_meal.get('temperature'),
            'image_url': selected_meal.get('image_url'),
            'temperature_difference': abs(selected_meal.get('temperature', 20) - current_temp)
        },
        
        # Selected advice
        'recommended_advice': {
            'advice_text': selected_advice['advice'],
            'advice_temperature': selected_advice['temperature'],
            'temperature_difference': abs(selected_advice['temperature'] - current_temp)
        },
        
        # Merge metadata
        'merge_metadata': {
            'merge_timestamp': current_timestamp,
            'weather_fetch_time': current_weather.get('time', ''),
            'total_meals_available': len(meals_data) if 'meals_data' in locals() else 0,
            'total_advice_entries': len(advice_data) if 'advice_data' in locals() else 0,
            'selection_method': 'temperature_based_with_randomness'
        }
    }
    
    return merged_record


def main():
    """Main execution function."""
    print("=== Formatted Data Merge Started ===")
    
    spark = SparkSession.builder \
        .appName("MergeFormattedData") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Define directories
        base_dir = "/usr/local/airflow/include"
        meals_dir = os.path.join(base_dir, "formatted", "meals")
        weather_dir = os.path.join(base_dir, "formatted", "weather")
        merged_dir = os.path.join(base_dir, "usage")
        
        # Create merged directory
        os.makedirs(merged_dir, exist_ok=True)
        
        # Get latest files
        print("Finding latest formatted files...")
        meals_file, meals_timestamp = get_latest_file(meals_dir, "formatted_meals_", ".parquet")
        weather_file, weather_timestamp = get_latest_file(weather_dir, "formatted_weather_", ".json")
        
        print(f"Latest meals file: {meals_file}")
        print(f"Latest weather file: {weather_file}")
        
        # Load data
        print("Loading formatted data...")
        meals_df = spark.read.parquet(meals_file)
        weather_data = load_weather_data(weather_file)
        advice_data = load_advice_data()
        
        meal_count = meals_df.count()
        hourly_count = len(weather_data.get('hourly', []))
        advice_count = len(advice_data)
        current_temp = weather_data.get('current', {}).get('temperature_2m', 0)
        
        print(f"Loaded {meal_count} meals, {hourly_count} hourly weather records, and {advice_count} advice entries")
        print(f"Current temperature: {current_temp}°C")
        
        # Collect meals data for selection
        meals_data = meals_df.collect()
        
        # Select one meal and one advice based on temperature
        print("Selecting meal and advice based on temperature matching...")
        selected_meal = select_meal_by_temperature(meals_data, current_temp)
        selected_advice = select_advice_by_temperature(advice_data, current_temp)
        
        print(f"Selected meal: {selected_meal.get('meal_name')} (temp: {selected_meal.get('temperature')}°C)")
        print(f"Selected advice: {selected_advice['advice'][:50]}... (temp: {selected_advice['temperature']}°C)")
        
        # Create merged recommendation
        merged_record = create_merged_recommendation(weather_data, selected_meal, selected_advice)
        
        # Generate output timestamp and filename
        merge_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"merged_data_{merge_timestamp}.json"
        json_output = os.path.join(merged_dir, json_filename)
        
        # Save as clean JSON file
        print(f"Saving merged recommendation to: {json_output}")
        with open(json_output, 'w') as f:
            json.dump(merged_record, f, indent=2, default=str)
        
        print(f"\n=== Merge Completed Successfully ===")
        print(f"Weather location: {merged_record['weather_location']['name']}")
        print(f"Current temperature: {current_temp}°C")
        print(f"Recommended meal: {selected_meal.get('meal_name')}")
        print(f"Meal category: {selected_meal.get('category')}")
        print(f"Preparation time: {selected_meal.get('preparation_time')} minutes")
        print(f"Advice: {selected_advice['advice']}")
        print(f"Output file: {json_filename}")
        
    except Exception as e:
        print(f"Error in data merge: {e}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
