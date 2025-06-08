"""
Merge Formatted Data Module

This module merges formatted meal and weather data to create analytical datasets.
It combines the latest formatted meals (Parquet) with weather data (JSON).
"""

import os
import json
import glob
import pandas as pd
from datetime import datetime
import traceback


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


def merge_data(meals_data, weather_data):
    """Merge meals and weather data into analytical format."""
    merged_data = {
        "pipeline_info": {
            "merged_at": datetime.now().isoformat(),
            "total_meals": len(meals_data),
            "has_weather_data": bool(weather_data)
        },
        "weather": weather_data,
        "meals": meals_data,
        "analytics": {
            "meal_categories": {},
            "regions": {},
            "temperature_ranges": {
                "cold": [],  # meals with temp < 10
                "warm": [],  # meals with temp 10-20
                "hot": []    # meals with temp > 20
            }
        }
    }
    
    # Generate analytics
    if meals_data:
        # Count by category
        for meal in meals_data:
            category = meal.get('category', 'Unknown')
            merged_data["analytics"]["meal_categories"][category] = \
                merged_data["analytics"]["meal_categories"].get(category, 0) + 1
            
            # Count by region
            region = meal.get('region', 'Unknown')
            merged_data["analytics"]["regions"][region] = \
                merged_data["analytics"]["regions"].get(region, 0) + 1
            
            # Temperature categorization
            temp = meal.get('temperature', 15)
            if temp < 10:
                merged_data["analytics"]["temperature_ranges"]["cold"].append(meal.get('meal_name', ''))
            elif temp <= 20:
                merged_data["analytics"]["temperature_ranges"]["warm"].append(meal.get('meal_name', ''))
            else:
                merged_data["analytics"]["temperature_ranges"]["hot"].append(meal.get('meal_name', ''))
    
    return merged_data


def save_merged_data(merged_data):
    """Save merged data to usage directory."""
    usage_dir = "/usr/local/airflow/include/usage"
    os.makedirs(usage_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"merged_data_{timestamp}.json"
    filepath = os.path.join(usage_dir, filename)
    
    with open(filepath, 'w') as f:
        json.dump(merged_data, f, indent=2, default=str)
    
    print(f"Merged data saved to: {filepath}")
    return filepath


def main():
    """Main execution function."""
    print("=== Starting Data Merge Process ===")
    
    try:
        # Load data
        meals_data = load_latest_meals_data()
        weather_data = load_latest_weather_data()
        
        print(f"Loaded {len(meals_data)} meals")
        print(f"Weather data available: {bool(weather_data)}")
        
        # Merge data
        merged_data = merge_data(meals_data, weather_data)
        
        # Save merged data
        output_file = save_merged_data(merged_data)
        
        print("=== Data Merge Completed Successfully ===")
        print(f"Output file: {output_file}")
        
    except Exception as e:
        print(f"Error in data merge process: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
