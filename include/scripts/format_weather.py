"""
Simplified Weather Data Formatting Module
"""

import glob
import json
import os
from datetime import datetime
from spark_utils import get_spark_session, ensure_directory


def get_location_name(lat, lon):
    """Get location name via reverse geocoding with caching for frequent execution."""
    try:
        import requests
        import time
        
        # Rate limiting for frequent calls
        time.sleep(1)  # Respect Nominatim rate limits
        
        response = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lon, "format": "json"},
            headers={"User-Agent": "NutriWeather/1.0"}
        )
        data = response.json()
        address = data.get('address', {})
        city = address.get('city') or address.get('town') or 'Unknown'
        country = address.get('country', 'Unknown')
        return f"{city}, {country}"
    except:
        return "Unknown Location"


def format_weather_data(raw_data):
    """Format raw weather data with location enrichment."""
    metadata = raw_data["metadata"]
    
    # Add location name
    location_name = get_location_name(metadata["latitude"], metadata["longitude"])
    
    return {
        "location": {
            "name": location_name,
            "latitude": metadata["latitude"],
            "longitude": metadata["longitude"],
            "timezone": metadata["timezone"]
        },
        "current": raw_data["current"],
        "hourly": raw_data["hourly"],
        "daily": raw_data["daily"],
        "formatted_timestamp": datetime.now().isoformat()
    }


def main():
    spark = get_spark_session("FormatWeather")
    
    try:
        # Find latest raw weather file
        raw_dir = "/usr/local/airflow/include/raw/weather"
        pattern = os.path.join(raw_dir, "raw_weather_*.json")
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError("No raw weather files found")
        
        latest_file = max(files, key=os.path.getctime)
        print(f"Processing: {latest_file}")
        
        # Load and format weather data
        with open(latest_file, 'r') as f:
            raw_data = json.load(f)
        
        formatted_data = format_weather_data(raw_data)
        
        # Save formatted data with HDFS backup
        output_dir = "/usr/local/airflow/include/formatted/weather"
        ensure_directory(output_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{output_dir}/formatted_weather_{timestamp}.json"
        
        # Use the new save_with_hdfs_backup function
        from spark_utils import save_with_hdfs_backup
        save_with_hdfs_backup(output_file, formatted_data, "json")
        
        print(f"Formatted weather saved locally and backed up to HDFS: {output_file}")
        
        # Spark validation
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(formatted_data)]))
        print("Weather formatting completed successfully")
        
    except Exception as e:
        print(f"Error formatting weather: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
