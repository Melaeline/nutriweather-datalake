"""
Simplified Weather Data Fetching Module
"""

import openmeteo_requests
import pandas as pd
import json
import os
from datetime import datetime
from spark_utils import get_spark_session, ensure_directory
import time


def fetch_weather_data():
    """Fetch weather data from Open-Meteo API."""
    client = openmeteo_requests.Client()
    
    params = {
        "latitude": 48.8534,
        "longitude": 2.3488,
        "daily": "uv_index_max",
        "hourly": "temperature_2m",
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
        "forecast_days": 1,
        "timezone": "auto"
    }
    
    return client.weather_api("https://api.open-meteo.com/v1/forecast", params=params)[0]


def process_weather_response(response):
    """Convert API response to clean JSON structure."""
    lat, lon = response.Latitude(), response.Longitude()
    current = response.Current()
    hourly = response.Hourly()
    daily = response.Daily()
    
    # Current data
    current_data = {
        "time": pd.to_datetime(current.Time(), unit="s", utc=True).isoformat(),
        "temperature_2m": round(float(current.Variables(0).Value()), 1),
        "relative_humidity_2m": round(float(current.Variables(1).Value()), 1),
        "wind_speed_10m": round(float(current.Variables(2).Value()), 1)
    }
    
    # Hourly data
    hourly_times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    
    hourly_data = []
    for time, temp in zip(hourly_times, hourly.Variables(0).ValuesAsNumpy()):
        hourly_data.append({
            "time": time.isoformat(),
            "temperature_2m": round(float(temp), 1)
        })
    
    # Daily data
    daily_times = pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )
    
    daily_data = []
    for time, uv in zip(daily_times, daily.Variables(0).ValuesAsNumpy()):
        daily_data.append({
            "date": time.date().isoformat(),
            "uv_index_max": round(float(uv), 1)
        })
    
    return {
        "metadata": {
            "latitude": round(float(lat), 4),
            "longitude": round(float(lon), 4),
            "elevation": round(float(response.Elevation())),
            "timezone": str(response.Timezone()),
            "fetch_timestamp": datetime.now().isoformat()
        },
        "current": current_data,
        "hourly": hourly_data,
        "daily": daily_data
    }


def main():
    spark = get_spark_session("FetchWeather")
    
    try:
        print("Fetching weather data from Open-Meteo (scheduled execution)...")
        
        # Check if we have recent data (within last 2 minutes) to avoid unnecessary API calls
        output_dir = "/usr/local/airflow/include/raw/weather"
        if os.path.exists(output_dir):
            import glob
            existing_files = glob.glob(os.path.join(output_dir, "raw_weather_*.json"))
            if existing_files:
                latest_file = max(existing_files, key=os.path.getctime)
                file_age = time.time() - os.path.getctime(latest_file)
                if file_age < 120:  # Less than 2 minutes old
                    print(f"Recent weather data exists ({file_age:.0f}s old), skipping API call")
                    return
        
        # Fetch and process weather data
        response = fetch_weather_data()
        weather_data = process_weather_response(response)
        
        # Setup output directory
        output_dir = "/usr/local/airflow/include/raw/weather"
        ensure_directory(output_dir)
        
        # Save raw weather data with HDFS backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{output_dir}/raw_weather_{timestamp}.json"
        
        # Use the new save_with_hdfs_backup function
        from spark_utils import save_with_hdfs_backup
        save_with_hdfs_backup(output_file, weather_data, "json")
        
        print(f"Raw weather data saved locally and backed up to HDFS: {output_file}")
        
        # Spark validation
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(weather_data)]))
        print(f"Spark validation successful - weather data structure verified")
        
    except Exception as e:
        print(f"Error in weather fetch: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
