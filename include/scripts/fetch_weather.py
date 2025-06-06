"""
Weather Data Fetching Module

This module fetches comprehensive weather data from Open-Meteo API.
The data is saved in raw JSON format for further processing.
"""

import os
import sys
import json
from datetime import datetime
import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd


def fetch_weather_data():
    """
    Fetch comprehensive weather data from Open-Meteo API following the exact pattern.
    
    Returns:
        tuple: Raw weather data, formatted weather data, hourly dataframe, and daily dataframe
    """
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.8534,
        "longitude": 2.3488,
        "daily": "uv_index_max",
        "hourly": "temperature_80m",
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
        "timezone": "Europe/Paris",  # Add timezone parameter
        "forecast_days": 1
    }
    
    print("Fetching weather data from Open-Meteo API...")
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Current values. The order of variables needs to be the same as requested.
    current = response.Current()
    current_temperature_2m = current.Variables(0).Value()
    current_relative_humidity_2m = current.Variables(1).Value()
    current_wind_speed_10m = current.Variables(2).Value()

    print(f"Current time {current.Time()}")
    print(f"Current temperature_2m {current_temperature_2m}")
    print(f"Current relative_humidity_2m {current_relative_humidity_2m}")
    print(f"Current wind_speed_10m {current_wind_speed_10m}")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_80m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_80m"] = hourly_temperature_80m
    hourly_dataframe = pd.DataFrame(data = hourly_data)
    print("Hourly data:")
    print(hourly_dataframe)

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_uv_index_max = daily.Variables(0).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}

    daily_data["uv_index_max"] = daily_uv_index_max
    daily_dataframe = pd.DataFrame(data = daily_data)
    print("Daily data:")
    print(daily_dataframe)
    
    # Create comprehensive weather data structure for RAW storage
    raw_weather_data = {
        'timestamp': datetime.now().isoformat(),
        'coordinates': {
            'latitude': response.Latitude(),
            'longitude': response.Longitude(),
            'elevation': response.Elevation()
        },
        'timezone': {
            'name': response.Timezone() or 'Europe/Paris',  # Fallback timezone
            'abbreviation': response.TimezoneAbbreviation(),
            'utc_offset_seconds': response.UtcOffsetSeconds()
        },
        'current': {
            'time': current.Time(),
            'temperature_2m': current_temperature_2m,
            'relative_humidity_2m': current_relative_humidity_2m,
            'wind_speed_10m': current_wind_speed_10m
        },
        'hourly': {
            'times': [int(t.timestamp()) for t in hourly_data["date"]],
            'temperature_80m': hourly_temperature_80m.tolist()
        },
        'daily': {
            'dates': [int(t.timestamp()) for t in daily_data["date"]],
            'uv_index_max': daily_uv_index_max.tolist()
        }
    }
    
    return raw_weather_data, None, hourly_dataframe, daily_dataframe


def save_weather_data(raw_weather_data, formatted_weather_data, hourly_dataframe, daily_dataframe):
    """
    Save weather data to appropriate directories in both raw and formatted structures.
    
    Args:
        raw_weather_data (dict): Raw weather data dictionary
        formatted_weather_data (dict): Enhanced/formatted weather data dictionary (not used)
        hourly_dataframe (DataFrame): Pandas DataFrame with hourly data
        daily_dataframe (DataFrame): Pandas DataFrame with daily data
    """
    # Save data to directories (use relative paths for cross-platform compatibility)
    raw_dir = "include/raw/weather"
    os.makedirs(raw_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save RAW data as JSON only (formatting will be handled by separate script)
    raw_json_filename = f"raw_weather_{timestamp}.json"
    raw_json_path = os.path.join(raw_dir, raw_json_filename)
    with open(raw_json_path, 'w') as f:
        json.dump(raw_weather_data, f, indent=2, default=str)
    print(f"Saved raw weather data to {raw_json_path}")

    print(f"Weather data extraction complete!")
    print(f"Hourly records: {len(hourly_dataframe)}")
    print(f"Daily records: {len(daily_dataframe)}")


def main():
    """Main function to execute the weather data pipeline."""
    try:
        print("=== Weather Data Fetching Started ===")
        raw_data, formatted_data, hourly_df, daily_df = fetch_weather_data()
        save_weather_data(raw_data, formatted_data, hourly_df, daily_df)
        print("=== Weather Data Fetching Completed ===")
    except Exception as e:
        print(f"Error in weather data pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
