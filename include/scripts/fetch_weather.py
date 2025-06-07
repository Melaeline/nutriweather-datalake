"""
Weather Data Fetching Module

This module fetches comprehensive weather data from Open-Meteo API.
The data is saved in raw JSON format for further processing.
"""

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import requests
import json
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def setup_weather_client():
    """Setup Open-Meteo API client with caching and retry logic"""
    try:
        # Use basic client to avoid typing issues
        return openmeteo_requests.Client()
    except Exception:
        # Fallback to basic client if setup fails
        return openmeteo_requests.Client()

def fetch_raw_weather_data():
    """STAGE 1: Fetch raw weather data from API"""
    client = setup_weather_client()
    params = {
        "latitude": 48.8534, "longitude": 2.3488,
        "daily": "uv_index_max", "hourly": "temperature_2m",
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
        "forecast_days": 1, "timezone": "auto"
    }
    return client.weather_api("https://api.open-meteo.com/v1/forecast", params=params)[0]

def build_raw_stage_json(api_response):
    """STAGE 1: Convert API response to raw JSON structure (no 'stage' field)"""
    lat, lon = api_response.Latitude(), api_response.Longitude()
    current = api_response.Current()
    
    # Extract raw current data
    current_raw = {}
    current_vars = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"]
    for i, var in enumerate(current_vars):
        current_raw[var] = float(current.Variables(i).Value())
    
    # Extract raw hourly data
    hourly = api_response.Hourly()
    hourly_raw = {
        "time_start": int(hourly.Time()),
        "time_end": int(hourly.TimeEnd()),
        "interval": int(hourly.Interval()),
        "temperature_2m": [float(x) for x in hourly.Variables(0).ValuesAsNumpy()]
    }
    
    # Extract raw daily data
    daily = api_response.Daily()
    daily_raw = {
        "time_start": int(daily.Time()),
        "time_end": int(daily.TimeEnd()),
        "interval": int(daily.Interval()),
        "uv_index_max": [float(x) for x in daily.Variables(0).ValuesAsNumpy()]
    }
    
    return {
        # 'stage': 'RAW',  # REMOVED
        "metadata": {
            "latitude": float(lat),
            "longitude": float(lon),
            "elevation": float(api_response.Elevation()),
            "timezone": api_response.Timezone(),
            "timezone_abbreviation": api_response.TimezoneAbbreviation(),
            "utc_offset_seconds": int(api_response.UtcOffsetSeconds())
        },
        "current": {
            "time": int(current.Time()),
            "data": current_raw
        },
        "hourly": hourly_raw,
        "daily": daily_raw
    }

def save_raw_stage_data(raw_data, filepath=None):
    """STAGE 1: Save raw data to file"""
    if filepath is None:
        # Use directory structure for Airflow
        raw_dir = "include/raw/weather"
        os.makedirs(raw_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(raw_dir, f"raw_weather_{timestamp}.json")
    
    with open(filepath, 'w') as f:
        json.dump(raw_data, f, indent=2, default=str)
    print(f"RAW stage data saved to: {filepath}")
    return filepath

# ============================================================================
# STAGE 2: FORMATTED DATA PROCESSING
# ============================================================================

def get_location_name(lat, lon):
    """STAGE 2: Get location name using reverse geocoding"""
    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lon, "format": "json", "addressdetails": 1},
            headers={"User-Agent": "WeatherApp/1.0"}
        )
        address = response.json().get('address', {})
        city = (address.get('city') or address.get('town') or 
                address.get('village') or address.get('municipality') or 'Unknown')
        country = address.get('country', 'Unknown')
        return f"{city}, {country}"
    except:
        return "Unknown location"

def setup_spark_for_formatting():
    """STAGE 2: Initialize Spark session for formatting"""
    return SparkSession.builder \
        .appName("WeatherDataFormatter") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def build_formatted_stage_json_simple(raw_data):
    """STAGE 2: Process raw data into formatted JSON without Spark (no 'stage' field)"""
    # Process metadata
    metadata = raw_data["metadata"]
    timezone = metadata["timezone"]
    if isinstance(timezone, bytes):
        timezone = timezone.decode('utf-8')
    
    location_name = get_location_name(metadata["latitude"], metadata["longitude"])
    
    # Create formatted location info
    location_formatted = {
        "name": location_name,
        "coordinates": {
            "latitude": round(metadata["latitude"], 2),
            "longitude": round(metadata["longitude"], 2)
        },
        "elevation": round(metadata["elevation"]),
        "timezone": timezone,
        "utc_offset_hours": round(metadata["utc_offset_seconds"] / 3600)
    }
    
    # Format current data
    current_formatted = {}
    for k, v in raw_data["current"]["data"].items():
        current_formatted[k] = round(v, 1) if v is not None else None
    
    # Add formatted time to current data
    current_time = pd.to_datetime(raw_data["current"]["time"], unit="s", utc=True)
    current_formatted["time"] = current_time.strftime('%Y-%m-%d %H:%M:%S UTC')
    
    # Format hourly data
    hourly_raw = raw_data["hourly"]
    hourly_dates = pd.date_range(
        start=pd.to_datetime(hourly_raw["time_start"], unit="s", utc=True),
        end=pd.to_datetime(hourly_raw["time_end"], unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly_raw["interval"]),
        inclusive="left"
    )
    
    hourly_formatted = []
    for date, temp in zip(hourly_dates, hourly_raw["temperature_2m"]):
        hourly_formatted.append({
            "date": date.strftime('%Y-%m-%d %H:%M:%S UTC'),
            "temperature_2m": round(temp, 1) if temp is not None else None
        })
      # Format daily data
    daily_raw = raw_data["daily"]
    daily_dates = pd.date_range(
        start=pd.to_datetime(daily_raw["time_start"], unit="s", utc=True),
        end=pd.to_datetime(daily_raw["time_end"], unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily_raw["interval"]),
        inclusive="left"
    )
    
    daily_formatted = []
    for date, uv in zip(daily_dates, daily_raw["uv_index_max"]):
        daily_formatted.append({
            "date": date.strftime('%Y-%m-%d'),
            "uv_index_max": round(uv, 1) if uv is not None else None
        })
    
    return {
        # 'stage': 'FORMATTED',  # REMOVED
        "location": location_formatted,
        "current": current_formatted,
        "hourly": hourly_formatted,
        "daily": daily_formatted
    }

def build_formatted_stage_json_with_spark(raw_data):
    """STAGE 2: Process raw data into formatted JSON using Spark"""
    spark = setup_spark_for_formatting()
    
    try:
        # Process metadata
        metadata = raw_data["metadata"]
        timezone = metadata["timezone"]
        if isinstance(timezone, bytes):
            timezone = timezone.decode('utf-8')
        
        location_name = get_location_name(metadata["latitude"], metadata["longitude"])
        
        # Create formatted location info
        location_formatted = {
            "name": location_name,
            "coordinates": {
                "latitude": round(metadata["latitude"], 2),
                "longitude": round(metadata["longitude"], 2)
            },
            "elevation": round(metadata["elevation"]),
            "timezone": timezone,
            "utc_offset_hours": round(metadata["utc_offset_seconds"] / 3600)
        }
        
        # Format current data with Spark
        current_data = [(k, v) for k, v in raw_data["current"]["data"].items()]
        current_schema = StructType([
            StructField("variable", StringType(), True),
            StructField("value", DoubleType(), True)
        ])
        
        current_df = spark.createDataFrame(current_data, current_schema)
        current_df = current_df.withColumn("value", spark_round(col("value"), 1))
        current_formatted = {row.variable: row.value for row in current_df.collect()}
        
        # Add formatted time to current data
        current_time = pd.to_datetime(raw_data["current"]["time"], unit="s", utc=True)
        current_formatted["time"] = current_time.strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Format hourly data with Spark
        hourly_raw = raw_data["hourly"]
        hourly_dates = pd.date_range(
            start=pd.to_datetime(hourly_raw["time_start"], unit="s", utc=True),
            end=pd.to_datetime(hourly_raw["time_end"], unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly_raw["interval"]),
            inclusive="left"
        )
        
        hourly_data = [(date.strftime('%Y-%m-%d %H:%M:%S UTC'), temp) 
                       for date, temp in zip(hourly_dates, hourly_raw["temperature_2m"])]
        
        hourly_schema = StructType([
            StructField("date", StringType(), True),
            StructField("temperature_2m", DoubleType(), True)
        ])
        
        hourly_df = spark.createDataFrame(hourly_data, hourly_schema)
        hourly_df = hourly_df.withColumn("temperature_2m", spark_round(col("temperature_2m"), 1))
        hourly_formatted = [row.asDict() for row in hourly_df.collect()]
        
        # Format daily data with Spark
        daily_raw = raw_data["daily"]
        daily_dates = pd.date_range(
            start=pd.to_datetime(daily_raw["time_start"], unit="s", utc=True),
            end=pd.to_datetime(daily_raw["time_end"], unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily_raw["interval"]),
            inclusive="left"
        )
        
        daily_data = [(date.strftime('%Y-%m-%d'), uv) 
                      for date, uv in zip(daily_dates, daily_raw["uv_index_max"])]
        
        daily_schema = StructType([
            StructField("date", StringType(), True),
            StructField("uv_index_max", DoubleType(), True)
        ])
        
        daily_df = spark.createDataFrame(daily_data, daily_schema)
        daily_df = daily_df.withColumn("uv_index_max", spark_round(col("uv_index_max"), 1))
        daily_formatted = [row.asDict() for row in daily_df.collect()]
        
        return {
            # 'stage': 'FORMATTED',  # REMOVED
            "location": location_formatted,
            "current": current_formatted,
            "hourly": hourly_formatted,
            "daily": daily_formatted
        }
        
    finally:
        spark.stop()

def save_formatted_stage_data(formatted_data):
    """STAGE 2: Save formatted data to files"""
    # Create formatted directory
    formatted_dir = "include/formatted/weather"
    os.makedirs(formatted_dir, exist_ok=True)
    
    # Save complete formatted JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    formatted_filepath = os.path.join(formatted_dir, f"formatted_weather_{timestamp}.json")
    
    with open(formatted_filepath, 'w') as f:
        json.dump(formatted_data, f, indent=2, default=str)
    
    print("FORMATTED stage data saved to:")
    print(f"  - {formatted_filepath}")
    
    return formatted_filepath


def main():
    """Main execution pipeline: RAW only"""
    print("="*60)
    print("WEATHER DATA PROCESSING PIPELINE")
    print("="*60)
    try:
        # STAGE 1: RAW DATA PROCESSING
        print("\n🔄 STAGE 1: RAW DATA PROCESSING")
        print("-" * 40)
        
        api_response = fetch_raw_weather_data()
        raw_stage_data = build_raw_stage_json(api_response)
        raw_filepath = save_raw_stage_data(raw_stage_data)
        
        print("✅ RAW stage completed")
        print(json.dumps(raw_stage_data, indent=2, default=str)[:500] + "...")
        
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
    except Exception as e:
        print(f"Error in weather data pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
