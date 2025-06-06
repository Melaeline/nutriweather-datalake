"""
## Weather Data Fetch DAG

This DAG fetches weather data from Open-Meteo API and saves them as raw CSV data.
It saves the results to the include/raw directory for further processing.

This is the first stage of the weather processing pipeline.
"""

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import pandas as pd
import os
from typing import Dict


@dag(
    dag_id="fetch_weather_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "nutriweather", "retries": 3},
    tags=["weather", "fetch", "raw"],
    max_active_runs=1,
)
def fetch_weather_dag():
    
    @task
    def extract_weather_from_api() -> None:
        """Extract weather data from Open-Meteo API and save as CSV."""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 48.8534,
            "longitude": 2.3488,
            "hourly": ["temperature_2m", "relative_humidity_2m", "cloud_cover", "wind_direction_10m"],
            "timezone": "auto",
            "forecast_days": 1
        }
        
        print("Fetching weather data from Open-Meteo API...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Extract hourly data - this is the key fix!
        hourly_data = data.get('hourly', {})
        if not hourly_data:
            raise ValueError("No hourly data received from API")
            
        # Create DataFrame from hourly arrays
        df = pd.DataFrame(hourly_data)
        
        # Convert time column
        df["time"] = pd.to_datetime(df["time"])
        
        # Add metadata
        df['fetch_timestamp'] = datetime.now().isoformat()
        df['latitude'] = data.get('latitude', params['latitude'])
        df['longitude'] = data.get('longitude', params['longitude'])
        df['timezone'] = data.get('timezone', 'UTC')
        df['elevation'] = data.get('elevation', 0)
        
        print(f"Weather data extracted: {len(df)} hourly records")
        print(f"Data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Save to raw directory
        raw_dir = "/usr/local/airflow/include/raw/weather"
        os.makedirs(raw_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_weather_{timestamp}.csv"
        file_path = os.path.join(raw_dir, filename)
        
        df.to_csv(file_path, index=False)
        print(f"Saved weather data to {file_path}")
        print(f"Sample data:\n{df.head()}")
    
    trigger_formatting = TriggerDagRunOperator(
        task_id="trigger_formatting",
        trigger_dag_id="format_weather_dag",
        wait_for_completion=False,
        conf={"triggered_by": "fetch_weather_dag"}
    )
    
    extract_weather_from_api() >> trigger_formatting


fetch_weather_dag()
