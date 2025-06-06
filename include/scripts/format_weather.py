"""
Weather Data Formatting Module

This module formats raw weather data into optimized structures for analytics.
It handles multiple input formats and saves data in Parquet format for efficient processing.
"""

import os
import json
import glob
from datetime import datetime
import pandas as pd
import numpy as np


def load_raw_weather_files():
    """Load all raw weather JSON files from the raw directory."""
    raw_dir = "include/raw/weather"
    pattern = os.path.join(raw_dir, "raw_weather_*.json")
    json_files = glob.glob(pattern)
    
    print(f"Found {len(json_files)} raw weather files to process")
    
    all_records = []
    
    for file_path in json_files:
        print(f"Processing: {file_path}")
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract base metadata
            coords = data.get('coordinates', {})
            timezone_info = data.get('timezone', {})
            current_data = data.get('current', {})
            hourly_data = data.get('hourly', {})
            daily_data = data.get('daily', {})
            
            base_record = {
                'latitude': coords.get('latitude'),
                'longitude': coords.get('longitude'),
                'elevation': coords.get('elevation'),
                'timezone': timezone_info.get('name', 'UTC')
            }
            
            # Process current data record
            current_record = base_record.copy()
            current_record.update({
                'timestamp': data.get('timestamp', datetime.now().isoformat()),
                'data_type': 'current',
                'temperature_2m': current_data.get('temperature_2m'),
                'relative_humidity_2m': current_data.get('relative_humidity_2m'),
                'wind_speed_10m': current_data.get('wind_speed_10m'),
                'temperature_80m': hourly_data.get('temperature_80m', [None])[0] if hourly_data.get('temperature_80m') else None,
                'uv_index_max': daily_data.get('uv_index_max', [None])[0] if daily_data.get('uv_index_max') else None
            })
            all_records.append(current_record)
            
            # Process hourly data records
            hourly_times = hourly_data.get('times', [])
            hourly_temps = hourly_data.get('temperature_80m', [])
            
            for i, timestamp in enumerate(hourly_times):
                try:
                    dt = datetime.fromtimestamp(timestamp)
                    hourly_record = base_record.copy()
                    hourly_record.update({
                        'timestamp': dt.isoformat(),
                        'data_type': 'hourly',
                        'temperature_2m': None,
                        'relative_humidity_2m': None,
                        'wind_speed_10m': None,
                        'temperature_80m': hourly_temps[i] if i < len(hourly_temps) else None,
                        'uv_index_max': None
                    })
                    all_records.append(hourly_record)
                except (ValueError, IndexError) as e:
                    print(f"Warning: Error processing hourly record {i}: {e}")
                    continue
            
            # Process daily data records
            daily_dates = daily_data.get('dates', [])
            daily_uv = daily_data.get('uv_index_max', [])
            
            for i, timestamp in enumerate(daily_dates):
                try:
                    dt = datetime.fromtimestamp(timestamp)
                    daily_record = base_record.copy()
                    daily_record.update({
                        'timestamp': dt.isoformat(),
                        'data_type': 'daily',
                        'temperature_2m': None,
                        'relative_humidity_2m': None,
                        'wind_speed_10m': None,
                        'temperature_80m': None,
                        'uv_index_max': daily_uv[i] if i < len(daily_uv) else None
                    })
                    all_records.append(daily_record)
                except (ValueError, IndexError) as e:
                    print(f"Warning: Error processing daily record {i}: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue
    
    print(f"Successfully loaded {len(all_records)} weather records")
    return pd.DataFrame(all_records)


def format_weather_data(df):
    """Format and clean the weather data."""
    print("Formatting weather data...")
    
    # Convert timestamp to datetime with flexible parsing
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Fill missing timezone values
    df['timezone'] = df['timezone'].fillna('UTC')
    
    # Handle numeric columns
    numeric_columns = ['latitude', 'longitude', 'elevation', 'temperature_2m', 
                      'relative_humidity_2m', 'wind_speed_10m', 'temperature_80m', 'uv_index_max']
    
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows with all null weather data
    weather_cols = ['temperature_2m', 'relative_humidity_2m', 'wind_speed_10m', 'temperature_80m', 'uv_index_max']
    df = df.dropna(subset=weather_cols, how='all')
    
    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    print(f"Formatted data contains {len(df)} records")
    print(f"Data types: {df['data_type'].value_counts().to_dict()}")
    
    return df


def save_formatted_data(df):
    """Save formatted weather data to parquet file."""
    # Create output directory
    formatted_dir = "include/formatted/weather"
    os.makedirs(formatted_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"formatted_weather_{timestamp}.parquet"
    filepath = os.path.join(formatted_dir, filename)
    
    # Save to parquet
    df.to_parquet(filepath, index=False)
    
    print(f"Formatted weather data saved to: {filepath}")
    print(f"File contains {len(df)} records")
    
    return filepath


def main():
    """Main execution function."""
    print("=== Weather Data Formatting Started ===")
    
    try:
        # Load raw data
        raw_df = load_raw_weather_files()
        
        if raw_df.empty:
            print("No raw weather data found to process")
            return
        
        # Format data
        formatted_df = format_weather_data(raw_df)
        
        # Save formatted data
        save_formatted_data(formatted_df)
        
        print("=== Weather Data Formatting Completed ===")
        
    except Exception as e:
        print(f"Error in weather data formatting: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
