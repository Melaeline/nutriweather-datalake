import pandas as pd
import os
import glob
import shutil
import json
from datetime import datetime


def main():
    try:
        print("Reading raw weather data...")
        raw_dir = "/usr/local/airflow/include/raw/weather"
        
        # Look for CSV files first (preferred), then JSON files as fallback
        csv_files = glob.glob(os.path.join(raw_dir, "*.csv"))
        json_files = glob.glob(os.path.join(raw_dir, "*.json"))
        
        if not csv_files and not json_files:
            raise FileNotFoundError("No raw weather files found")
        
        processed_count = 0
        os.makedirs("/usr/local/airflow/include/formatted/weather", exist_ok=True)
        
        # Process CSV files (preferred format)
        for file_path in csv_files:
            print(f"Processing CSV file: {file_path}")
            df = pd.read_csv(file_path)
            
            # Debug: Print data info
            print(f"Data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"Sample data:\n{df.head()}")
            
            # Skip if DataFrame is empty or has only one row with NaN
            if df.empty or (len(df) == 1 and df.isnull().all().all()):
                print(f"Skipping empty or invalid file: {file_path}")
                continue
            
            # Format time column if exists
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
            elif 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            elif 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Standardize column names
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            
            # Add processing timestamp
            df['formatted_date'] = pd.Timestamp.now()
            
            # Save as Parquet with clean filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_path = f"/usr/local/airflow/include/formatted/weather/formatted_weather_{timestamp}.parquet"
            
            df.to_parquet(final_path, index=False, compression='snappy')
            print(f"Successfully saved formatted weather to: {final_path}")
            print(f"Formatted data shape: {df.shape}")
            processed_count += 1
        
        # Process JSON files (fallback for legacy data)
        for file_path in json_files:
            print(f"Processing JSON file: {file_path}")
            
            # Read JSON file manually to handle nested structure
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract metadata
            fetch_timestamp = data.get('fetch_timestamp', '')
            location = data.get('location', '')
            coordinates = data.get('coordinates', {})
            latitude = coordinates.get('latitude', data.get('weather_data', {}).get('latitude'))
            longitude = coordinates.get('longitude', data.get('weather_data', {}).get('longitude'))
            
            # Extract hourly data
            weather_data = data.get('weather_data', {})
            hourly_data = weather_data.get('hourly', {})
            
            if not hourly_data:
                print(f"No hourly data found in {file_path}, skipping...")
                continue
                
            # Convert hourly arrays to DataFrame
            df = pd.DataFrame(hourly_data)
            
            # Add metadata columns
            df['fetch_timestamp'] = fetch_timestamp
            df['location'] = location
            df['latitude'] = latitude
            df['longitude'] = longitude
            df['timezone'] = weather_data.get('timezone', '')
            df['elevation'] = weather_data.get('elevation', 0)
            
            # Format time column
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
            
            # Standardize column names
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            
            # Add processing timestamp
            df['formatted_date'] = pd.Timestamp.now()
            
            # Save as Parquet with clean filename (matching meals approach)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_path = f"/usr/local/airflow/include/formatted/weather/formatted_weather_{timestamp}.parquet"
            
            df.to_parquet(final_path, index=False, compression='snappy')
            print(f"Successfully saved formatted weather to: {final_path}")
            processed_count += 1
        
        print(f"Formatting complete! Processed {processed_count} weather files")
        
        if processed_count == 0:
            raise ValueError("No valid weather files were processed")
        
    except Exception as e:
        print(f"Error formatting weather: {e}")
        raise


if __name__ == "__main__":
    main()
