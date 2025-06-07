"""
Weather Data Formatting Module - Simplified Version

This module formats raw weather data into optimized structures for analytics.
It handles the new JSON format with raw and formatted stages.
"""

import os
import json
import glob
import traceback
from datetime import datetime


def save_formatted_data_clean_json(formatted_json):
    """Save only the clean, nested JSON weather data."""
    formatted_dir = "include/formatted/weather"
    os.makedirs(formatted_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"formatted_weather_{timestamp}.json"
    json_filepath = os.path.join(formatted_dir, json_filename)
    with open(json_filepath, 'w') as f:
        json.dump(formatted_json, f, indent=2)
    print(f"Clean formatted weather data saved to: {json_filepath}")
    return json_filepath


def main():
    """Main execution function."""
    print("=== Weather Data Formatting Started ===")
    
    try:
        from fetch_weather import build_formatted_stage_json_simple
        raw_dir = "include/raw/weather"
        pattern = os.path.join(raw_dir, "raw_weather_*.json")
        json_files = glob.glob(pattern)
        if not json_files:
            print("No raw weather data found to process")
            return
        for file_path in json_files:
            print(f"Processing: {file_path}")
            with open(file_path, 'r') as f:
                raw_data = json.load(f)
            if raw_data.get("stage") == "RAW" or True:  # Always process, ignore 'stage'
                # Remove 'stage' if present
                raw_data.pop("stage", None)
                # Ensure timezone is always a clean string
                tz = raw_data["metadata"].get("timezone")
                if isinstance(tz, bytes):
                    tz = tz.decode('utf-8')
                elif isinstance(tz, str) and tz.startswith("b'") and tz.endswith("'"):
                    tz = tz[2:-1]
                raw_data["metadata"]["timezone"] = tz
                formatted_json = build_formatted_stage_json_simple(raw_data)
                # Remove 'stage' from formatted output if present
                formatted_json.pop("stage", None)
                save_formatted_data_clean_json(formatted_json)
        print("=== Weather Data Formatting Completed ===")
    except Exception as e:
        print(f"Error in weather data formatting: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
