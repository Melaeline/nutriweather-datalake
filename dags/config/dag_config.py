"""
Shared DAG configuration for NutriWeather pipeline.
"""
import os
from datetime import datetime, timedelta

# Common DAG arguments
DEFAULT_DAG_ARGS = {
    "owner": "nutriweather",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Common DAG configuration
COMMON_DAG_CONFIG = {
    "start_date": datetime(2024, 1, 1),
    "schedule": None,
    "catchup": False,
    "max_active_runs": 1,
    "default_args": DEFAULT_DAG_ARGS,
}

# File paths
AIRFLOW_HOME = os.path.abspath(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
INCLUDE_DIR = f"{AIRFLOW_HOME}/include"
RAW_DIR = f"{INCLUDE_DIR}/raw"
FORMATTED_DIR = f"{INCLUDE_DIR}/formatted"
USAGE_DIR = f"{INCLUDE_DIR}/usage"
SCRIPTS_DIR = f"{INCLUDE_DIR}/scripts"

# API Configuration
MEALDB_BASE_URL = "https://www.themealdb.com/api/json/v1/1"
WEATHER_API_TIMEOUT = 30

# Elasticsearch Configuration
ELASTIC_CONFIG = {
    "host": "es01",
    "port": 9200,
    "scheme": "http",
    "temperature_index": "nutriweather_temperature",
    "enhanced_index": "nutriweather_enhanced",
}
