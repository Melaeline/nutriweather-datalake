from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
import json
import os
import glob

# Configuration
ELASTIC_HOST = "es01"
ELASTIC_PORT = 9200
ELASTIC_SCHEME = "http"
TEMPERATURE_INDEX = "nutriweather_temperature"
ENHANCED_INDEX = "nutriweather_enhanced"
USAGE_FOLDER = "/usr/local/airflow/include/usage"

def get_latest_files():
    """Find the latest temperature and enhanced data files"""
    temp_pattern = os.path.join(USAGE_FOLDER, "temperature_timeseries_*.jsonl")
    enhanced_pattern = os.path.join(USAGE_FOLDER, "enhanced_recommendations_*.jsonl")
    
    temp_files = glob.glob(temp_pattern)
    enhanced_files = glob.glob(enhanced_pattern)
    
    if not temp_files and not enhanced_files:
        raise FileNotFoundError(f"No data files found in {USAGE_FOLDER}")
    
    latest_temp = max(temp_files, key=os.path.getmtime) if temp_files else None
    latest_enhanced = max(enhanced_files, key=os.path.getmtime) if enhanced_files else None
    
    return latest_temp, latest_enhanced

def index_jsonl_to_elasticsearch():
    """Index both temperature and enhanced records to Elasticsearch"""
    es = Elasticsearch(
        hosts=[f"{ELASTIC_SCHEME}://{ELASTIC_HOST}:{ELASTIC_PORT}"]
    )

    temp_file, enhanced_file = get_latest_files()
    
    # Index temperature records
    if temp_file:
        print(f"Indexing temperature data from: {os.path.basename(temp_file)}")
        with open(temp_file, "r") as file:
            for line_num, line in enumerate(file, 1):
                if line.strip():
                    doc = json.loads(line)
                    es.index(index=TEMPERATURE_INDEX, document=doc)
        print(f"Indexed {line_num} temperature records")
    
    # Index enhanced records
    if enhanced_file:
        print(f"Indexing enhanced data from: {os.path.basename(enhanced_file)}")
        with open(enhanced_file, "r") as file:
            for line_num, line in enumerate(file, 1):
                if line.strip():
                    doc = json.loads(line)
                    es.index(index=ENHANCED_INDEX, document=doc)
        print(f"Indexed {line_num} enhanced records")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
}

with DAG(
    "index_elasticsearch_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["elasticsearch", "indexing", "kibana"],
) as dag:

    index_task = PythonOperator(
        task_id="index_jsonl_to_elasticsearch",
        python_callable=index_jsonl_to_elasticsearch,
    )

    index_task