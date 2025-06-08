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
INDEX_NAME = "nutriweather_index"
USAGE_FOLDER = "/usr/local/airflow/include/usage"

def get_latest_merged_data_file():
    """Find the latest merged data file based on file modification time"""
    pattern = os.path.join(USAGE_FOLDER, "merged_data_*.json")
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No merged data files found in {USAGE_FOLDER}")
    
    # Find the file with the latest modification time using max()
    latest_file = max(files, key=os.path.getmtime)
    print(f"Using latest merged data file: {os.path.basename(latest_file)}")
    return latest_file

def index_file_to_elasticsearch():
    es = Elasticsearch(
        hosts=[f"{ELASTIC_SCHEME}://{ELASTIC_HOST}:{ELASTIC_PORT}"]
    )

    # Get the latest merged data file
    file_path = get_latest_merged_data_file()
    
    with open(file_path, "r") as file:
        doc = json.load(file)

    response = es.index(index=INDEX_NAME, document=doc)
    print(f"Document indexed with ID: {response['_id']} from file: {os.path.basename(file_path)}")

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
    tags=["elasticsearch", "indexing"],
) as dag:

    index_task = PythonOperator(
        task_id="index_json_to_elasticsearch",
        python_callable=index_file_to_elasticsearch,
    )

    index_task