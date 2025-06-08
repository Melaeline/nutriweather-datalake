from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
import json

# Configuration
ELASTIC_HOST = "es01"
ELASTIC_PORT = 9200
ELASTIC_SCHEME = "http"
INDEX_NAME = "sample_index"
FILE_PATH = "include/usage/merged_data_20250607_210845.json"

def index_file_to_elasticsearch():
    es = Elasticsearch(
        hosts=[f"{ELASTIC_SCHEME}://{ELASTIC_HOST}:{ELASTIC_PORT}"]
    )

    with open(FILE_PATH, "r") as file:
        doc = json.load(file)

    response = es.index(index=INDEX_NAME, document=doc)
    print(f"Document indexed with ID: {response['_id']}")

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