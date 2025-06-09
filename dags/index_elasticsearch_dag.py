"""
Elasticsearch Indexing DAG - Indexes processed data to Elasticsearch.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from config.dag_config import COMMON_DAG_CONFIG, USAGE_DIR, ELASTIC_CONFIG
from elasticsearch import Elasticsearch
import json
import os
import glob
import hashlib
from collections import defaultdict

dag = DAG(
    dag_id="index_elasticsearch_dag",
    description="Index processed data to Elasticsearch",
    tags=["elasticsearch", "indexing"],
    **COMMON_DAG_CONFIG
)

def get_latest_files():
    """Find latest data files for indexing."""
    temp_files = glob.glob(os.path.join(USAGE_DIR, "temperature_timeseries_*.jsonl"))
    enhanced_files = glob.glob(os.path.join(USAGE_DIR, "enhanced_recommendations_*.jsonl"))
    
    latest_temp = max(temp_files, key=os.path.getmtime) if temp_files else None
    latest_enhanced = max(enhanced_files, key=os.path.getmtime) if enhanced_files else None
    
    return latest_temp, latest_enhanced

def index_to_elasticsearch():
    """Index data to Elasticsearch."""
    es = Elasticsearch(
        hosts=[f"{ELASTIC_CONFIG['scheme']}://{ELASTIC_CONFIG['host']}:{ELASTIC_CONFIG['port']}"]
    )
    
    temp_file, enhanced_file = get_latest_files()
    
    # Index temperature data with duplicate detection
    if temp_file:
        print(f"Indexing temperature data from: {os.path.basename(temp_file)}")
        line_num = 0
        duplicate_count = 0
        doc_hashes = defaultdict(list)
        
        with open(temp_file, "r") as f:
            for line in f:
                if line.strip():
                    doc = json.loads(line)
                    
                    # Create hash of temperature data (excluding timestamp)
                    temp_data = {k: v for k, v in doc.items() if k != 'timestamp'}
                    doc_hash = hashlib.md5(json.dumps(temp_data, sort_keys=True).encode()).hexdigest()
                    
                    # Track duplicates
                    if doc_hash in doc_hashes:
                        duplicate_count += 1
                        doc_hashes[doc_hash].append(doc.get('timestamp', 'unknown'))
                    else:
                        doc_hashes[doc_hash] = [doc.get('timestamp', 'unknown')]
                    
                    es.index(index=ELASTIC_CONFIG['temperature_index'], document=doc)
                    line_num += 1
        
        print(f"Indexed {line_num} temperature records")
        print(f"Found {duplicate_count} duplicate temperature readings")
        print(f"Unique temperature patterns: {len(doc_hashes)}")
        
        # Log examples of duplicates
        for doc_hash, timestamps in doc_hashes.items():
            if len(timestamps) > 1:
                print(f"Duplicate pattern found {len(timestamps)} times at: {timestamps[:5]}...")  # Show first 5
                break
    else:
        print("No temperature data file found to index.")

    # Index enhanced data
    if enhanced_file:
        print(f"Indexing enhanced data from: {os.path.basename(enhanced_file)}")
        line_num = 0
        with open(enhanced_file, "r") as f:
            for line in f:
                if line.strip():
                    doc = json.loads(line)
                    es.index(index=ELASTIC_CONFIG['enhanced_index'], document=doc)
                    line_num += 1
        print(f"Indexed {line_num} enhanced records")
    else:
        print("No enhanced data file found to index.")

    print("Indexing to Elasticsearch completed.")

# Tasks
index_task = PythonOperator(
    task_id="index_to_elasticsearch",
    python_callable=index_to_elasticsearch,
    dag=dag
)