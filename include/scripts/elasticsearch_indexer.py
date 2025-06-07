"""
Elasticsearch indexing utilities for Nutriweather Datalake
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Sequence
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticsearchIndexer:
    """
    Handles indexing of nutritional and weather data into Elasticsearch
    """
    
    def __init__(self, 
                 host: str = "https://es01:9200",  # Use Docker service name
                 username: str = "elastic",
                 password: str = "elastic",
                 ca_cert_path: Optional[str] = None):
        """
        Initialize Elasticsearch client with fallback options
        
        Args:
            host: Elasticsearch host URL
            username: Username for authentication
            password: Password for authentication
            ca_cert_path: Path to CA certificate for SSL verification
        """
        # Try HTTPS first, then HTTP as fallback
        connection_attempts = [
            {
                "hosts": [host],
                "basic_auth": (username, password),
                "verify_certs": False,
                "ssl_show_warn": False,
                "max_retries": 2,
                "retry_on_timeout": True,
                "request_timeout": 30
            }
        ]
        
        # Add HTTP fallback if HTTPS fails
        if host.startswith("https://"):
            http_host = host.replace("https://", "http://")
            connection_attempts.append({
                "hosts": [http_host],
                "basic_auth": (username, password),
                "max_retries": 2,
                "retry_on_timeout": True,
                "request_timeout": 30
            })
        
        last_error = None
        
        for i, config in enumerate(connection_attempts):
            try:
                logger.info(f"Attempting Elasticsearch connection {i+1}/{len(connection_attempts)}: {config['hosts'][0]}")
                self.es = Elasticsearch(**config)
                
                # Test connection
                if self.es.ping():
                    logger.info(f"✅ Successfully connected to Elasticsearch at {config['hosts'][0]}")
                    return
                else:
                    logger.warning(f"⚠️ Connection attempt {i+1} failed: ping unsuccessful")
                    
            except Exception as e:
                last_error = e
                logger.warning(f"⚠️ Connection attempt {i+1} failed: {e}")
                continue
        
        # If we get here, all attempts failed
        logger.error("❌ All connection attempts failed")
        raise Exception(f"Elasticsearch connection failed: {last_error}")
    
    def create_index_template(self, index_name: str, mapping: Dict[str, Any]) -> bool:
        """
        Create an index template with proper mappings
        
        Args:
            index_name: Name of the index
            mapping: Elasticsearch mapping configuration
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            template_body = {
                "index_patterns": [f"{index_name}-*"],
                "template": {
                    "mappings": mapping,
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                }
            }
            
            self.es.indices.put_index_template(
                name=f"{index_name}_template",
                body=template_body
            )
            logger.info(f"✅ Created index template for {index_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create index template for {index_name}: {e}")
            return False
    
    def index_documents(self, index_name: str, documents: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Index documents into Elasticsearch
        
        Args:
            index_name: Name of the index
            documents: List of documents to index
            
        Returns:
            Dict with success status and statistics
        """
        if not documents:
            logger.warning("No documents to index")
            return {"success": False, "reason": "No documents provided"}
        
        try:
            # Prepare documents for bulk indexing
            actions = []
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                actions.append(action)
            
            # Perform bulk indexing
            success_count, failed_items = bulk(
                self.es,
                actions,
                chunk_size=1000,
                request_timeout=60
            )
            
            total_docs = len(documents)
            failed_count = len(failed_items) if isinstance(failed_items, list) else 0
            
            if failed_count > 0:
                logger.warning(f"Failed to index {failed_count} out of {total_docs} documents")
                return {
                    "success": True,
                    "indexed": success_count,
                    "failed": failed_count,
                    "total": total_docs,
                    "failed_items": failed_items
                }
            else:
                logger.info(f"✅ Successfully indexed {success_count} documents to {index_name}")
                return {
                    "success": True,
                    "indexed": success_count,
                    "failed": 0,
                    "total": total_docs
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to index documents to {index_name}: {e}")
            return {
                "success": False,
                "reason": str(e),
                "total": len(documents)
            }
    
    def create_nutrition_mapping(self) -> Dict[str, Any]:
        """
        Create mapping for nutrition data
        
        Returns:
            Elasticsearch mapping for nutrition documents
        """
        return {
            "properties": {
                "food_name": {"type": "text", "analyzer": "standard"},
                "calories": {"type": "float"},
                "protein": {"type": "float"},
                "fat": {"type": "float"},
                "carbohydrates": {"type": "float"},
                "fiber": {"type": "float"},
                "sugar": {"type": "float"},
                "sodium": {"type": "float"},
                "category": {"type": "keyword"},
                "serving_size": {"type": "text"},
                "timestamp": {"type": "date"},
                "location": {"type": "geo_point"}
            }
        }
    
    def create_weather_mapping(self) -> Dict[str, Any]:
        """
        Create mapping for weather data
        
        Returns:
            Elasticsearch mapping for weather documents
        """
        return {
            "properties": {
                "location": {"type": "geo_point"},
                "city": {"type": "keyword"},
                "country": {"type": "keyword"},
                "temperature": {"type": "float"},
                "humidity": {"type": "float"},
                "pressure": {"type": "float"},
                "weather_condition": {"type": "keyword"},
                "wind_speed": {"type": "float"},
                "wind_direction": {"type": "float"},
                "visibility": {"type": "float"},
                "uv_index": {"type": "float"},
                "timestamp": {"type": "date"},
                "date": {"type": "date"}
            }
        }
    
    def setup_indices(self) -> Dict[str, bool]:
        """
        Setup all required indices with proper mappings
        
        Returns:
            Dict with setup status for each index
        """
        results = {}
        
        # Setup nutrition index
        nutrition_mapping = self.create_nutrition_mapping()
        results["nutrition"] = self.create_index_template("nutrition", nutrition_mapping)
        
        # Setup weather index
        weather_mapping = self.create_weather_mapping()
        results["weather"] = self.create_index_template("weather", weather_mapping)
        
        return results
    
    def process_csv_file(self, file_path: str, index_name: str) -> Dict[str, Any]:
        """
        Process a CSV file and index its contents
        
        Args:
            file_path: Path to the CSV file
            index_name: Target index name
            
        Returns:
            Processing results
        """
        try:
            if not os.path.exists(file_path):
                return {"success": False, "reason": f"File not found: {file_path}"}
              # Read CSV file
            df = pd.read_csv(file_path)
            
            if df.empty:
                return {"success": False, "reason": "CSV file is empty"}
            
            # Convert DataFrame to list of dictionaries
            documents = df.to_dict(orient='records')
            
            # Ensure all keys are strings and add timestamp if not present
            processed_documents = []
            for doc in documents:
                # Convert dict to ensure string keys
                str_doc = {str(k): v for k, v in doc.items()}
                if 'timestamp' not in str_doc:
                    str_doc['timestamp'] = datetime.now().isoformat()
                processed_documents.append(str_doc)
              # Index documents
            result = self.index_documents(index_name, processed_documents)
            result["file_path"] = file_path
            result["records_processed"] = len(processed_documents)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to process CSV file {file_path}: {e}")
            return {
                "success": False,
                "reason": str(e),
                "file_path": file_path
            }
    
    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """
        Get statistics for an index
        
        Args:
            index_name: Name of the index
            
        Returns:
            Index statistics
        """
        try:
            stats = self.es.indices.stats(index=index_name)
            return {
                "success": True,
                "index": index_name,
                "doc_count": stats['indices'][index_name]['total']['docs']['count'],
                "size_bytes": stats['indices'][index_name]['total']['store']['size_in_bytes']
            }
        except Exception as e:
            logger.error(f"❌ Failed to get stats for index {index_name}: {e}")
            return {
                "success": False,
                "reason": str(e),
                "index": index_name
            }

def create_indexer() -> ElasticsearchIndexer:
    """
    Factory function to create an ElasticsearchIndexer instance
    
    Returns:
        ElasticsearchIndexer instance
    """
    # Get configuration from environment variables
    es_host = os.getenv("ELASTICSEARCH_HOST", "https://es01:9200")
    es_username = os.getenv("ELASTICSEARCH_USERNAME", "elastic")
    es_password = os.getenv("ELASTICSEARCH_PASSWORD", "elastic")
    
    return ElasticsearchIndexer(
        host=es_host,
        username=es_username,
        password=es_password
    )

# Main execution functions for Airflow tasks
def setup_elasticsearch_indices() -> Dict[str, Any]:
    """
    Setup Elasticsearch indices - called by Airflow task
    """
    try:
        indexer = create_indexer()
        results = indexer.setup_indices()
        
        all_successful = all(results.values())
        
        return {
            "success": all_successful,
            "results": results,
            "message": "All indices setup successfully" if all_successful else "Some indices failed to setup"
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to setup indices: {e}")
        return {
            "success": False,
            "reason": str(e)
        }

def index_nutrition_data(file_path: str = "/opt/airflow/data/nutrition_data.csv") -> Dict[str, Any]:
    """
    Index nutrition data - called by Airflow task
    """
    try:
        indexer = create_indexer()
        result = indexer.process_csv_file(file_path, "nutrition-data")
        
        logger.info(f"Nutrition data indexing result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Failed to index nutrition data: {e}")
        return {
            "success": False,
            "reason": str(e),
            "file_path": file_path
        }

def index_weather_data(file_path: str = "/opt/airflow/data/weather_data.csv") -> Dict[str, Any]:
    """
    Index weather data - called by Airflow task
    """
    try:
        indexer = create_indexer()
        result = indexer.process_csv_file(file_path, "weather-data")
        
        logger.info(f"Weather data indexing result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Failed to index weather data: {e}")
        return {
            "success": False,
            "reason": str(e),
            "file_path": file_path
        }

if __name__ == "__main__":
    # Test the indexer
    try:
        indexer = create_indexer()
        print("✅ Elasticsearch connection successful")
        
        # Setup indices
        setup_results = indexer.setup_indices()
        print(f"Setup results: {setup_results}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
