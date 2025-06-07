es ="""
## Elasticsearch Data Indexing DAG

This DAG handles indexing of nutritional and weather data to Elasticsearch for analytics and search capabilities.
Uses Airflow 3.0.2 TaskFlow API for better task management and error handling.

The DAG performs the following operations:
- Checks Elasticsearch connectivity
- Indexes weather data from formatted files
- Indexes meals/nutritional data
- Creates and indexes usage analytics
- Handles bulk indexing with proper error handling
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch
import sys
import os
import logging
import traceback
from time import sleep
from urllib3.exceptions import NewConnectionError
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout

# Add the include directory to the Python path
sys.path.append('/usr/local/airflow/include')

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("elasticsearch").setLevel(logging.DEBUG)
logging.getLogger("elastic_transport").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

def get_es_connection_params():
    conn = BaseHook.get_connection("elasticsearch_default")
    return {
        "host": conn.host,
        "port": conn.port,
        "schema": conn.schema,
        "username": conn.login,
        "password": conn.password,
        "verify_certs": False,
        "ssl_show_warn": False  # Disable SSL warnings
    }

@dag(
    dag_id='elasticsearch_indexing_dag',
    description='Index nutritional and weather data to Elasticsearch using Airflow 3.0.2',
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['elasticsearch', 'indexing', 'analytics', 'v3.0.2'],
    default_args={
        'owner': 'nutriweather-team',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    doc_md=__doc__,
    max_active_runs=1,
)
def elasticsearch_indexing_dag():

    @task(task_id='check_elasticsearch_connection', retries=3, retry_delay=timedelta(seconds=5))
    def check_elasticsearch_connection():
        """Check if Elasticsearch is accessible"""
        params = get_es_connection_params()
        logger.info(f"Connecting to Elasticsearch with: host={params['host']}, port={params['port']}, scheme={params['schema']}, username={params['username']}, verify_certs={params['verify_certs']}")

        es = Elasticsearch(
            f"{params['schema']}://{params['host']}:{params['port']}",
            basic_auth=(params['username'], params['password']),
            verify_certs=params['verify_certs'],
            ssl_show_warn=params['ssl_show_warn'],
            retry_on_timeout=True,
            max_retries=3,
            timeout=30,
            http_compress=False,  # Disable HTTP compression
            keep_alive=False      # Disable persistent connections
        )

        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                health = es.cluster.health(wait_for_status='yellow', timeout='10s')
                logger.info(f"✅ Elasticsearch connection successful - cluster status: {health['status']}")
                return {"status": "success", "message": "Elasticsearch connection successful", "health": health}
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                    sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                logger.error(f"❌ Failed to connect to Elasticsearch after {max_retries} attempts: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"❌ Unexpected error: {str(e)}")
                logger.error(traceback.format_exc())
                raise
        try:
            health = es.cluster.health(wait_for_status='yellow', timeout='10s')
            logger.info(f"✅ Elasticsearch connection successful - cluster status: {health['status']}")
            return {"status": "success", "message": "Elasticsearch connection successful", "health": health}
        except Exception as e:
            logger.error(f"❌ Error connecting to Elasticsearch: {str(e)}")
            raise

    @task(task_id='index_weather_data')
    def index_weather_data():
        """Index weather data to Elasticsearch"""
        from scripts.elasticsearch_indexer import ElasticsearchIndexer

        logger.info("🌤️ Starting weather data indexing...")

        params = get_es_connection_params()
        indexer = ElasticsearchIndexer(
            host=params['host'],
            port=params['port'],
            schema=params['schema'],
            username=params['username'],
            password=params['password'],
            verify_certs=params['verify_certs']
        )
        weather_path = "/usr/local/airflow/include/formatted/weather"

        if os.path.exists(weather_path):
            result = indexer.index_weather_data(weather_path)
            logger.info("✅ Weather data indexing completed")
            return {"status": "success", "indexed_files": result}
        else:
            logger.warning("⚠️ No weather data found to index")
            return {"status": "warning", "message": "No weather data found"}

    @task(task_id='index_meals_data')
    def index_meals_data():
        """Index meals data to Elasticsearch"""
        from scripts.elasticsearch_indexer import ElasticsearchIndexer

        logger.info("🍽️ Starting meals data indexing...")

        params = get_es_connection_params()
        indexer = ElasticsearchIndexer(
            host=params['host'],
            port=params['port'],
            schema=params['schema'],
            username=params['username'],
            password=params['password'],
            verify_certs=params['verify_certs']
        )
        meals_path = "/usr/local/airflow/include/formatted/meals"

        if os.path.exists(meals_path):
            result = indexer.index_meals_data(meals_path)
            logger.info("✅ Meals data indexing completed")
            return {"status": "success", "indexed_files": result}
        else:
            logger.warning("⚠️ No meals data found to index")
            return {"status": "warning", "message": "No meals data found"}

    @task(task_id='create_usage_analytics')
    def create_usage_analytics():
        """Create analytics data for usage patterns"""
        from scripts.elasticsearch_indexer import ElasticsearchIndexer
        import json
        import random

        logger.info("📊 Creating usage analytics data...")

        # Generate sample usage analytics
        usage_data = []

        # Generate data for the last 30 days
        for i in range(30):
            date = datetime.now() - timedelta(days=i)

            # Simulate daily usage patterns
            usage_data.extend([
                {
                    "@timestamp": date.isoformat(),
                    "data_type": "weather_queries",
                    "count": random.randint(50, 200),
                    "avg_response_time": random.uniform(0.1, 2.0),
                    "peak_hour": random.randint(8, 18),
                    "unique_locations": random.randint(10, 50)
                },
                {
                    "@timestamp": date.isoformat(),
                    "data_type": "meal_searches",
                    "count": random.randint(30, 150),
                    "avg_response_time": random.uniform(0.2, 1.5),
                    "popular_categories": ["Breakfast", "Lunch", "Dinner"][random.randint(0, 2)],
                    "unique_recipes": random.randint(5, 25)
                }
            ])

        # Save to usage directory
        usage_path = "/usr/local/airflow/include/usage"
        os.makedirs(usage_path, exist_ok=True)

        filename = f"{usage_path}/usage_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(usage_data, f, indent=2)

        # Index to Elasticsearch
        params = get_es_connection_params()
        indexer = ElasticsearchIndexer(
            host=params['host'],
            port=params['port'],
            schema=params['schema'],
            username=params['username'],
            password=params['password'],
            verify_certs=params['verify_certs']
        )

        # Create usage analytics mapping
        usage_mapping = {
            "properties": {
                "@timestamp": {"type": "date"},
                "data_type": {"type": "keyword"},
                "count": {"type": "integer"},
                "avg_response_time": {"type": "float"},
                "peak_hour": {"type": "integer"},
                "unique_locations": {"type": "integer"},
                "popular_categories": {"type": "keyword"},
                "unique_recipes": {"type": "integer"}
            }
        }

        indexer.create_index_template("nutriweather-usage", usage_mapping)

        # Index the usage data
        date_str = datetime.now().strftime('%Y-%m-%d')
        index_name = f"nutriweather-usage-{date_str}"

        for doc in usage_data:
            if '@timestamp' not in doc:
                doc['@timestamp'] = datetime.now().isoformat()

        indexer._bulk_index(usage_data, index_name)

        logger.info(f"✅ Created and indexed {len(usage_data)} usage analytics records")
        return {
            "status": "success",
            "records_created": len(usage_data),
            "filename": filename,
            "index_name": index_name
        }

    @task(task_id='index_usage_data')
    def index_usage_data():
        """Index usage data to Elasticsearch"""
        from scripts.elasticsearch_indexer import ElasticsearchIndexer

        logger.info("📈 Starting usage data indexing...")

        params = get_es_connection_params()
        indexer = ElasticsearchIndexer(
            host=params['host'],
            port=params['port'],
            schema=params['schema'],
            username=params['username'],
            password=params['password'],
            verify_certs=params['verify_certs']
        )
        usage_path = "/usr/local/airflow/include/usage"

        if os.path.exists(usage_path):
            result = indexer.index_usage_data(usage_path)
            logger.info("✅ Usage data indexing completed")
            return {"status": "success", "indexed_files": result}
        else:
            logger.warning("⚠️ No usage data found to index")
            return {"status": "warning", "message": "No usage data found"}

    # Wait for Elasticsearch to be ready using BashOperator
    wait_for_elasticsearch = BashOperator(
        task_id='wait_for_elasticsearch',
        bash_command='''
        echo "Waiting for Elasticsearch to be ready..."
        for i in {1..30}; do
            if curl -k -u elastic:elastic -s https://es01:9200/_cluster/health | grep -q '"status":"green"\\|"status":"yellow"'; then
                echo "✅ Elasticsearch is ready"
                exit 0
            fi
            echo "⏳ Attempt $i/30 - Elasticsearch not ready yet, waiting 10 seconds..."
            sleep 10
        done
        echo "❌ Elasticsearch failed to become ready after 5 minutes"
        exit 1
        ''',
    )

    # Define task dependencies using the TaskFlow API
    es_check = check_elasticsearch_connection()
    weather_task = index_weather_data()
    meals_task = index_meals_data()
    analytics_task = create_usage_analytics()
    usage_task = index_usage_data()

    # Set up dependencies
    wait_for_elasticsearch >> es_check
    es_check >> [weather_task, meals_task, analytics_task, usage_task]


# Instantiate the DAG
elasticsearch_indexing_dag_instance = elasticsearch_indexing_dag()