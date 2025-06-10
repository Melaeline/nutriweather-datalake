# NutriWeather Data Lake

A comprehensive data engineering project that combines weather data with meal recommendations using Apache Airflow, Apache Spark, Elasticsearch, and HDFS. This pipeline fetches real-time weather data and meal information, processes them through multiple stages, and provides intelligent meal recommendations based on current weather conditions.

**🕐 Scheduling**: The pipeline runs automatically every minute to provide continuous, real-time data updates.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Processing    │    │    Storage &    │
│                 │    │    Pipeline     │    │  Visualization  │
│                 │    │  (Every Minute) │    │                 │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ Open-Meteo API │────▶│ Apache Airflow  │────▶│ HDFS Cluster    │
│ TheMealDB API   │    │ Apache Spark    │    │ Elasticsearch   │
│ OSM Nominatim   │    │ PySpark         │    │ Kibana          │
└─────────────────┘    └─────────────────┘    │ Local Files     │
                                              └─────────────────┘
```

## 🛠️ Technology Stack

### Core Technologies
- **Apache Airflow 2.9.2** - Workflow orchestration and scheduling
- **Apache Spark 3.5.6** - Distributed data processing
- **PySpark** - Python API for Spark
- **Apache Hadoop HDFS 3.3.5** - Distributed file storage
- **Elasticsearch 8.15.0** - Search and analytics engine
- **Kibana 8.15.0** - Data visualization and exploration
- **Docker & Docker Compose** - Containerization and orchestration

### Development & Runtime
- **Astronomer Runtime 3.0-2** - Airflow distribution
- **Python 3.11+** - Primary programming language
- **OpenJDK 17** - Java runtime for Spark and Hadoop
- **Bitnami Spark Images** - Pre-configured Spark containers
- **Apache Hadoop Official Images** - HDFS cluster components

### Data Processing Libraries
- **pandas 1.5.0+** - Data manipulation and analysis
- **pyarrow 10.0.0+** - Columnar data format support
- **numpy 1.26.0+** - Numerical computing
- **requests 2.31.0+ (< 2.33.0)** - HTTP client library (version-pinned for urllib3 compatibility)
- **urllib3 1.26.0+ (< 2.3.0)** - HTTP library (compatible with requests)
- **hdfs 2.7.0** - Python HDFS client for distributed file operations

### API Integration
- **openmeteo-requests 1.1.0+** - Weather API client
- **requests-cache 1.1.0+** - HTTP request caching
- **retry-requests 2.0.0+** - Request retry mechanism

## 🌐 External APIs

### 1. Open-Meteo Weather API
- **Endpoint**: `https://api.open-meteo.com/v1/forecast`
- **Purpose**: Real-time weather data collection
- **Data Retrieved**:
  - Current temperature, humidity, wind speed
  - Hourly temperature forecasts
  - Daily UV index maximum
  - Location metadata (coordinates, timezone)
- **Rate Limits**: Free tier, no authentication required
- **Coverage**: Global weather data with high accuracy

### 2. TheMealDB API
- **Endpoint**: `https://www.themealdb.com/api/json/v1/1`
- **Purpose**: Comprehensive meal database access
- **Data Retrieved**:
  - Meal names, categories, and regions
  - Detailed cooking instructions
  - Ingredient lists with measurements
  - Meal thumbnails and metadata
- **Coverage**: 1000+ international recipes
- **Search Method**: Alphabetical iteration (a-z) for complete dataset

### 3. OpenStreetMap Nominatim
- **Endpoint**: `https://nominatim.openstreetmap.org/reverse`
- **Purpose**: Reverse geocoding for location names
- **Usage**: Convert coordinates to human-readable locations
- **Rate Limits**: 1 request per second (implemented with delays)

## 📊 Data Pipeline Flow with Continuous Updates

### Real-time Data Processing (Every Minute)
```
Minute 1: fetch_weather → format_weather → merge → index_elasticsearch
Minute 2: fetch_weather → format_weather → merge → index_elasticsearch
Minute 3: fetch_weather → format_weather → merge → index_elasticsearch
...continuous execution...
```

### Optimized Execution Strategy
- **Weather Data**: Fresh API calls with intelligent caching
- **Meal Data**: Consider reducing frequency to hourly for full dataset refresh
- **Processing**: Incremental updates where possible
- **Storage**: Efficient file management with automatic cleanup options

## 🚀 DAG Execution & Orchestration

### Primary DAG: `start_pipeline_dag`
**Trigger**: Automatic execution every minute
**Schedule**: `*/1 * * * *` (CRON expression for every minute)
**Max Active Runs**: 1 (prevents concurrent executions)

#### Scheduling Benefits
- **Real-time Updates**: Fresh weather data every minute
- **Continuous Monitoring**: Constant data availability for dashboards
- **Fault Tolerance**: Automatic recovery from transient failures
- **Rate Limiting**: Built-in protections for API endpoints

#### Performance Optimizations for Frequent Execution
- **Smart Caching**: Weather data cached for 2 minutes to reduce API calls
- **Rate Limiting**: Respectful delays between external API requests
- **Duplicate Detection**: Elasticsearch indexing handles duplicate data gracefully
- **Resource Management**: Single active run prevents resource conflicts

#### Task Details (Optimized for Minute-by-Minute Execution)
1. **fetch_meals_data** (Python Task)
   - Runtime: ~2-3 minutes
   - **Optimization**: Rate limiting between API calls (0.1s delay)
   - **Consideration**: Full dataset fetch may not be needed every minute
   - **HDFS**: Automatic backup to `/nutriweather/raw/meals/`

2. **fetch_weather_data** (Bash → Python Script)
   - Runtime: ~10-15 seconds
   - **Optimization**: Skips API call if data is less than 2 minutes old
   - **Rate Limiting**: Respects Open-Meteo free tier limits
   - **HDFS**: Automatic backup to `/nutriweather/raw/weather/`

3. **format_meals_data** (Spark Job)
   - Runtime: ~30-45 seconds
   - **Optimization**: Efficient processing of latest data only
   - **HDFS**: Parquet files backed up to `/nutriweather/formatted/meals/`

4. **format_weather_data** (Python Script)
   - Runtime: ~5-10 seconds
   - **Optimization**: 1-second delay for Nominatim API respect
   - **HDFS**: Enhanced JSON backed up to `/nutriweather/formatted/weather/`

5. **merge_formatted_data** (Spark + Python)
   - Runtime: ~15-20 seconds
   - **Optimization**: Processes latest data efficiently
   - **HDFS**: Both outputs backed up to respective directories

6. **trigger_elasticsearch_indexing** (TriggerDAG)
   - **Optimization**: Duplicate detection prevents index bloat
   - **Performance**: Bulk indexing with smart batching

## 📂 File Formats & Data Organization

### Directory Structure
```
Local Storage (/usr/local/airflow/include/):
├── raw/
│   ├── meals/           # JSON files from TheMealDB API
│   └── weather/         # JSON files from Open-Meteo API
├── formatted/
│   ├── meals/           # Parquet files (optimized for Spark)
│   └── weather/         # JSON files (enriched with location data)
├── usage/               # JSONL files (Elasticsearch-ready)
└── scripts/             # Python processing modules

HDFS Storage (/nutriweather/):
├── raw/                 # All raw data files (meals + weather)
├── formatted/           # All formatted data files (meals + weather)
├── usage/               # All usage layer files (timeseries + recommendations)
├── indexed/             # Elasticsearch indexing metadata
└── backup/              # General backup location
```

### HDFS Integration Benefits
- **Fault Tolerance**: 2x replication across DataNodes
- **Scalability**: Horizontal scaling for large datasets
- **Historical Storage**: Long-term archival of all pipeline outputs
- **Cross-Platform Access**: Available to Spark, Python, and external tools
- **Backup Strategy**: Automatic distributed backup for critical data
- **Real-time Sync**: All pipeline outputs automatically backed up to HDFS
- **Disaster Recovery**: Complete data recovery from HDFS in case of local storage failure

### File Naming Convention
- **Pattern**: `{type}_{category}_{YYYYMMDD_HHMMSS}.{extension}`
- **Example**: `formatted_meals_20241219_153045.parquet`
- **Benefits**: Chronological sorting, easy latest file identification

### Format Justifications
- **Raw → JSON**: Preserves original API response structure
- **Formatted Meals → Parquet**: Columnar format optimized for Spark operations
- **Formatted Weather → JSON**: Maintains nested structure for complex weather data
- **Usage → JSONL**: Elasticsearch bulk indexing compatibility

## 🚨 Error Handling & Recovery for Continuous Operation

### Retry Mechanisms (Enhanced for Frequent Execution)
- **Airflow Tasks**: 2 retries with 5-minute delays
- **API Requests**: Exponential backoff with retry-requests
- **Rate Limiting**: Smart delays to respect API limits
- **Resource Management**: Single active run prevents conflicts

### Monitoring for Continuous Operation
- **Task Duration Tracking**: Monitor for performance degradation
- **API Rate Limit Monitoring**: Track API usage patterns
- **Storage Growth**: Monitor disk usage with continuous data generation
- **HDFS Health**: Continuous backup verification

## 🔮 Future Enhancements for Continuous Operation

### Planned Optimizations
- **Incremental Processing**: Process only changed data
- **Smart Caching**: Redis-based caching for frequently accessed data
- **Dynamic Scheduling**: Adjust frequency based on data change patterns
- **Resource Optimization**: Auto-scaling based on execution patterns

### Performance Monitoring
- **Execution Time Trends**: Track pipeline performance over time
- **Resource Usage**: Monitor CPU, memory, and storage patterns
- **API Usage Analytics**: Optimize API call patterns
- **Data Quality Metrics**: Continuous validation of incoming data

## ⚠️ Important Considerations for Minute-by-Minute Execution

### API Rate Limits
- **Open-Meteo**: Free tier, monitor usage patterns
- **TheMealDB**: Consider caching strategy for full dataset
- **Nominatim**: 1 request per second limit enforced

### Storage Management
- **Rapid Data Growth**: ~1440 executions per day
- **Cleanup Strategy**: Consider automated file retention policies
- **HDFS Capacity**: Monitor storage usage trends

### Resource Usage
- **CPU Load**: Continuous Spark job execution
- **Memory Usage**: Multiple concurrent processes
- **Network Bandwidth**: Frequent API calls and data transfers

## 🔧 Configuration for Scheduled Execution

### Environment Variables
```yaml
# Airflow Configuration
AIRFLOW_UID: 50000
AIRFLOW_HOME: /usr/local/airflow

# Scheduling Configuration
PIPELINE_SCHEDULE: "*/1 * * * *"  # Every minute
MAX_ACTIVE_RUNS: 1
CATCHUP: false

# Spark Configuration
SPARK_MASTER_URL: spark://spark-master:7077
SPARK_WORKER_MEMORY: 2G

# Elasticsearch Configuration
ELASTICSEARCH_HOSTS: http://es01:9200

# HDFS Configuration
HDFS_NAMENODE_URL: http://namenode:9870
HDFS_NAMENODE_API: hdfs://namenode:8020
```

## 🚀 Quick Start

### Local Development Setup
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd nutriweather-datalake
   ```

2. **Create HDFS Configuration Directory**:
   ```bash
   mkdir -p hdfs_config scripts
   ```

3. **Start All Services (Including HDFS Cluster)**:
   ```bash
   docker-compose up -d
   ```

4. **Monitor Logs (Including HDFS Services)**:
   ```bash
   docker-compose logs -f
   ```

5. **Access Services**:
   - Airflow: http://localhost:8080 (admin/admin)
   - Kibana: http://localhost:5601
   - Spark UI: http://localhost:8082
   - Elasticsearch: http://localhost:9200
   - HDFS Web UI: http://localhost:9870

### Astro CLI Integration
```bash
# Install Astro CLI
curl -sSL install.astronomer.io | sudo bash

# Initialize Airflow project
astro dev init

# Start development environment
astro dev start

# Deploy to production
astro deploy
```

## 📈 Monitoring & Observability

### Airflow Monitoring
- **Web UI**: Task status, logs, execution history
- **Metrics**: Task duration, success rate, resource usage
- **Alerting**: Email notifications on failure (configurable)

### Spark Monitoring
- **Spark UI**: Job execution, stage details, executor status
- **Metrics**: Memory usage, task distribution, shuffle operations
- **Logs**: Driver and executor logs via Docker

### Elasticsearch Health
- **Cluster Health**: `GET /_cluster/health`
- **Index Statistics**: `GET /_stats`
- **Document Counts**: Real-time via Kibana dashboards

### HDFS Cluster Health
- **NameNode Web UI**: `http://localhost:9870` - Cluster overview, DataNode status
- **Filesystem Health**: `GET /webhdfs/v1/?op=GETFILESTATUS`
- **Block Reports**: DataNode health and storage utilization
- **Replication Status**: File replication factor and under-replicated blocks
- **Backup Monitoring**: Verify all pipeline files are backed up correctly

## 🔧 Development Setup

### Prerequisites
- Docker Desktop 4.0+
- Docker Compose V2
- 8GB+ available RAM
- 10GB+ free disk space

### Installation & Startup
```bash
# Clone repository
git clone <repository-url>
cd nutriweather-datalake

# Create HDFS configuration directory
mkdir -p hdfs_config scripts

# Start all services (including HDFS cluster)
docker-compose up -d

# Monitor logs (including HDFS services)
docker-compose logs -f

# Access services
# Airflow: http://localhost:8080 (admin/admin)
# Kibana: http://localhost:5601
# Spark UI: http://localhost:8082
# Elasticsearch: http://localhost:9200
# HDFS Web UI: http://localhost:9870
```

### Astro CLI Integration
```bash
# Install Astro CLI
curl -sSL install.astronomer.io | sudo bash

# Initialize Airflow project
astro dev init

# Start development environment
astro dev start

# Deploy to production
astro deploy
```

## 🔧 HDFS Integration Examples

### Python HDFS Client Usage in Pipeline Scripts
```python
from spark_utils import get_hdfs_client, backup_to_hdfs, save_with_hdfs_backup

# Automatic backup during file creation
save_with_hdfs_backup("/local/path/data.json", data_dict, "json")

# Manual backup of existing files
hdfs_client = get_hdfs_client()
backup_to_hdfs("/local/file.parquet", "/nutriweather/formatted/meals", hdfs_client)

# Read historical data from HDFS for reprocessing
client = get_hdfs_client()
with client.read('/nutriweather/raw/weather/archive.json') as reader:
    historical_data = reader.read()
```

### Spark-HDFS Integration for Large Scale Processing
```python
# Read from HDFS in Spark jobs for historical analysis
df = spark.read.parquet("hdfs://namenode:8020/nutriweather/formatted/meals/")

# Write aggregated results back to HDFS
df.write.mode("append").parquet("hdfs://namenode:8020/nutriweather/analytics/")
```

### HDFS Data Recovery Scenarios
```python
# Recover latest processed meals if local storage fails
client = get_hdfs_client()
files = client.list('/nutriweather/formatted/meals/')
latest_meals = max(files, key=lambda x: x.split('_')[-1])
client.download(f'/nutriweather/formatted/meals/{latest_meals}', '/local/recovery/')
```

## 🚨 Error Handling & Recovery

### Dependency Management
- **Version Compatibility**: Fixed urllib3/requests compatibility issues
- **Pinned Versions**: Critical dependencies pinned to prevent breaking changes
- **Container Isolation**: Dependencies managed within Docker containers

### Retry Mechanisms
- **Airflow Tasks**: 2 retries with 5-minute delays
- **API Requests**: Exponential backoff with retry-requests
- **Spark Jobs**: Automatic task retry on executor failure
- **HDFS Operations**: Automatic retry with connection recovery

### Data Validation
- **Schema Validation**: Spark DataFrame schema enforcement
- **Null Checks**: Required field validation before processing
- **API Response Validation**: HTTP status and content verification
- **HDFS Backup Verification**: Confirm successful backup operations

### HDFS Fault Tolerance
- **DataNode Failure**: Automatic block replication to healthy nodes
- **NameNode Recovery**: Metadata checkpoint and journal recovery
- **Network Partitions**: Client retry with exponential backoff
- **Storage Full**: Automatic load balancing across DataNodes
- **Backup Recovery**: Complete pipeline restart from HDFS if local storage fails

### Monitoring Alerts
- **Failed Tasks**: Immediate notification via Airflow
- **Service Health**: Docker container health checks
- **Data Quality**: Custom validation rules in processing scripts
- **HDFS Health**: NameNode and DataNode availability monitoring
- **Backup Status**: Verify all files are successfully backed up to HDFS

## 🎯 Use Cases & Applications

### Business Intelligence
- **Weather-Food Correlation Analysis**: Understand eating patterns vs. weather
- **Seasonal Menu Planning**: Restaurant menu optimization
- **Supply Chain**: Weather-based ingredient demand forecasting
- **Historical Analysis**: Multi-year trend analysis using HDFS archived data

### Personal Applications
- **Smart Meal Planning**: Automated meal suggestions based on weather
- **Health & Nutrition**: Weather-appropriate nutrition recommendations
- **Recipe Discovery**: Context-aware recipe recommendations

### Data Science Projects
- **Machine Learning**: Weather-food preference modeling using historical HDFS data
- **Predictive Analytics**: Meal demand forecasting
- **Time Series Analysis**: Weather pattern correlation with food choices

## 📝 Configuration Management

### Environment Variables
```yaml
# Airflow Configuration
AIRFLOW_UID: 50000
AIRFLOW_HOME: /usr/local/airflow

# Scheduling Configuration
PIPELINE_SCHEDULE: "*/1 * * * *"  # Every minute
MAX_ACTIVE_RUNS: 1
CATCHUP: false

# Spark Configuration
SPARK_MASTER_URL: spark://spark-master:7077
SPARK_WORKER_MEMORY: 2G

# Elasticsearch Configuration
ELASTICSEARCH_HOSTS: http://es01:9200

# HDFS Configuration
HDFS_NAMENODE_URL: http://namenode:9870
HDFS_NAMENODE_API: hdfs://namenode:8020
```
