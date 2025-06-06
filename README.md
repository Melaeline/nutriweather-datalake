# Nutriweather Datalake

A modern containerized data lake solution that combines nutritional and weather data processing using Apache Airflow and Apache Spark. This project demonstrates enterprise-grade data engineering practices with automated ETL pipelines, processing data from TheMealDB API and Open-Meteo weather API to enable comprehensive food production and agricultural analysis.

## 🏗️ Architecture Overview

The project implements a microservices architecture with the following technology stack:

### Core Technologies
- **Apache Airflow 2.8+**: Workflow orchestration and pipeline management
- **Apache Spark 3.5.6**: Distributed data processing and transformation  
- **Python 3.12**: Data processing scripts and pipeline logic
- **Docker & Docker Compose**: Container orchestration for development
- **Astronomer CLI**: Development and deployment tooling

### Data Technologies
- **Pandas**: Data manipulation and analysis
- **PyArrow**: Columnar data processing and Parquet format support
- **PySpark**: Large-scale data processing with Spark SQL
- **Parquet**: Optimized columnar storage format for analytics

### APIs & Data Sources
- **TheMealDB API**: Comprehensive meal and recipe database
- **Open-Meteo API**: Open-source weather forecast and historical data

## 🔄 Data Pipeline Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Extraction     │    │   Raw Storage   │
│                 │    │                  │    │                 │
│ • TheMealDB API │───▶│ • fetch_meals    │───▶│ • JSON files    │
│ • Open-Meteo    │    │ • fetch_weather  │    │ • CSV files     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Analytics Layer │    │  Transformation  │    │ Processed Data  │
│                 │    │                  │    │                 │
│ • BI Tools      │◀───│ • format_meals   │◀───│ • Parquet files │
│ • ML Models     │    │ • format_weather │    │ • Structured    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🚀 Data Pipeline

### Current Implementation
The system implements a complete Extract, Transform, Load (ETL) pipeline:

1. **Data Extraction** 
   - **Meals**: Fetch comprehensive meal data from TheMealDB API by searching A-Z
   - **Weather**: Extract hourly weather forecasts from Open-Meteo API for Paris coordinates
   - **Storage**: Raw data saved as JSON and CSV with timestamps

2. **Data Transformation**
   - **Cleaning**: Deduplication, null value handling, data type conversion
   - **Enhancement**: Computed fields (preparation time estimates, temperature categorization)
   - **Optimization**: Convert to Parquet format for 10x faster query performance

3. **Data Storage**
   - **Raw Layer**: `/include/raw/` - Timestamped JSON/CSV files for audit trail
   - **Processed Layer**: `/include/formatted/` - Optimized Parquet files for analytics
   - **Metadata**: Embedded fetch timestamps, data lineage, and quality metrics

### Pipeline Features
- **Automatic Triggering**: Fetch DAGs automatically trigger formatting DAGs
- **Error Handling**: Comprehensive retry logic with exponential backoff
- **Data Quality**: Built-in validation and logging for monitoring
- **Scalability**: Spark-based processing handles large datasets efficiently
- **Idempotency**: Timestamped outputs prevent data conflicts

## 📋 Prerequisites

### System Requirements
- **OS**: Windows 10/11, macOS 10.14+, or Linux (Ubuntu 18.04+)
- **Memory**: Minimum 8GB RAM (16GB recommended for Spark processing)
- **Storage**: 5GB free disk space for containers and data
- **Network**: Internet access for API calls and container downloads

### Required Software
- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux) - v20.10+
- **Docker Compose** v2.0+
- **Git** for version control
- **Astronomer CLI** (recommended) - `curl -sSL install.astronomer.io | sudo bash -s`

### Optional Tools
- **VS Code** with Docker extension for development
- **Apache Superset** for data visualization (future integration)
- **Jupyter Lab** for data exploration notebooks

## ⚡ Quick Start

### Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd nutriweather-datalake
   ```

2. **Start the services:**
   ```bash
   # Using Astronomer CLI (recommended)
   astro dev start
   
   # Or using Docker Compose directly
   docker-compose up -d
   ```

3. **Verify deployment:**
   ```bash
   # Check all services are running
   docker-compose ps
   
   # View startup logs
   docker-compose logs -f
   ```

4. **Access the interfaces:**
   - **Airflow UI**: http://localhost:8080 (admin/admin)
   - **Spark Master UI**: http://localhost:8081
   - **Container Health**: All services should show "healthy" status

### First Pipeline Run

1. Open Airflow UI at http://localhost:8080
2. Enable the `fetch_meals_dag` and `fetch_weather_dag`
3. Trigger both DAGs manually from the UI
4. Monitor execution in the Graph View
5. Check processed data in `/include/formatted/` directory

## 🛠️ Development Workflow

### Management Commands

| Command | Description | Use Case |
|---------|-------------|----------|
| `astro dev start` | Start the complete environment | Initial setup, development |
| `astro dev stop` | Stop all containers gracefully | End of work session |
| `astro dev restart` | Restart all services | After configuration changes |
| `astro dev ps` | Show container status | Health monitoring |
| `astro dev logs` | View aggregated logs | Debugging issues |
| `astro dev bash` | Access Airflow scheduler shell | Direct container access |

### Docker Compose Commands (Alternative)

| Command | Description | Advanced Usage |
|---------|-------------|----------------|
| `docker-compose up -d` | Start in detached mode | Production-like setup |
| `docker-compose down` | Stop and remove containers | Clean shutdown |
| `docker-compose logs -f <service>` | Follow specific service logs | `spark-master`, `airflow-scheduler` |
| `docker-compose exec <service> bash` | Access service shell | `spark-master`, `airflow-webserver` |
| `docker-compose build` | Rebuild custom images | After Dockerfile changes |

## 📁 Project Structure

```
nutriweather-datalake/
├── 🏗️ Infrastructure
│   ├── docker-compose.override.yml    # Spark cluster configuration
│   ├── Dockerfile                     # Custom Airflow image (OpenJDK-17)
│   ├── requirements.txt               # Python dependencies
│   ├── airflow_settings.yaml         # Connections & variables
│   └── packages.txt                   # System packages
│
├── 🔄 Pipeline Definitions
│   └── dags/                          # Airflow DAG definitions
│       ├── fetch_meals_dag.py         # TheMealDB data extraction
│       ├── format_meals_dag.py        # Meal data transformation
│       ├── fetch_weather_dag.py       # Open-Meteo weather extraction  
│       └── format_weather_dag.py      # Weather data transformation
│
├── 📊 Data Processing
│   └── include/
│       ├── scripts/                   # Processing logic
│       │   ├── fetch_meals.py         # MealDB API client
│       │   ├── format_meals.py        # PySpark meal processing
│       │   └── format_weather.py      # Weather data transformation
│       ├── raw/                       # Raw data storage (runtime)
│       │   ├── meals/                 # JSON files with timestamps
│       │   └── weather/               # CSV + JSON files with timestamps
│       └── formatted/                 # Processed data (runtime)
│           ├── meals/                 # Parquet files
│           └── weather/               # Parquet files
│
├── ⚡ Spark Applications
│   ├── apps/                          # Custom Spark applications
│   └── data/                          # Spark output directory
│
└── 🧪 Quality Assurance
    └── tests/
        └── dags/                      # DAG validation tests
            └── test_dag_example.py    # Syntax and structure tests
```

### Key Directories Explained

- **`dags/`**: Contains all Airflow DAG definitions following TaskFlow API patterns
- **`include/scripts/`**: Reusable Python modules for data processing logic
- **`include/raw/`**: Raw data storage with timestamped files for audit trail
- **`include/formatted/`**: Optimized Parquet files for analytical queries
- **`apps/`**: Custom Spark applications and job definitions
- **Runtime directories**: Created automatically during pipeline execution

## 🔧 Configuration & Environment

### Infrastructure Configuration

#### Spark Cluster Settings
```yaml
# docker-compose.override.yml
spark-master:
  image: bitnami/spark:3.5.6
  ports: ['8081:8081', '7077:7077']
  environment:
    SPARK_MASTER_WEBUI_PORT: 8081
    
spark-worker:
  image: bitnami/spark:3.5.6  
  environment:
    SPARK_WORKER_MEMORY: 2G
    SPARK_WORKER_CORES: 2
```

#### Airflow Connections
```yaml
# airflow_settings.yaml
connections:
  - conn_id: my_spark_conn
    conn_type: spark
    conn_host: spark://spark-master
    conn_port: 7077
```

#### Python Dependencies
```pip
# requirements.txt - Key packages
pyspark==3.5.6                        # Distributed processing
apache-airflow-providers-apache-spark  # Spark integration
requests>=2.28.0                       # HTTP API calls
pandas>=1.5.0                          # Data manipulation
pyarrow>=10.0.0                        # Parquet format support
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Task execution method |
| `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `postgresql://...` | Database connection |
| `SPARK_MASTER_URL` | `spark://spark-master:7077` | Spark cluster endpoint |
| `JAVA_HOME` | `/usr/lib/jvm/java-17-openjdk-amd64` | Java runtime path |

### API Configuration

#### Weather Data (Open-Meteo)
- **Endpoint**: `https://api.open-meteo.com/v1/forecast`
- **Location**: Paris, France (48.8534°N, 2.3488°E)
- **Parameters**: Temperature, humidity, cloud cover, wind direction
- **Frequency**: Hourly forecasts, 1-day ahead

#### Meal Data (TheMealDB)
- **Endpoint**: `https://www.themealdb.com/api/json/v1/1/search.php`
- **Method**: Alphabetical search (A-Z) for comprehensive coverage
- **Rate Limiting**: Built-in delays to respect API limits

## 📊 Data Pipeline Details

### DAG Architecture & Execution Flow

#### 1. 🍽️ Fetch Meals DAG (`fetch_meals_dag`)
```python
# Pipeline: API → Raw JSON → Trigger Formatting
Tasks: extract_meals_from_api → save_raw_meals → trigger_formatting
```
- **Purpose**: Extract comprehensive meal database from TheMealDB
- **Schedule**: Manual trigger (on-demand execution)
- **Data Volume**: ~300 meals covering global cuisines
- **Output Format**: Timestamped JSON files in `/include/raw/meals/`
- **Error Handling**: 3 retries with exponential backoff
- **Trigger**: Automatically launches `format_meals_dag` upon completion

#### 2. 🏭 Format Meals DAG (`format_meals_dag`)
```python
# Pipeline: Raw JSON → PySpark Processing → Parquet
Tasks: format_meals (PySpark job execution)
```
- **Purpose**: Transform and enhance raw meal data for analytics
- **Schedule**: Triggered by fetch DAG completion
- **Processing Engine**: PySpark for distributed computation
- **Enhancements**: 
  - Estimated preparation time based on meal complexity
  - Temperature categorization (hot/cold/ambient)
  - Data deduplication by meal ID
  - Ingredient parsing and standardization
- **Output**: Optimized Parquet files in `/include/formatted/meals/`

#### 3. 🌤️ Fetch Weather DAG (`fetch_weather_dag`)
```python
# Pipeline: API → Raw CSV + JSON → Trigger Formatting  
Tasks: extract_weather_from_api → trigger_formatting
```
- **Purpose**: Extract hourly weather forecasts from Open-Meteo API
- **Schedule**: Manual trigger (configurable for automation)
- **Location**: Paris, France coordinates (48.8534°N, 2.3488°E)
- **Data Points**: Temperature, humidity, cloud cover, wind direction
- **Dual Storage**: 
  - CSV format for structured data analysis
  - JSON format preserving complete API response
- **Forecast Range**: 24-hour ahead predictions with hourly granularity

#### 4. 🌦️ Format Weather DAG (`format_weather_dag`)
```python
# Pipeline: Raw CSV/JSON → Python Processing → Parquet
Tasks: format_weather (Python script execution)
```
- **Purpose**: Standardize and optimize weather data for queries
- **Schedule**: Triggered by fetch DAG completion  
- **Multi-format Support**: Handles both CSV (preferred) and JSON (legacy)
- **Processing Features**:
  - Time zone normalization
  - Unit conversion and standardization
  - Data quality validation
  - Missing value imputation
- **Output**: Parquet files optimized for time-series analysis

### Data Processing Features

- **Deduplication**: Removes duplicate meals by ID
- **Data Enhancement**: Adds computed fields (prep time, temperature estimates)
- **Format Optimization**: Converts JSON/CSV to Parquet for better performance
- **Error Handling**: Comprehensive logging and retry mechanisms
- **Multi-format Support**: Handles CSV (weather) and JSON (meals) raw data

## Configuration

### Environment Variables

Key configurations are managed through:
- `airflow_settings.yaml`: Airflow connections and variables
- `docker-compose.override.yml`: Spark cluster settings
- `requirements.txt`: Python package dependencies

### Spark Configuration

The Spark cluster includes:
- 1 Master node (Web UI on port 8081)
- 1 Worker node (2GB memory, 2 cores)
- Shared volumes for data exchange with Airflow

## Monitoring & Troubleshooting

### Health Checks

1. **Airflow**: Check http://localhost:8080 for UI access
2. **Spark**: Check http://localhost:8081 for cluster status
3. **Containers**: Run `docker-compose ps` to verify all services are running

### Common Issues

**Problem**: Airflow UI not accessible
```bash
# Solution: Check container status and restart if needed
docker-compose ps
docker-compose restart
```

**Problem**: Spark jobs failing
```bash
# Solution: Check Spark logs and resource allocation
docker-compose logs spark-master
docker-compose logs spark-worker
```

**Problem**: Out of disk space
```bash
# Solution: Clean up old data and Docker volumes
docker system prune -a
docker volume prune
```

### Log Analysis

```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark-master

# Access container for debugging
docker-compose exec airflow-scheduler bash
```

## Development Guidelines

### Adding New DAGs

1. Create Python file in `dags/` directory
2. Use Airflow's TaskFlow API for clean task definitions
3. Follow naming convention: `<purpose>_dag.py`
4. Add comprehensive docstrings and tags

### Data Processing Scripts

1. Place scripts in `include/scripts/`
2. Use PySpark for distributed processing
3. Implement proper error handling and logging
4. Save outputs with timestamps for traceability

### Testing

```bash
# Test DAG syntax
docker-compose exec airflow-scheduler bash
python -m py_compile dags/your_dag.py

# Run specific DAG tasks
airflow tasks test <dag_id> <task_id> <execution_date>
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Test your changes locally with `docker-compose up -d`
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## Future Roadmap

- [ ] Weather data integration (OpenWeatherMap API)
- [ ] Real-time data streaming with Apache Kafka
- [ ] Data visualization dashboard with Apache Superset
- [ ] Machine learning models for yield prediction
- [ ] Data quality monitoring and alerting
- [ ] Production deployment with Kubernetes

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Astronomer CLI Documentation](https://docs.astronomer.io/astro/cli/overview)
- [TheMealDB API Documentation](https://www.themealdb.com/api.php)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting section above
- Review Airflow and Spark documentation