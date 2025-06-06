# Nutriweather Datalake

A containerized data lake solution built with Apache Airflow and Apache Spark for processing nutritional and weather data. This project demonstrates modern data engineering practices using TheMealDB API as a data source, with plans to integrate weather data for comprehensive food production and agricultural analysis.

## Architecture Overview

The project uses a microservices architecture with the following components:

- **Apache Airflow**: Orchestrates data pipelines and workflow management
- **Apache Spark**: Distributed data processing and transformation
- **Docker Compose**: Container orchestration for development environment
- **Python**: Data processing scripts and pipeline logic

## Data Pipeline

### Current Implementation
1. **Data Extraction**: Fetch meal data from TheMealDB API and weather data from meteorological APIs
2. **Data Transformation**: Clean, enhance, and structure raw JSON/CSV data
3. **Data Storage**: Save processed data in Parquet format for efficient querying

### Planned Features
- Weather data integration from meteorological APIs
- Nutritional analysis correlations with climate patterns
- Agricultural yield predictions based on weather conditions

## Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Docker Compose v2.0+
- Git
- At least 8GB RAM available for containers

## Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd nutriweather-datalake
   ```

2. **Build and start the services:**
   ```bash
   # Start all services (Airflow + Spark cluster)
   astro dev start

   # Or manually with docker-compose (if not using Astronomer CLI)
   docker-compose up -d
   ```

3. **Access the services:**
   - Airflow UI: http://localhost:8080 (admin/admin)
   - Spark Master UI: http://localhost:8081

## Development Workflow

### Available Commands

| Command | Description |
|---------|-------------|
| `astro dev start` | Start the development environment |
| `astro dev stop` | Stop all running containers |
| `astro dev restart` | Restart the development environment |
| `astro dev ps` | Show status of running containers |
| `astro dev logs` | View logs from all services |
| `astro dev bash` | Access Airflow scheduler container shell |

### Alternative Docker Commands (without Astronomer CLI)

| Command | Description |
|---------|-------------|
| `docker-compose up -d` | Start services in detached mode |
| `docker-compose down` | Stop and remove containers |
| `docker-compose logs -f spark-master` | Follow Spark master logs |
| `docker-compose exec spark-master bash` | Access Spark master container |

## Project Structure

```
nutriweather-datalake/
├── dags/                      # Airflow DAG definitions
│   ├── fetch_meals_dag.py     # Data extraction pipeline
│   ├── format_meals_dag.py    # Data transformation pipeline
│   ├── fetch_weather_dag.py   # Weather data extraction
│   └── format_weather_dag.py  # Weather data transformation
├── include/                   # Shared code and data
│   ├── scripts/               # Python processing scripts
│   │   ├── fetch_meals.py     # MealDB API extraction
│   │   ├── format_meals.py    # Data transformation logic
│   │   └── format_weather.py  # Weather data transformation
│   ├── raw/                   # Raw data storage (created at runtime)
│   └── formatted/             # Processed data (created at runtime)
├── apps/                      # Spark applications
├── data/                      # Processed data output (created at runtime)
├── docker-compose.override.yml # Spark cluster configuration
├── Dockerfile                 # Custom Airflow image
├── requirements.txt           # Python dependencies
├── airflow_settings.yaml     # Airflow connections & variables
└── .gitignore                # Git ignore file
```

## Data Pipeline Details

### DAGs (Directed Acyclic Graphs)

#### 1. Fetch Meals DAG (`fetch_meals_dag`)
- **Purpose**: Extract meal data from TheMealDB API
- **Schedule**: Manual trigger
- **Tasks**:
  - `extract_meals_from_api`: Fetches meals by searching A-Z
  - `save_raw_meals`: Stores raw JSON data with timestamps
  - `trigger_formatting`: Automatically triggers transformation pipeline

#### 2. Format Meals DAG (`format_meals_dag`)
- **Purpose**: Transform and enhance raw meal data
- **Schedule**: Triggered by fetch DAG
- **Tasks**:
  - `format_meals`: Uses PySpark to clean and structure data
  - Adds estimated preparation time and temperature
  - Outputs Parquet files for efficient querying

#### 3. Fetch Weather DAG (`fetch_weather_dag`)
- **Purpose**: Extract weather data from meteorological APIs
- **Schedule**: Manual trigger
- **Tasks**:
  - `extract_weather_from_api`: Fetches weather data
  - `save_raw_weather`: Stores raw CSV data with timestamps
  - `trigger_formatting`: Automatically triggers transformation pipeline

#### 4. Format Weather DAG (`format_weather_dag`)
- **Purpose**: Transform and enhance raw weather data
- **Schedule**: Triggered by fetch DAG
- **Tasks**:
  - `format_weather`: Cleans and structures CSV data
  - Handles both CSV (preferred) and JSON (legacy) formats
  - Outputs Parquet files for efficient querying

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