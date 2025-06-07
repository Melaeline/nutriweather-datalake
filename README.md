# NutriWeather Data Lake

A robust data lake and analytics pipeline that integrates nutritional meal data with weather information to enable cross-domain analytics for agricultural, dietary, and environmental insights.

---

## 🚀 Overview

NutriWeather Data Lake is a modular, production-ready data pipeline built on **Apache Airflow** and **Apache Spark**. It ingests, processes, and merges data from public APIs (TheMealDB and Open-Meteo), producing analytical datasets for downstream applications.

---

## 🏗️ Architecture

### High-Level Pipeline

```
Raw Data Sources
├── TheMealDB API (Meal Data)
└── Open-Meteo API (Weather Data)
       ↓
Airflow DAGs (Orchestration)
├── fetch_meals.py
├── fetch_weather.py
├── format_meals.py
├── format_weather.py
└── merge_formatted.py
       ↓
Data Lake Storage (Local FS or Volume)
├── include/raw/         # Raw API data
├── include/formatted/   # Cleaned/structured data
└── include/usage/       # Analytical/merged outputs
```

### Data Flow

1. **Ingestion**: Fetches meal and weather data from APIs.
2. **Formatting**: Cleans, enriches, and structures data using PySpark.
3. **Merging**: Combines meal and weather data for analytics.
4. **Analytics**: Enables cross-domain queries and insights.

---

## 🧰 Technologies & Tools

| Technology         | Role/Usage                                                                                   |
|--------------------|---------------------------------------------------------------------------------------------|
| **Apache Airflow** | Workflow orchestration, DAG scheduling, monitoring, and dependency management               |
| **Apache Spark**   | Distributed data processing (ETL, transformation, Parquet/JSON handling)                    |
| **Python 3.8+**    | Core programming language for all scripts and orchestration                                 |
| **Pandas**         | Data manipulation for smaller datasets (e.g., weather formatting)                           |
| **Requests**       | HTTP client for API integration (TheMealDB, Open-Meteo, Nominatim)                          |
| **openmeteo-requests** | Specialized client for Open-Meteo API                                                   |
| **requests-cache** | Caching for API calls (weather geocoding)                                                   |
| **retry-requests** | Robustness for API calls (automatic retries)                                                |
| **Parquet**        | Efficient columnar storage for formatted meal data                                          |
| **JSON**           | Standard format for weather and merged outputs                                              |
| **Elasticsearch**  | (Optional) For indexing and search (see Airflow 3 upgrade notes)                           |

---

## 📁 Project Structure

```
nutriweather-datalake/
├── dags/                           # Airflow DAG definitions (Python)
│   ├── fetch_meals_dag.py
│   ├── fetch_weather_dag.py
│   ├── format_meals_dag.py
│   ├── format_weather_dag.py
│   └── merge_formatted_dag.py
├── include/
│   ├── scripts/                    # Data processing scripts (Python)
│   │   ├── fetch_meals.py
│   │   ├── fetch_weather.py
│   │   ├── format_meals.py
│   │   ├── format_weather.py
│   │   └── merge_formatted.py
│   ├── raw/                        # Raw data storage (JSON)
│   │   ├── meals/
│   │   └── weather/
│   ├── formatted/                  # Processed data storage
│   │   ├── meals/                  # Parquet files
│   │   ├── weather/                # JSON files
│   │   └── merged/                 # (Optional) Merged datasets
│   ├── usage/                      # Final merged analytics outputs (JSON)
│   ├── logs/                       # Processing logs
│   └── advice_dataset.csv          # Meal/weather advice mapping
├── requirements.txt                # Python dependencies
├── README.md                       # Project documentation
└── AIRFLOW_3_UPGRADE_NOTES.md      # Upgrade notes for Airflow 3.x
```

---

## ⚙️ Configuration & Setup

### Prerequisites

- **Python**: 3.8 or newer
- **Apache Airflow**: 2.x or 3.x (see upgrade notes)
- **Apache Spark**: 3.x (local or cluster)
- **pip**: For Python package management

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/nutriweather-datalake.git
   cd nutriweather-datalake
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Airflow**
   - Set `AIRFLOW_HOME` to the project directory or desired location.
   - Point Airflow to the `dags/` directory:
     ```bash
     export AIRFLOW_HOME=$(pwd)
     export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
     ```

4. **Configure Spark**
   - Set `SPARK_HOME` if running Spark locally.
   - Ensure `pyspark` is installed and available.

5. **Start Airflow**
   ```bash
   airflow db init
   airflow scheduler &
   airflow webserver &
   ```

---

## 🔗 API Integrations

- **TheMealDB**: `https://www.themealdb.com/api/json/v1/1/`
- **Open-Meteo**: `https://api.open-meteo.com/v1/forecast`
- **Nominatim (OpenStreetMap)**: For reverse geocoding weather locations

---

## 🛠️ Airflow DAGs & Scripts

| DAG Name              | Script                      | Description                                               |
|-----------------------|----------------------------|-----------------------------------------------------------|
| `fetch_meals_dag`     | `fetch_meals.py`           | Fetches meal data from TheMealDB API                      |
| `fetch_weather_dag`   | `fetch_weather.py`         | Fetches weather data from Open-Meteo API                  |
| `format_meals_dag`    | `format_meals.py`          | Formats and enriches meal data, outputs Parquet           |
| `format_weather_dag`  | `format_weather.py`        | Formats weather data, outputs JSON                        |
| `merge_formatted_dag` | `merge_formatted.py`       | Merges meal and weather data for analytics                |

- **All DAGs** are orchestrated via Airflow and can be triggered manually or scheduled.
- **Scripts** are invoked by Airflow's `BashOperator` and run in the Airflow environment.

---

## 🗄️ Data Storage

- **Raw Data**: `include/raw/`
  - Meals: JSON files from TheMealDB
  - Weather: JSON files from Open-Meteo
- **Formatted Data**: `include/formatted/`
  - Meals: Parquet files (columnar, efficient for analytics)
  - Weather: JSON files (structured, time-series)
- **Merged/Usage Data**: `include/usage/`
  - Final merged recommendations and analytics datasets (JSON)

---

## 🧑‍💻 Technologies in Detail

### Apache Airflow

- **Purpose**: Orchestrates all ETL steps as DAGs.
- **Configuration**: DAGs defined in `dags/`, scripts in `include/scripts/`.
- **Scheduling**: Manual or scheduled via Airflow UI/CLI.
- **Monitoring**: Airflow UI provides DAG run status, logs, and task details.

### Apache Spark

- **Purpose**: High-performance ETL, data transformation, Parquet/JSON handling.
- **Usage**: All heavy data processing (formatting, merging) is done via PySpark scripts.
- **Configuration**: Spark session is created in each script; can be run locally or on a cluster.

### Python & Libraries

- **pandas**: Used for lightweight data manipulation (weather formatting).
- **requests/openmeteo-requests**: API calls for data ingestion.
- **pyarrow**: Parquet file handling.
- **numpy**: Numeric operations.
- **requests-cache/retry-requests**: Robust, cached API calls.

### Data Formats

- **JSON**: Used for raw and merged data, and for weather data.
- **Parquet**: Used for formatted meal data (efficient for analytics).

---

## 📊 Data Schema

### Meal Data (Parquet)

- `meal_id`: Unique identifier
- `meal_name`: Recipe name
- `category`: Food category
- `region`: Cuisine origin
- `ingredients`: List of ingredients with quantities
- `instructions`: Cooking steps (cleaned, structured)
- `preparation_time`: Estimated cooking time (minutes)
- `temperature`: Recommended serving temperature (°C)
- `tags`: Recipe tags
- `image_url`, `youtube_url`, `source_url`: Media links

### Weather Data (JSON)

- `location`: Name, coordinates, elevation, timezone
- `current`: Temperature, humidity, wind speed, timestamp
- `hourly`: List of hourly temperature records (24h)
- `daily`: List of daily UV index records

### Merged Data (JSON)

- `weather_location`: Weather context
- `current_weather`: Snapshot at merge time
- `hourly_weather_data`: 24-hour temperature history
- `daily_weather_summary`: UV index summary
- `recommended_meal`: Best-matched meal for current weather
- `recommended_advice`: Contextual advice based on temperature
- `merge_metadata`: Timestamps, counts, method

---

## 📝 How to Run the Pipeline

1. **Trigger DAGs in Order** (via Airflow UI or CLI):
   - `fetch_meals_dag`
   - `fetch_weather_dag`
   - `format_meals_dag`
   - `format_weather_dag`
   - `merge_formatted_dag`

2. **Outputs**:
   - Formatted meals: `include/formatted/meals/`
   - Formatted weather: `include/formatted/weather/`
   - Merged analytics: `include/usage/`

---

## 🧪 Testing & Monitoring

- **Airflow UI**: Monitor DAG runs, task logs, and failures.
- **Logs**: All scripts log to stdout and `include/logs/`.
- **Data Quality**: Scripts include basic validation and error handling.
- **Manual Testing**: Run scripts directly for debugging.

---

## 🔒 Security & Best Practices

- **API Keys**: Not required for public APIs used.
- **Isolation**: All processing is local to the Airflow environment.
- **Error Handling**: Robust exception handling in all scripts.

---

## 🌠 Astro CLI for Airflow Containers & Configuration

[Astro CLI](https://docs.astronomer.io/astro/cli/overview) is a tool for creating, managing, and deploying Airflow environments using containers. It is **not** a frontend or dashboarding framework. Instead, Astro CLI helps you:

- Initialize and configure Airflow projects
- Build and run Airflow containers locally or in the cloud
- Manage Airflow dependencies and environment variables
- Simplify Airflow deployment and orchestration

**How to use Astro CLI with NutriWeather Data Lake:**
1. Install the Astro CLI (`pip install astro-cli` or see Astro docs).
2. Use `astro dev init` to scaffold an Airflow project.
3. Place your DAGs and scripts in the appropriate directories.
4. Use `astro dev start` to launch Airflow in containers for local development.
5. Configure Airflow environment variables and connections as needed.

> **Note:** Astro CLI is for Airflow infrastructure management. For frontend/dashboarding, use a separate framework (e.g., Astro web framework, React, etc.) if desired.

Astro CLI is optional but recommended for teams seeking robust, containerized Airflow development and deployment.

---

## 🔗 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [TheMealDB API](https://www.themealdb.com/api.php)
- [Open-Meteo API](https://open-meteo.com/en/docs)
- [OpenStreetMap Nominatim](https://nominatim.org/release-docs/latest/api/Reverse/)

---