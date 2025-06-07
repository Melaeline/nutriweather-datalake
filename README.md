# NutriWeather Data Lake

A comprehensive data processing pipeline that combines nutritional meal data with weather information to enable cross-domain analytics for agricultural and dietary insights.

## 🏗️ Architecture Overview

The NutriWeather Data Lake consists of three main processing stages:

1. **Data Ingestion & Formatting** - Raw data collection and standardization
2. **Data Merging** - Combining meal and weather datasets
3. **Analytics & Insights** - Cross-domain analysis capabilities

```
Raw Data Sources
├── TheMealDB API (Meal Data)
└── Open-Meteo API (Weather Data)
       ↓
Formatting Layer
├── format_meals_dag.py
└── format_weather_dag.py
       ↓
Merge Layer
└── merge_formatted_dag.py
       ↓
Analytics Layer
└── Enhanced meal-weather datasets
```

## 📁 Project Structure

```
nutriweather-datalake/
├── dags/                           # Airflow DAG definitions
│   ├── format_meals_dag.py        # Meal data formatting pipeline
│   ├── format_weather_dag.py      # Weather data formatting pipeline
│   └── merge_formatted_dag.py     # Data merging pipeline
├── include/
│   ├── scripts/                   # Processing scripts
│   │   ├── format_meals.py       # Meal data formatting logic
│   │   ├── format_weather.py     # Weather data formatting logic
│   │   └── merge_formatted.py    # Data merging logic
│   ├── raw/                      # Raw data storage
│   │   ├── meals/                # Raw meal JSON files
│   │   └── weather/              # Raw weather JSON files
│   ├── formatted/                # Processed data storage
│   │   ├── meals/                # Formatted meal Parquet files
│   │   ├── weather/              # Formatted weather JSON files
│   │   └── merged/               # Merged analytical datasets
│   └── logs/                     # Processing logs
└── README.md                     # This file
```

## 🚀 Getting Started

### Prerequisites

- Apache Airflow 2.x
- Python 3.8+
- Apache Spark (for data processing)
- Required Python packages:
  - `pyspark`
  - `pandas`
  - `requests`

### Installation

1. Clone the repository
2. Install dependencies
3. Configure Airflow to point to the `dags/` directory
4. Start the Airflow scheduler and webserver

### Running the Pipeline

The data processing pipeline consists of three sequential DAGs:

1. **Format Meals DAG** (`format_meals_dag`)
   - Fetches meal data from TheMealDB API
   - Standardizes and enriches meal information
   - Outputs: Parquet files in `include/formatted/meals/`

2. **Format Weather DAG** (`format_weather_dag`)
   - Fetches weather data from Open-Meteo API
   - Processes current and hourly weather information
   - Outputs: JSON files in `include/formatted/weather/`

3. **Merge Formatted DAG** (`merge_formatted_dag`)
   - Combines meal and weather datasets
   - Creates enhanced analytical datasets with cross-domain insights
   - Outputs: JSON files in `include/formatted/merged/`

## 📊 Data Schema

### Meal Data Fields
- `meal_id`: Unique identifier
- `meal_name`: Recipe name
- `category`: Food category (e.g., Dessert, Chicken)
- `region`: Cuisine origin
- `ingredients`: Ingredient list with quantities
- `instructions`: Cooking steps
- `preparation_time`: Estimated cooking time
- `temperature`: Recommended serving temperature
- `tags`: Recipe tags

### Weather Data Fields
- `location_name`: Geographic location
- `current_temperature`: Real-time temperature
- `humidity`: Current humidity percentage
- `wind_speed`: Wind speed measurement
- `hourly_weather`: 24-hour temperature history

### Merged Data Features
- **Temperature Matching**: Compares meal serving temperature with current weather
- **Weather Recommendations**: Suggests meal types based on weather conditions
- **Seasonal Analysis**: Enables correlation between weather patterns and food preferences

## 🔧 Configuration

### Environment Variables
- `AIRFLOW_HOME`: Airflow installation directory
- `SPARK_HOME`: Spark installation directory (if using local Spark)

### API Endpoints
- **TheMealDB**: `https://www.themealdb.com/api/json/v1/1/`
- **Open-Meteo**: `https://api.open-meteo.com/v1/forecast`

## 📈 Analytics Use Cases

1. **Seasonal Food Trends**: Analyze correlation between weather patterns and meal preferences
2. **Regional Cuisine Analysis**: Compare food choices across different climates
3. **Temperature-Based Recommendations**: Suggest appropriate meals based on current weather
4. **Agricultural Insights**: Correlate weather conditions with ingredient availability

## 🔍 Monitoring & Logging

- All processing steps are logged with timestamps
- Failed runs include detailed error information
- Data quality metrics are tracked throughout the pipeline
- Airflow UI provides visual monitoring of DAG runs

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📝 License

This project is open source and available under the MIT License.

## 🔗 External Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [TheMealDB API](https://www.themealdb.com/api.php)
- [Open-Meteo API](https://open-meteo.com/en/docs)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)