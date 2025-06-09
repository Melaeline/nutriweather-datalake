"""
Spark Utilities Module - Common patterns and configurations for all scripts.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_date
import os
from datetime import datetime


def get_spark_session(app_name: str) -> SparkSession:
    """Get a standardized Spark session with common configurations."""
    return SparkSession.builder \
        .appName(f"NutriWeather-{app_name}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def save_as_single_file(df, output_path: str, format_type: str = "json"):
    """Save DataFrame as a single file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format_type == "parquet":
        final_path = f"{output_path}/data_{timestamp}.parquet"
        df.coalesce(1).write.mode("overwrite").parquet(final_path)
    else:
        final_path = f"{output_path}/data_{timestamp}.json"
        df.coalesce(1).write.mode("overwrite").json(final_path)
    
    return final_path


def ensure_directory(path: str):
    """Ensure directory exists."""
    os.makedirs(path, exist_ok=True)


def add_metadata_columns(df):
    """Add standard metadata columns to DataFrame."""
    return df.withColumn("processed_date", current_date()) \
             .withColumn("processed_timestamp", lit(datetime.now().isoformat()))


def clean_string_column(df, column_name: str):
    """Clean and standardize string columns."""
    return df.withColumn(
        column_name,
        when(col(column_name).isNull() | (col(column_name) == ""), None)
        .otherwise(col(column_name))
    )
