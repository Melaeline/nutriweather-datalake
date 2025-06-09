"""
Spark Utilities Module - Common patterns and configurations for all scripts.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_date
import os
import shutil
import glob
from datetime import datetime


def get_spark_session(app_name: str) -> SparkSession:
    """Get a standardized Spark session with common configurations."""
    return SparkSession.builder \
        .appName(f"NutriWeather-{app_name}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def save_parquet_clean(df, output_dir: str, filename: str):
    """Save DataFrame as a single clean parquet file without Spark artifacts."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_filename = f"{filename}_{timestamp}.parquet"
    
    # Create temporary directory for Spark output
    temp_dir = f"{output_dir}/temp_{timestamp}"
    ensure_directory(temp_dir)
    
    try:
        # Write to temporary directory
        df.coalesce(1).write.mode("overwrite").parquet(temp_dir)
        
        # Find the actual parquet file (excluding _SUCCESS and other artifacts)
        parquet_files = glob.glob(f"{temp_dir}/*.parquet")
        if not parquet_files:
            raise FileNotFoundError("No parquet file found in Spark output")
        
        # Move the parquet file to final location
        source_file = parquet_files[0]  # Should be only one due to coalesce(1)
        final_path = f"{output_dir}/{final_filename}"
        shutil.move(source_file, final_path)
        
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
        
        print(f"Clean parquet file saved: {final_path}")
        return final_path
        
    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise e


def save_as_single_file(df, output_path: str, format_type: str = "json"):
    """Save DataFrame as a single file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format_type == "parquet":
        # Use the new clean parquet saving method
        directory = os.path.dirname(output_path)
        filename = os.path.basename(output_path).replace('.parquet', '')
        return save_parquet_clean(df, directory, filename)
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
