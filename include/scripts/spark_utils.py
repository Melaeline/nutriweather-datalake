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


def get_hdfs_client():
    """Get HDFS client connection with improved error handling."""
    try:
        from hdfs import InsecureClient
        # Try multiple connection methods
        namenode_urls = [
            'http://namenode:9870',  # Docker internal
            'http://localhost:9870',  # Local development
            'http://127.0.0.1:9870'   # Fallback
        ]
        
        for url in namenode_urls:
            try:
                client = InsecureClient(url, user='root')
                # Test connection by listing root directory
                client.list('/')
                print(f"✓ Connected to HDFS at {url}")
                return client
            except Exception as e:
                print(f"Failed to connect to HDFS at {url}: {e}")
                continue
        
        raise Exception("Could not connect to any HDFS NameNode")
        
    except ImportError as e:
        print(f"Warning: hdfs library not available - {e}")
        print("Install with: pip install hdfs==2.7.0")
        return None
    except Exception as e:
        print(f"Warning: Could not connect to HDFS: {e}")
        return None


def backup_to_hdfs(local_file_path: str, hdfs_directory: str, client=None):
    """Backup a local file to HDFS with better error handling."""
    if client is None:
        client = get_hdfs_client()
    
    if client is None:
        print("HDFS client not available, skipping backup")
        return False
    
    if not os.path.exists(local_file_path):
        print(f"Local file does not exist: {local_file_path}")
        return False
    
    try:
        # Ensure HDFS directory exists
        client.makedirs(hdfs_directory)
        
        # Extract filename from local path
        filename = os.path.basename(local_file_path)
        hdfs_path = f"{hdfs_directory}/{filename}"
        
        # Upload file to HDFS
        with open(local_file_path, 'rb') as local_file:
            client.write(hdfs_path, local_file, overwrite=True)
        
        print(f"✓ Backed up to HDFS: {hdfs_path}")
        return True
        
    except Exception as e:
        print(f"Error backing up to HDFS: {e}")
        return False


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
        
        # Backup to HDFS
        hdfs_dir = get_hdfs_backup_path(output_dir)
        backup_to_hdfs(final_path, hdfs_dir)
        
        return final_path
        
    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise e


def get_hdfs_backup_path(local_path: str) -> str:
    """Convert local path to corresponding HDFS backup path."""
    # Convert local include paths to HDFS paths
    if "/raw/" in local_path:
        if "/meals/" in local_path:
            return "/nutriweather/raw/meals"
        elif "/weather/" in local_path:
            return "/nutriweather/raw/weather"
    elif "/formatted/" in local_path:
        if "/meals/" in local_path:
            return "/nutriweather/formatted/meals"
        elif "/weather/" in local_path:
            return "/nutriweather/formatted/weather"
    elif "/usage/" in local_path:
        return "/nutriweather/usage"
    
    # Default fallback
    return "/nutriweather/backup"


def save_with_hdfs_backup(file_path: str, data, file_format: str = "json"):
    """Save file locally and backup to HDFS."""
    # Ensure local directory exists
    ensure_directory(os.path.dirname(file_path))
    
    # Save locally
    if file_format == "json":
        import json
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    print(f"Local file saved: {file_path}")
    
    # Backup to HDFS
    hdfs_dir = get_hdfs_backup_path(file_path)
    backup_to_hdfs(file_path, hdfs_dir)
    
    return file_path


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
