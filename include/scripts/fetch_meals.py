from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, explode
from datetime import datetime
import os
import shutil


def setup_output_directories():
    """Setup output directories"""
    output_dirs = [
        "/usr/local/airflow/include/raw/meals"
    ]
    
    for dir_path in output_dirs:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path, exist_ok=True)
        print(f"Created directory: {dir_path}")


def main():
    setup_output_directories()
    
    spark = SparkSession.builder \
        .appName("MealDB Data Fetch") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Read the raw meals JSON file
        print("Reading raw meals data...")
        raw_meals_path = "/usr/local/airflow/include/raw/meals/raw_meals.json"
        
        # Read JSON file and extract meals array
        df_raw = spark.read.option("multiline", "true").json(raw_meals_path)
        df = df_raw.select(explode(col("meals")).alias("meal")).select("meal.*")
        
        print(f"Total meals loaded: {df.count()}")
        
        # Process and transform the data
        processed_df = df.select(
            col("idMeal").alias("meal_id"),
            col("strMeal").alias("meal_name"),
            col("strCategory").alias("category"),
            col("strArea").alias("region"),
            col("strInstructions").alias("instructions"),
            col("strMealThumb").alias("image_url"),
            col("strYoutube").alias("youtube_url"),
            col("strSource").alias("source_url")
        ).withColumn(
            "processed_date", current_date()
        ).filter(col("meal_name").isNotNull())
        
        print("Sample of processed data:")
        processed_df.select("meal_name", "region", "category").show(10, truncate=False)
        
        # Save processed meals with timestamp
        print("Saving processed meals...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"processed_meals_{timestamp}.json"
        output_path = f"/usr/local/airflow/include/raw/meals/{filename}"
        
        processed_df.coalesce(1).write \
            .mode("overwrite") \
            .json(output_path)
        
        print(f"Successfully saved processed meals to: {output_path}")
        
        total_meals = processed_df.count()
        print(f"Processing complete! Processed {total_meals} meals")
        
    except Exception as e:
        print(f"Error processing meals: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
