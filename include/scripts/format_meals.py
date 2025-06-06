from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_date, explode
from pyspark.sql.types import IntegerType
from datetime import datetime
import random
import os
import glob
import shutil


def estimate_prep_time(meal_name):
    if not meal_name:
        return random.randint(25, 60)
    
    meal_lower = meal_name.lower()
    
    if any(keyword in meal_lower for keyword in ["soup", "stew", "roast", "pot roast", "bake", "casserole", "lasagna", "tagine", "biryani", "rendang"]):
        return random.randint(60, 120)
    elif any(keyword in meal_lower for keyword in ["salad", "sandwich", "omelette", "pancake", "toast"]):
        return random.randint(10, 25)
    elif any(keyword in meal_lower for keyword in ["pie", "cake", "tart", "brownie", "pudding", "cookies", "cheesecake"]):
        return random.randint(30, 90)
    elif any(keyword in meal_lower for keyword in ["grill", "fry", "chops", "cutlet", "sauté"]):
        return random.randint(20, 45)
    else:
        return random.randint(25, 60)


def estimate_temperature(meal_name):
    if not meal_name:
        return random.randint(15, 25)
    
    meal_lower = meal_name.lower()
    
    if any(keyword in meal_lower for keyword in ["soup", "stew", "roast", "pot roast", "hotpot", "chili", "bake", "casserole"]):
        return random.randint(5, 15)
    elif any(keyword in meal_lower for keyword in ["salad", "sushi", "cold", "congee"]):
        return random.randint(20, 35)
    elif any(keyword in meal_lower for keyword in ["cake", "pudding", "dessert", "cookies", "pastries"]):
        return random.randint(10, 25)
    else:
        return random.randint(15, 25)


def main():
    spark = SparkSession.builder \
        .appName("MealDB Data Format") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        print("Reading raw meals data...")
        raw_dir = "/usr/local/airflow/include/raw/meals"
        pattern = os.path.join(raw_dir, "raw_meals_*.json")
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError("No raw meal files found")
        
        latest_file = max(files, key=os.path.getctime)
        print(f"Using file: {latest_file}")
        
        df_raw = spark.read.option("multiline", "true").json(latest_file)
        df = df_raw.select(explode(col("meals")).alias("meal")).select("meal.*")
        
        print(f"Total meals loaded: {df.count()}")
        
        prep_time_udf = udf(estimate_prep_time, IntegerType())
        temperature_udf = udf(estimate_temperature, IntegerType())
        
        formatted_df = df.select(
            col("idMeal").alias("meal_id"),
            col("strMeal").alias("meal_name"),
            col("strCategory").alias("category"),
            col("strArea").alias("region"),
            col("strInstructions").alias("instructions"),
            col("strMealThumb").alias("image_url"),
            col("strYoutube").alias("youtube_url"),
            col("strSource").alias("source_url")
        ).withColumn(
            "preparation_time", prep_time_udf(col("meal_name"))
        ).withColumn(
            "temperature", temperature_udf(col("meal_name"))
        ).withColumn(
            "formatted_date", current_date()
        ).filter(col("meal_name").isNotNull())
        
        print("Sample of formatted data:")
        formatted_df.select("meal_name", "region", "preparation_time", "temperature").show(10, truncate=False)
        
        print("Saving formatted meals as Parquet...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        os.makedirs("/usr/local/airflow/include/formatted/meals", exist_ok=True)
        
        # Write to temporary directory first
        temp_output = f"/tmp/formatted_meals_{timestamp}_temp"
        formatted_df.coalesce(1).write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(temp_output)
        
        # Find the actual parquet file and move it
        parquet_files = [f for f in os.listdir(temp_output) if f.endswith('.parquet')]
        if not parquet_files:
            raise RuntimeError("No parquet file found in temp directory")
        
        temp_parquet_path = os.path.join(temp_output, parquet_files[0])
        final_path = f"/usr/local/airflow/include/formatted/meals/formatted_meals_{timestamp}.parquet"
        shutil.move(temp_parquet_path, final_path)
        
        # Clean up temp directory
        shutil.rmtree(temp_output)
        
        print(f"Successfully saved formatted meals to: {final_path}")
        
        total_meals = formatted_df.count()
        print(f"Formatting complete! Processed {total_meals} meals")
        
    except Exception as e:
        print(f"Error formatting meals: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
