from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, udf, current_date, explode, array, struct, when, lit, 
    collect_list, create_map, arrays_zip, transform, concat_ws, array_join
)
from pyspark.sql.types import IntegerType, ArrayType, StringType, StructType, StructField
from datetime import datetime
import random
import os
import glob
import shutil


def split_instructions(instructions):
    """Split instructions by newlines and clean up"""
    if not instructions:
        return ""
    
    # Split on various newline patterns and clean up
    steps = instructions.replace('\r\n\r\n', '\n').replace('\r\n', '\n').split('\n')
    
    # Clean up steps and filter out empty ones
    cleaned_steps = []
    for step in steps:
        step = step.strip()
        if step:
            # Split steps further if they contain multiple sentences
            parts = []
            current = ""
            for char in step:
                current += char
                if char == '.' and not (current.strip()[-3:].replace('.', '').isdigit()):
                    cleaned = current.strip()
                    if cleaned:
                        parts.append(cleaned)
                    current = ""
            
            if current.strip():
                parts.append(current.strip())
            
            # Add non-empty parts to cleaned steps
            cleaned_steps.extend([p for p in parts if p])
    
    return ";".join(cleaned_steps)


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
        
        # Read and explode meals array
        df_raw = spark.read.option("multiline", "true").json(latest_file)
        df = df_raw.select(explode(col("meals")).alias("meal")).select("meal.*")
        print(f"Total meals loaded: {df.count()}")
        
        # Define UDFs with proper return types
        prep_time_udf = udf(estimate_prep_time, IntegerType())
        temperature_udf = udf(estimate_temperature, IntegerType())
        instructions_udf = udf(split_instructions, StringType())
        
        # First, create separate arrays for ingredients and measures
        ingredient_arrays = []
        measure_arrays = []
        for i in range(1, 21):
            ingredient_arrays.append(
                when(
                    (col(f"strIngredient{i}").isNotNull()) & 
                    (col(f"strIngredient{i}") != "") & 
                    (col(f"strIngredient{i}") != " "), 
                    concat_ws(": ", col(f"strIngredient{i}"), col(f"strMeasure{i}"))
                )
            )
        
        # Create the initial dataframe with base columns
        formatted_df = df.select(
            col("idMeal").alias("meal_id"),
            col("strMeal").alias("meal_name"),
            col("strCategory").alias("category"),
            col("strArea").alias("region"),
            col("strInstructions").alias("raw_instructions"),
            col("strMealThumb").alias("image_url"),
            col("strYoutube").alias("youtube_url"),
            col("strSource").alias("source_url"),
            col("strTags").alias("tags"),
            # Create string of non-null ingredients
            array(*ingredient_arrays).alias("ingredients_array")
        )
        
        # Create final DataFrame with all required columns
        final_df = formatted_df.select(
            "meal_id",
            "meal_name", 
            "category",
            "region",
            array_join("ingredients_array", ";").alias("ingredients"),
            instructions_udf(col("raw_instructions")).alias("instructions"),
            prep_time_udf(col("meal_name")).alias("preparation_time"),
            temperature_udf(col("meal_name")).alias("temperature"),
            "image_url",
            "youtube_url", 
            "source_url",
            "tags"
        ).withColumn(
            "formatted_date", current_date()
        )
        
        # Show sample with proper formatting
        print("\nSample of formatted data:")
        final_df.select(
            "meal_name",
            "ingredients",
            "instructions"
        ).show(2, truncate=False)
        
        print("\nSaving formatted meals as Parquet...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "/usr/local/airflow/include/formatted/meals"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to a temporary directory first
        temp_dir = f"/tmp/formatted_meals_{timestamp}_temp"
        final_path = f"{output_dir}/formatted_meals_{timestamp}.parquet"
        
        # Write as a single file using coalesce(1)
        final_df.coalesce(1).write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(temp_dir)
        
        # Find the parquet file in temp directory
        parquet_files = [f for f in os.listdir(temp_dir) if f.endswith('.parquet')]
        if not parquet_files:
            raise RuntimeError("No parquet file found in temp directory")
        
        # Move the single parquet file to final location
        temp_parquet_path = os.path.join(temp_dir, parquet_files[0])
        shutil.move(temp_parquet_path, final_path)
        
        # Clean up temp directory
        shutil.rmtree(temp_dir)
        
        print(f"Successfully saved formatted meals to: {final_path}")
        total_meals = final_df.count()
        print(f"Formatting complete! Processed {total_meals} meals")
        
    except Exception as e:
        print(f"Error formatting meals: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
