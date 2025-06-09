"""
Simplified Meals Data Formatting Module
"""

import glob
import os
from datetime import datetime
from spark_utils import get_spark_session, ensure_directory, add_metadata_columns, save_parquet_clean
from pyspark.sql.functions import col, explode, udf, array, when, concat_ws, array_join
from pyspark.sql.types import IntegerType, StringType


def estimate_prep_time(meal_name):
    """Simple preparation time estimation."""
    if not meal_name:
        return 30
    
    meal_lower = meal_name.lower()
    base_time = len(meal_name) % 20 + 20
    
    if any(word in meal_lower for word in ["soup", "stew", "roast", "casserole"]):
        return min(120, base_time + 60)
    elif any(word in meal_lower for word in ["salad", "sandwich", "pancake"]):
        return max(10, base_time - 10)
    else:
        return base_time + 15


def clean_instructions(instructions):
    """Clean and format cooking instructions."""
    if not instructions:
        return ""
    
    # Simple cleaning - split on newlines and rejoin with semicolons
    steps = [step.strip() for step in instructions.replace('\r\n', '\n').split('\n') if step.strip()]
    return "; ".join(steps[:10])  # Limit to 10 steps


def main():
    spark = get_spark_session("FormatMeals")
    
    try:
        # Find latest raw meals file
        raw_dir = "/usr/local/airflow/include/raw/meals"
        pattern = os.path.join(raw_dir, "raw_meals_*.json")
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError("No raw meal files found")
        
        latest_file = max(files, key=os.path.getctime)
        print(f"Processing: {latest_file}")
        
        # Read and explode meals
        df_raw = spark.read.option("multiline", "true").json(latest_file)
        df = df_raw.select(explode(col("meals")).alias("meal")).select("meal.*")
        
        print(f"Processing {df.count()} meals")
        
        # Create UDFs
        prep_time_udf = udf(estimate_prep_time, IntegerType())
        instructions_udf = udf(clean_instructions, StringType())
        
        # Build ingredients array (simplified)
        ingredient_cols = []
        for i in range(1, 21):
            ingredient_cols.append(
                when(
                    col(f"strIngredient{i}").isNotNull() & 
                    (col(f"strIngredient{i}") != ""),
                    concat_ws(": ", col(f"strIngredient{i}"), col(f"strMeasure{i}"))
                )
            )
        
        # Create formatted DataFrame
        formatted_df = df.select(
            col("idMeal").alias("meal_id"),
            col("strMeal").alias("meal_name"),
            col("strCategory").alias("category"),
            col("strArea").alias("region"),
            array_join(array(*ingredient_cols), "; ").alias("ingredients"),
            instructions_udf(col("strInstructions")).alias("instructions"),
            prep_time_udf(col("strMeal")).alias("preparation_time"),
            col("strMealThumb").alias("image_url")
        ).filter(col("meal_name").isNotNull())
        
        # Add metadata
        final_df = add_metadata_columns(formatted_df)
        
        # Save as clean Parquet file
        output_dir = "/usr/local/airflow/include/formatted/meals"
        ensure_directory(output_dir)
        
        output_path = save_parquet_clean(final_df, output_dir, "formatted_meals")
        
        print(f"Formatted meals saved to: {output_path}")
        print(f"Total meals formatted: {final_df.count()}")
        
    except Exception as e:
        print(f"Error formatting meals: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
