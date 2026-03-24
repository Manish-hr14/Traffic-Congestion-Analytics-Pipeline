import sys
import argparse
from pyspark.sql import functions as F
from src.utils.spark_utils import get_spark_session
from src.schemas.traffic_schema import TRAFFIC_JSON_SCHEMA

def run_transformation(input_path: str, output_path: str):
    """
    Main transformation logic for traffic data from S3 JSON to S3 Parquet.
    Includes flattening nested structures.
    """
    spark = get_spark_session("TrafficTransformationJob")
    
    # 1. Read JSON with pre-defined schema (multiline = true)
    print(f"Reading data from: {input_path}")
    df = spark.read.option("multiline", "true").schema(TRAFFIC_JSON_SCHEMA).json(input_path)
    
    # 2. Extract flowSegmentData fields
    # This promotes nested fields under flowSegmentData to the top level
    df_flattened = df.select("flowSegmentData.*")
    
    # 3. Handle nested coordinates (Exploding into multiple rows)
    # Each coordinate point (lat/long) becomes a row for that segment
    df_exploded = df_flattened.withColumn("coord_point", F.explode("coordinates.coordinate"))
    
    # 4. Final output selection
    # Dropping the coordinates struct and point struct to keep only flattened fields
    final_df = df_exploded.select(
        "frc",
        "currentSpeed",
        "freeFlowSpeed",
        "currentTravelTime",
        "freeFlowTravelTime",
        "confidence",
        "roadClosure",
        F.col("coord_point.latitude").alias("latitude"),
        F.col("coord_point.longitude").alias("longitude")
    )
    
    # 5. Display sample for verification
    print("Schema after flattening:")
    final_df.printSchema()
    final_df.show(10)
    
    # 6. Write to Parquet (Overwrite mode)
    print(f"Writing processed data to: {output_path}")
    final_df.write.mode("overwrite").parquet(output_path)
    
    print("Transformation successful.")

if __name__ == "__main__":
    # Standard argument parsing for extensibility
    parser = argparse.ArgumentParser(description="Process Traffic JSON to Parquet")
    parser.add_argument("--input", type=str, required=True, help="Input S3 bucket URL")
    parser.add_argument("--output", type=str, required=True, help="Output S3 bucket URL")
    
    args = parser.parse_args()
    
    run_transformation(args.input, args.output)
