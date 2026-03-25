import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, to_timestamp, explode

def run_transformation(input_path: str, output_path: str):
    """
    Main transformation logic for traffic data:
    - Explodes nested records
    - Flattens flowSegmentData
    - Engineers features: speed_ratio, congestion_level, delay, hour, rush_hour.
    """
    spark = SparkSession.builder.appName("TrafficFeatureEngineering").getOrCreate()
    
    print(f"Reading data from: {input_path}")
    # Read multiline JSON
    df = spark.read.option("multiline", "true").json(input_path)
    
    # 🔥 STEP 1: explode array to get individual location records
    df = df.select("ingestion_time", explode("data").alias("record"))
    
    # 🔥 STEP 2: flatten structure
    df = df.select(
        "ingestion_time",
        "record.latitude",
        "record.longitude",
        "record.flowSegmentData.*"
    )
    
    # FEATURE 1: Speed Ratio
    df = df.withColumn(
        "speed_ratio",
        col("currentSpeed") / col("freeFlowSpeed")
    )
    
    # FEATURE 2: Congestion Level
    df = df.withColumn(
        "congestion_level",
        when(col("speed_ratio") > 0.9, "LOW")
        .when(col("speed_ratio") > 0.6, "MEDIUM")
        .otherwise("HIGH")
    )
    
    # FEATURE 3: Delay
    df = df.withColumn(
        "delay",
        col("currentTravelTime") - col("freeFlowTravelTime")
    )
    
    # FEATURE 4: Extract Hour
    df = df.withColumn(
        "timestamp_converted",
        to_timestamp(col("ingestion_time"))
    )
    
    df = df.withColumn(
        "hour",
        hour(col("timestamp_converted"))
    )
    
    # FEATURE 5: Rush Hour Flag
    df = df.withColumn(
        "rush_hour",
        when((col("hour") >= 8) & (col("hour") <= 10), "YES")
        .when((col("hour") >= 17) & (col("hour") <= 20), "YES")
        .otherwise("NO")
    )
    
    print("Final Data Sample:")
    df.select(
        "currentSpeed",
        "freeFlowSpeed",
        "speed_ratio",
        "congestion_level",
        "hour",
        "roadClosure",
        "delay",
        "longitude",
        "rush_hour"
    ).show(20, truncate=False)
    
    df.printSchema()
    
    print(f"Writing processed data to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    
    print("Transformation successful.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Traffic JSON to Parquet")
    parser.add_argument("--input", type=str, required=True, help="Input S3 bucket URL or local path")
    parser.add_argument("--output", type=str, required=True, help="Output S3 bucket URL or local path")
    
    args = parser.parse_args()
    run_transformation(args.input, args.output)
