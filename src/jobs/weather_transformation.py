import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, to_timestamp, round

# Initialize Spark
spark = SparkSession.builder.appName("WeatherFeatureEngineering").getOrCreate()

input_path = "s3://traffic-data-project-manish/raw/2026/03/23/weather_20260326_011506.json"
output_path = "s3://traffic-data-project-manish/processed/"

print(f"Reading data from: {input_path}")

# Read multiline JSON
df = spark.read.option("multiline", "true").json(input_path)

# 🔥 STEP 1: Flatten structure
df = df.select(
    "ingestion_time",
    "name",
    "coord.lat",
    "coord.lon",
    "main.temp",
    "main.feels_like",
    "main.temp_min",
    "main.temp_max",
    "main.pressure",
    "main.humidity",
    "wind.speed",
    "wind.deg",
    "clouds.all",
    "sys.country",
    "weather",
    "visibility"
)

# 🔥 STEP 2: Feature Engineering

# FEATURE 1: Convert Kelvin to Celsius
df = df.withColumn("temp_c", round(col("temp") - 273.15, 2))
df = df.withColumn("feels_like_c", round(col("feels_like") - 273.15, 2))
df = df.withColumn("temp_min_c", round(col("temp_min") - 273.15, 2))
df = df.withColumn("temp_max_c", round(col("temp_max") - 273.15, 2))

# FEATURE 2: Extract Hour from ingestion_time
df = df.withColumn("timestamp_converted", to_timestamp(col("ingestion_time")))
df = df.withColumn("hour", hour(col("timestamp_converted")))

# FEATURE 3: Extreme Weather Flag
df = df.withColumn(
    "is_extreme_weather",
    when(
        (col("temp_c") > 40) |
        (col("temp_c") < 0) |
        (col("speed") > 15) |
        (col("visibility") < 1000),
        "YES"
    ).otherwise("NO")
)

# FEATURE 4: Extract weather condition from array
df = df.withColumn("weather_main", col("weather").getItem(0).getField("main"))
df = df.withColumn("weather_description", col("weather").getItem(0).getField("description"))

print("Final Data Sample:")
df.select(
    "name",
    "temp_c",
    "feels_like_c",
    "weather_main",
    "weather_description",
    "speed",
    "visibility",
    "is_extreme_weather",
    "hour"
).show(20, truncate=False)

df.printSchema()

# print(f"Writing processed data to: {output_path}")
# df.write.mode("overwrite").parquet(output_path)

print("Transformation successful.")