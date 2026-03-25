from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# Create a Spark session
spark = SparkSession.builder \
    .appName("RideSharingWindowAnalytics") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_stream = raw_stream \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp column to TimestampType and add a watermark
# Spark requires a watermark to manage state in windowed aggregations
stream_with_time = parsed_stream \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_aggs = stream_with_time \
    .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(_sum("fare_amount").alias("total_fare_in_window"))

# Extract window start and end times as separate columns (better format for CSV)
final_output = windowed_aggs \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare_in_window")
    ) \
    .orderBy("window_start")

output_dir = "outputs/task_3"
os.makedirs(output_dir, exist_ok=True)

# Define a function to write each batch to a CSV file with column names
def write_batch(batch_df, batch_id):
    # Print to console
    print(f"\n--- Batch {batch_id} ---")
    batch_df.show(truncate=False)
    
    # Save the batch DataFrame as a CSV file with headers included
    if batch_df.count() > 0:
        out_path = os.path.join(output_dir, f"batch_{batch_id}")
        batch_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
        print(f"[Task 3] Batch {batch_id} successfully written to {out_path}")

# Use foreachBatch to apply the function to each micro-batch
query = final_output.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
