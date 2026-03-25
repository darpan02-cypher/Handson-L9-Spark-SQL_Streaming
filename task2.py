from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg as _avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# Create a Spark session
spark = SparkSession.builder \
    .appName("RideSharingAggregations") \
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

# Compute aggregations: total fare and average distance grouped by driver_id
aggregated = parsed_stream \
    .groupBy("driver_id") \
    .agg(
        _sum("fare_amount").alias("total_fare"),
        _avg("distance_km").alias("avg_distance")
    )

output_dir = "outputs/task_2"
os.makedirs(output_dir, exist_ok=True)

def write_batch(batch_df, batch_id):
    # Action 1: Show in console
    batch_df.show(truncate=False)
    
    # Action 2: Write to CSV
    if batch_df.count() > 0:
        out_path = os.path.join(output_dir, f"batch_{batch_id}")
        batch_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
        print(f"[Task 2] Batch {batch_id} successfully written to {out_path}")

# Output to console and CSV using foreachBatch
query = aggregated.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
