from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session
# extraJavaOptions fix needed for Java 17+ with PySpark 4.x
spark = SparkSession.builder \
    .appName("RideSharingAnalytics") \
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

# Print parsed data to the console
query_console = parsed_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

# Write parsed data to CSV files in outputs/task_1
query_csv = parsed_stream.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "outputs/task_1") \
    .option("checkpointLocation", "outputs/task_1_checkpoint") \
    .option("header", True) \
    .start()

query_csv.awaitTermination()
