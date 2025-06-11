from pyspark.sql import SparkSession
from pyspark.sql  import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, StructField

# Define SparkSession
spark = SparkSession.builder \
    .appName("Kafka Aggregation") \
    .master("local[*]") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "devices") \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary to string
decoded_df = stream_df.selectExpr("CAST(value AS STRING) as json_str")

# Define schema
schema = StructType([
    StructField("eventId", StringType()),
    StructField("eventOffset", IntegerType()),
    StructField("eventPublisher", StringType()),
    StructField("customerId", StringType()),
    StructField("eventTime", StringType()),
    StructField("data", StructType([
        StructField("devices", ArrayType(StructType([
            StructField("deviceId", StringType()),
            StructField("temperature", IntegerType()),
            StructField("measure", StringType()),
            StructField("status", StringType())
        ])))
    ]))
])

# Parse and flatten JSON
parsed_df = decoded_df.withColumn("parsed", F.from_json(F.col("json_str"), schema)) \
    .select(
        F.col("parsed.customerId"),
        F.col("parsed.eventTime"),
        F.explode(F.col("parsed.data.devices")).alias("device")
    ) \
    .select(
        "customerId",
        F.col("eventTime").cast("timestamp").alias("event_time"),
        F.col("device.deviceId").alias("deviceId"),
        F.col("device.temperature").alias("temperature"),
        F.col("device.status").alias("status")
    )

# Filter SUCCESS events
success_df = parsed_df.filter(F.col("status") == "SUCCESS")

# Extract date
success_df = success_df.withColumn("event_date", F.date_format("event_time", "dd-MM-yyyy"))

# Aggregate: avg temp per customer/device/day
agg_df = success_df.groupBy("customerId", "deviceId", "event_date") \
    .agg(F.avg("temperature").alias("avg_temperature"))

# Output the aggregated results to console
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
