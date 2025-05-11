from spark_session import create_spark
spark,logger = create_spark()

# Create DataFrame
data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["Name", "Id"])

print(f"DataFrame count: {df.count()}")

logger.info(f"DataFrame count: {df.count()}")

# Stop the Spark session
spark.stop()