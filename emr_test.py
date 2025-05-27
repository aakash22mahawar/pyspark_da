from pyspark.sql import SparkSession

def create_spark():
    spark = (SparkSession.builder.appName("pyspark_course").config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=file:///home/amahawar/log4j.properties").getOrCreate())
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.INFO)  # Set desired logging level

    logger.info("Starting pyspark_course")

    return spark, logger

spark,logger = create_spark()

# Create DataFrame
data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["Name", "Id"])

print(f"DataFrame count: {df.count()}")

logger.info(f"DataFrame count: {df.count()}")

# Stop the Spark session
spark.stop()