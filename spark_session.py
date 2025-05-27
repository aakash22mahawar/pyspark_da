from pyspark.sql import SparkSession

def create_spark():
    spark = SparkSession.builder \
        .appName("pyspark_course") \
        .master("local[2]") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=file:///home/amahawar/spark_course/log4j.properties")\
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.INFO)  # Set desired logging level

    logger.info("Starting pyspark_course")

    return spark,logger





# if __name__ == '__main__':
#     create_spark()

