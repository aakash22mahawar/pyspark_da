from pyspark.sql import SparkSession
import os
import sys

# Set up environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jdk-11"

def create_spark():
    spark = (SparkSession.builder
        .appName("pyspark_course")
        .master("local[*]")      # uses all logical cores  psutil.cpu_count(logical=True)
        .config("spark.local.dir", r"C:\Users\AakashMahawar\aakash_spark")
        .config("spark.ui.port", "4051")
        .config("spark.driver.extraJavaOptions","-Dlog4j.configurationFile=file:///C:/Users/AakashMahawar/aakash_spark/conf/log4j.properties")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.default.parallelism", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=20")
        #.config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=20")
        .getOrCreate())

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.setLevel(log4jLogger.Level.INFO)  # Set desired logging level

    logger.info("Starting pyspark_100k")

    return spark,logger



# if __name__ == '__main__':
#     create_spark()

