import random
import os
import re
from spark_session_100k import create_spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Initialize Spark session and logger
spark, logger = create_spark()

# Path to the log file
LOG_FILE = "/home/amahawar/spark_course/pyspark_da/stream_dir/access_log.txt"

# UDF to read a random line from the log file
def get_random_log_line():
    try:
        if not os.path.exists(LOG_FILE):
            return None
        file_size = os.path.getsize(LOG_FILE)
        if file_size == 0:
            return None

        with open(LOG_FILE, "r") as lf:
            while True:
                random_position = random.randint(0, file_size - 1)
                lf.seek(random_position)
                lf.readline()  # skip partial line
                line = lf.readline().strip()
                if line:
                    return line
    except Exception as e:
        print(str(e))
        return None

# Regex to capture URL
userAgentExp = r'\"[^\"]*\" \"([^\"]+)\"'

# UDF to extract URL from log line; returns empty string instead of None to avoid NULLs
def extract_url(log_line):
    if log_line is None:
        return ""
    match = re.findall(userAgentExp, log_line)
    if match:
        return match[0]
    else:
        return ""

# Register Python UDFs as Spark UDFs with return types
get_random_log_udf = F.udf(get_random_log_line, StringType())

# Register extract_url UDF for SQL
spark.udf.register("extract_url", extract_url, StringType())

# Read from rate stream to simulate data arrival
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Add a column that holds a randomly sampled log line
logs_stream = rate_df.withColumn('log', get_random_log_udf())

# Create temporary view for SQL processing
logs_stream.createOrReplaceTempView("raw_logs")

# SQL query that extracts URL once, then filters on that column to exclude empty strings
url_count_query = """
WITH extracted AS (
  SELECT extract_url(log) AS url
  FROM raw_logs
)
SELECT url, COUNT(*) AS count
FROM extracted
WHERE url != ""
GROUP BY url
ORDER BY count DESC
LIMIT 5
"""

# Execute the SQL query
url_counts_df = spark.sql(url_count_query)

# Output the streaming result to console
query = url_counts_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Keep the stream running
query.awaitTermination()

# Stop Spark when done (optional, rarely reached in streaming)
spark.stop()

