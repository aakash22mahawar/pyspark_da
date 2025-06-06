import random
import os
import re
import json
from spark_session_100k import create_spark
from pyspark.sql import functions as F
from pyspark.sql.functions import udtf
from pyspark.sql.types import StringType

# Initialize Spark session and logger
spark, logger = create_spark()

# Path to the log file
LOG_FILE = "/home/amahawar/spark_course/pyspark_da/stream_dir/bluesky.jsonl"
hashes = r"#\w+"  # Hashtag pattern

# UDF: Get random line with hashtag
def get_random_log_line():
    try:
        if not os.path.exists(LOG_FILE):
            return ""
        file_size = os.path.getsize(LOG_FILE)
        if file_size == 0:
            return ""

        with open(LOG_FILE, "r") as lf:
            for _ in range(5):
                random_position = random.randint(0, file_size - 1)
                lf.seek(random_position)
                lf.readline()  # skip partial
                line = lf.readline().strip()
                if line:
                    text = json.loads(line)['text']
                    if '#' in text:
                        return text
        return ""
    except Exception as e:
        print(str(e))
        return ""

# UDF: Extract hashtags
def extract_hashtags(text):
    if not text:
        return ""
    hashtags = " ".join(re.findall(hashes, text))
    return hashtags

# Register UDFs
spark.udf.register("tags", extract_hashtags, StringType())
get_random_hash_udf = F.udf(get_random_log_line, StringType())

# Read from rate stream
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 50).load()
logs_stream = rate_df.withColumn('text', get_random_hash_udf())
logs_stream.createOrReplaceTempView("raw_logs")

# UDTF: Split by space instead of comma
@udtf(returnType="hashtags:string,tags: string")
# Define UDTF class
class SplitWordsFromDF:
    def eval(self, row):
        if row["hashtags"]:
            for word in row["hashtags"].split():
                yield (row["hashtags"],word.strip(),)

# Register UDTF
spark.udtf.register("split_words_udtf", SplitWordsFromDF)

# SQL query
query = """
  WITH cte AS (
    SELECT tags(text) as hashtags
    FROM raw_logs
    WHERE text IS NOT NULL AND text != ''
  )
  SELECT tags,count(*) as c_  FROM split_words_udtf(TABLE(cte)) group by 1 order by count(*) desc limit 10
"""

# Execute & write to console
result = spark.sql(query)

main = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

main.awaitTermination()
spark.stop()

