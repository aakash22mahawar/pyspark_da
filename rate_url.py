from spark_session_100k import create_spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re


spark,logger = create_spark()

# Step 2: Simulate rate source (1 row/second)
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# Step 3: Generate fake log lines with embedded URLs
def generate_fake_log(ts):
    return ('66.249.75.159 - - [timestamp] '
            '"GET /robots.txt HTTP/1.1" 200 55 "-" '
            '"Mozilla/5.0 (compatible; Googlebot/2.1; '
            '+http://www.google.com/bot.html)"')

generate_log_udf = F.udf(generate_fake_log, StringType())
logs_df = rate_df.withColumn("log", generate_log_udf("timestamp"))

# Step 4: Extract URL from log line
def extract_url(log_line):
    match = re.search(r'\+?(http[s]?://[^\s"\)]+)', log_line)
    return match.group(1) if match else None

extract_url_udf = F.udf(extract_url, StringType())
logs_df = logs_df.withColumn("url", extract_url_udf("log"))

# Step 5: Group by URL and count
result_df = logs_df.groupBy("url").count()

# Step 6: Output to console
query = result_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

