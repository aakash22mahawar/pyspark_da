from spark_session_100k import create_spark
from pyspark.sql import functions as F
import time

spark, logger = create_spark()

df_stream = (spark.readStream.format("text").option('maxFilesPerTrigger',1)
             .load("/home/amahawar/spark_course/pyspark_da/stream_dir/logs")) #consider only 1 file

print("Is streaming:", df_stream.isStreaming)

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logs_df = df_stream.select(F.regexp_extract('value', hostExp, 1).alias('host'),
                         F.regexp_extract('value', timeExp, 1).alias('timestamp'),
                         F.regexp_extract('value', generalExp, 1).alias('method'),
                         F.regexp_extract('value', generalExp, 2).alias('endpoint'),
                         F.regexp_extract('value', generalExp, 3).alias('protocol'),
                         F.regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         F.regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logs_df = logs_df.withColumn("eventTime", F.current_timestamp())

# Keep a running count of endpoints
logs_df = logs_df.groupBy(F.window(F.col("eventTime"),
      "30 seconds", "10 seconds"), F.col("endpoint")).count().sort('count',ascending = False) # 30 sec is window size ,10 sec is slide interval

def process_batch(df, batch_id):
    print(f"\n==== Batch: {batch_id} ====")
    df.show(truncate=False, n=10)  # Show up to 10 rows
    time.sleep(2)  # Delay between batches (5 seconds)


query = (logs_df.writeStream
    .format("console")
    .foreachBatch(process_batch)
    .outputMode("complete")   #only for aggregation mode
    .start())

query.awaitTermination()
# Cleanly shut down the session
spark.stop()









