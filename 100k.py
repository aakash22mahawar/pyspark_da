from spark_session_100k import create_spark
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, IntegerType, StructType, StructField,LongType


spark,logger = create_spark()
# Create schema when reading u.data
schema = StructType([StructField("userID", IntegerType(), True),
                     StructField("movieID", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])

# Read the file with the given schema and options
df1 = spark.read \
    .option("sep", "\t") \
    .schema(schema) \
    .csv("file:///C:/Users/AakashMahawar/aakash_spark_course/ml-100k/u.data")

logger.info('Reading rating data')
logger.info(f'Shape of the df1 : [{df1.count()},{len(df1.columns)}]')

schema1 = StructType([
                     StructField("movieID", IntegerType(), True),
                     StructField("title", StringType(), True),
                     StructField("release_date", StringType(), True),
                     StructField("skip", StringType(), True),  # placeholder for empty field
                     StructField("imdb_link", StringType(), True)])

# Read the file with the given schema and options
df2 = spark.read \
    .option("sep", "|") \
    .schema(schema1) \
    .csv("file:///C:/Users/AakashMahawar/aakash_spark_course/ml-100k/u.item")

df2 = df2[['movieID','title','release_date','imdb_link']]
logger.info('Reading movie data')
logger.info(f'Shape of the df2 : [{df2.count()},{len(df2.columns)}]')

df1_a = df1.alias('a')
df1_b = df1.alias('b')
logger.info('Made alias against both dfs')

# Perform the join
df = df1_a.join(F.broadcast(df1_b), on="userID", how="inner")
logger.info('Performed broadcast join')

# Rename columns clearly
dff = df.select(
    F.col("userID"),
    F.col("a.movieID").alias("movieID_a"),
    F.col("a.rating").alias("rating_a"),
    F.col("b.movieID").alias("movieID_b"),
    F.col("b.rating").alias("rating_b")
).filter(F.col("movieID_a") != F.col("movieID_b"))

# cosine similarity
#  Compute intermediate values
res = dff.withColumn('product',F.col('rating_a') * F.col('rating_b')).withColumn('rating_a_sqr',F.col('rating_a')**2)\
.withColumn('rating_b_sqr',F.col('rating_b')**2)
logger.info('Compute intermediate values')

# Group by movie pairs and compute cosine similarity
res1 = res.groupby(['movieID_a', 'movieID_b']).agg(
    F.sum('product').alias('dot_product'),
    F.sqrt(F.sum('rating_a_sqr')).alias('norm_a'),
    F.sqrt(F.sum('rating_b_sqr')).alias('norm_b')
)

res1 = res1.withColumn(
    'cosine_score',
    F.round(F.col('dot_product') / (F.col('norm_a') * F.col('norm_b')),2))[['movieID_a','movieID_b','cosine_score']]
logger.info('Compute cosine similarity')

# Rename and join on movieID_a
main = res1.join(
    F.broadcast(df2.selectExpr("movieID", "title as title_a")),
    on=df2.movieID == res1.movieID_a,
    how='inner'
).drop("movieID")


# # Now join on movieID_b
res_df = main.join(
    F.broadcast(df2.selectExpr("movieID", "title as title_b")),
    on=main.movieID_b == df2.movieID,
    how='inner'
)
# Select final columns
res_df = res_df[["movieID_a","title_a", "title_b", "cosine_score"]]

res_df = res_df[res_df['movieID_a']==101].sort('cosine_score',ascending = False) #specify movie id
logger.info('Get top 10 similar movies against any specified movie')
res_df.show(10,truncate = False)






