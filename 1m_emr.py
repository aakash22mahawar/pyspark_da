from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import random
import datetime


class MovieRecommender:
    def __init__(self, ratings_path, movies_path, output_path):
        self.spark = (SparkSession.builder.appName("pyspark_movie_recommender")
                      .config("spark.driver.memory", "4g")
                      .config("spark.executor.memory", "4g")
                      .config("spark.sql.shuffle.partitions", "50")
                      .config("spark.default.parallelism", "4")
                      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=20")
                      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=20")
                      .getOrCreate())

        sc = self.spark.sparkContext
        log4jLogger = sc._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger(__name__)
        self.logger.setLevel(log4jLogger.Level.INFO)
        self.logger.info("Starting Spark Session 1M data on EMR")

        self.ratings_path = ratings_path
        self.movies_path = movies_path
        self.output_path = output_path

        self.df_ratings = None
        self.df_movies = None
        self.similarity_df = None

    def load_data(self):
        ratings_schema = StructType([
            StructField("userID", IntegerType(), True),
            StructField("movieID", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("timestamp", LongType(), True)
        ])
        self.df_ratings = self.spark.read.option("sep", "::").schema(ratings_schema) \
            .csv(self.ratings_path).select("userID", "movieID", "rating").cache()
        self.logger.info(f"Ratings loaded with {self.df_ratings.count()} rows.")

        movies_schema = StructType([
            StructField("movieID", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("genre", StringType(), True),
        ])
        self.df_movies = self.spark.read.option("sep", "::").schema(movies_schema) \
            .csv(self.movies_path).select("movieID", "title").cache()
        self.logger.info(f"Movies loaded with {self.df_movies.count()} rows.")

    def compute_similarity(self):
        a = self.df_ratings.alias("a")
        b = self.df_ratings.alias("b")

        joined = a.join(F.broadcast(b), "userID") \
            .filter(F.col("a.movieID") != F.col("b.movieID")) \
            .select(
            F.col("a.movieID").alias("movieID_a"),
            F.col("b.movieID").alias("movieID_b"),
            F.col("a.rating").alias("rating_a"),
            F.col("b.rating").alias("rating_b")
        )

        features = joined.withColumn("product", F.col("rating_a") * F.col("rating_b")) \
            .withColumn("rating_a_sqr", F.col("rating_a") ** 2) \
            .withColumn("rating_b_sqr", F.col("rating_b") ** 2)

        self.similarity_df = features.groupBy("movieID_a", "movieID_b").agg(
            F.sum("product").alias("dot_product"),
            F.sqrt(F.sum("rating_a_sqr")).alias("norm_a"),
            F.sqrt(F.sum("rating_b_sqr")).alias("norm_b"),
            F.count("*").alias("numPairs")  # 👈 count of user pairs
        ).withColumn(
            "cosine_score",
            F.round(F.col("dot_product") / (F.col("norm_a") * F.col("norm_b")), 2)
        ).select("movieID_a", "movieID_b", "cosine_score", "numPairs")  # 👈 include numPairs

        self.similarity_df = self.similarity_df.filter(F.col("numPairs") > 50)

        self.logger.info("Similarity computation completed.")

    def get_top_similar_movies(self, movie_id, top_n=10):
        df = self.similarity_df
        df = df.join(F.broadcast(self.df_movies.withColumnRenamed("title", "title_a")),
                     df.movieID_a == self.df_movies.movieID).drop("movieID")
        df = df.join(F.broadcast(self.df_movies.withColumnRenamed("title", "title_b")),
                     df.movieID_b == self.df_movies.movieID).drop("movieID")

        result = df.filter(F.col("movieID_a") == movie_id) \
            .select("movieID_a", "title_a", "title_b", "cosine_score") \
            .orderBy(F.desc("cosine_score"))

        self.logger.info(f"Writing top {top_n} similar movies for movie ID {movie_id} to S3")

        timestamp = datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
        s3_output_dir = f"{self.output_path}/movie_{movie_id}_{timestamp}"

        result.write.mode("overwrite").parquet(s3_output_dir)

        self.logger.info(f"Output successfully written to {s3_output_dir}")
        result.show(top_n, truncate=False)

        return result


if __name__ == "__main__":
    recommender = MovieRecommender(
        ratings_path="s3://spark-bucket-aakash/data_files/ml-1m/ratings.dat",
        movies_path="s3://spark-bucket-aakash/data_files/ml-1m/movies.dat",
        output_path="s3://spark-bucket-aakash/output/similar_movies"
    )
    recommender.load_data()
    recommender.compute_similarity()

    movie_id_list = [row.movieID for row in recommender.df_movies.select("movieID").collect()]
    random_movie_id = random.choice(movie_id_list)
    recommender.logger.info(f"Randomly selected movie ID: {random_movie_id}")

    recommender.get_top_similar_movies(movie_id=random_movie_id, top_n=10)
