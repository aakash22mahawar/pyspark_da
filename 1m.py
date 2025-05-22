from spark_session_100k import create_spark
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


class MovieRecommender:
    def __init__(self, ratings_path, movies_path):
        self.spark, self.logger = create_spark()
        self.ratings_path = ratings_path
        self.movies_path = movies_path
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

        self.logger.info(f"Top {top_n} similar movies for movie ID {movie_id}")
        result.show(top_n, truncate=False)
        return result


# Usage
if __name__ == "__main__":
    recommender = MovieRecommender(
        ratings_path="file:///C:/Users/AakashMahawar/aakash_spark_course/ml-1m/ratings.dat",
        movies_path="file:///C:/Users/AakashMahawar/aakash_spark_course/ml-1m/movies.dat"
    )
    recommender.load_data()
    recommender.compute_similarity()
    recommender.get_top_similar_movies(movie_id=101, top_n=10)
