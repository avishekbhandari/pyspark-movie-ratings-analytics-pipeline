from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import *

#create Spark Session
spark = SparkSession.builder \
    .appName("ScalableMovieRatingsPipeline") \
    .getOrCreate()


#define schema for ratings dataset
ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

#define schema for movies dataset
movies_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("movie_name", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("release_year", IntegerType(), True)
])


#read datasets with defined schemas
ratings_df = spark.read.csv(
    "data/ratings.csv",
    header=True,
    schema=ratings_schema
)

movies_df = spark.read.csv(
    "data/movies.csv",
    header=True,
    schema=movies_schema
)


#view loaded data
ratings_df.show()
movies_df.show()


