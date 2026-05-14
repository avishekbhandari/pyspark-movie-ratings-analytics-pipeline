from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim
from pyspark.sql.types import *
from datetime import datetime

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


#Validation checks

#null value validation

print("Checking for null values in ratings dataset:")
ratings_df.select([
    count(when(col(c).isNull(), c)).alias(c + "_null_count")
    for c in ratings_df.columns
]).show()


print("\nChecking for null values in movies dataset:")
movies_df.select([
    count(when(col(c).isNull(), c)).alias(c + "_null_count")
    for c in movies_df.columns
]).show()


#duplicate record validation


duplicate_rating_rows_df = ratings_df.groupBy(ratings_df.columns).count().filter(col("count") > 1)
print("Checking for duplicate records in ratings dataset:")
duplicate_rating_rows_df.show()

duplicate_ratings_count = ratings_df.count() - ratings_df.dropDuplicates().count()
print(f"Number of duplicate records in ratings dataset: {duplicate_ratings_count}")


duplicate_movies_rows_df = movies_df.groupBy(movies_df.columns).count().filter(col("count") > 1)
print("\nChecking for duplicate records in movies dataset:")
duplicate_movies_rows_df.show()


duplicate_movies_count = movies_df.count() - movies_df.dropDuplicates().count()
print(f"Number of duplicate records in movies dataset: {duplicate_movies_count}")


#rating range validation

invalid_ratings = ratings_df.filter(
    (col("rating") < 0) | (col("rating") > 5)
)
print("\nChecking for invalid rating values in ratings dataset:")
invalid_ratings.show()

invalid_ratings_count = invalid_ratings.count()
print(f"Number of invalid rating values in ratings dataset: {invalid_ratings_count}")


#release year range validation

current_year = datetime.now().year
invalid_release_years = movies_df.filter(
    (col("release_year") < 1900) | (col("release_year") > current_year)
)
print("\nChecking for invalid release year values in movies dataset:")
invalid_release_years.show()

invalid_release_years_count = invalid_release_years.count()
print(f"Number of invalid release year values in movies dataset: {invalid_release_years_count}")


#empty string validation for movie_name

print("\nChecking for empty movie names:")

empty_movie_names = movies_df.filter(
    trim(col("movie_name")) == ""
)

empty_movie_names.show()
print(f"Empty movie names found: {empty_movie_names.count()}")