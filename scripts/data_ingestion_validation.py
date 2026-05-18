from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim, avg, desc
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


#data cleaning - removing duplicates and invalid records

cleaned_ratings_df = ratings_df.dropDuplicates().filter( #filter keeps rows that satisfy the condition
    (col("rating").isNotNull()) &
    (col("rating") >= 0) &
    (col("rating") <=5)
)

cleaned_movies_df = movies_df.dropDuplicates().filter(
    (col("release_year").isNotNull()) &
    (col("release_year") >= 1900) &
    (col("release_year") <= current_year) &
    (trim(col("movie_name")) != "")
).withColumn("movie_name", trim(col("movie_name"))) #remove leading/trailing whitespace from movie names

print("\nCleaned movies dataset:")
cleaned_movies_df.show()
print("\nCleaned ratings dataset:")
cleaned_ratings_df.show()

#data transformation - joining cleaned datasets to create a combined dataset for analysis
movies_with_ratings_df = cleaned_movies_df.join(
    cleaned_ratings_df,
    on = "movie_id",
    how ="inner" 
)

print("\nCombined movies with ratings dataset:")
movies_with_ratings_df.show()

#performing aggregations

#calculating average rating per movie along with total number of ratings for each movie
average_rating_per_movie_df = movies_with_ratings_df.groupBy(
    "movie_id", "movie_name", "genre", "release_year"
).agg(
    avg("rating").alias("average_rating"),
    count("rating").alias("total_ratings")
)
print("\nAverage rating and total ratings per movie:")
average_rating_per_movie_df.show()

#calculating most popular movies based on total number of ratings
popular_movies_df = average_rating_per_movie_df.orderBy(
    col("total_ratings").desc()
)
print("\nMost popular movies based on total ratings:")
popular_movies_df.show()

average_rating_by_genre_df = movies_with_ratings_df.groupBy(
    "genre"
).agg(
    avg("rating").alias("average_rating"),
    count("rating").alias("total_ratings")
)
print("\nAverage rating and total ratings by genre:")
average_rating_by_genre_df.show()