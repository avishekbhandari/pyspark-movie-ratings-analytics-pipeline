# PySpark Movie Ratings Analytics Pipeline

## About This Project

This is a beginner-friendly PySpark data engineering project where I worked with small movie and ratings datasets to practice data ingestion, data validation, data cleaning, joins, aggregations, and writing output in Parquet format.

The goal of this project was not to build a large production system. I built it to practice the basic steps that are commonly used in data engineering pipelines:

- reading structured data
- defining schemas
- checking data quality issues
- cleaning invalid records
- joining datasets
- creating analytical outputs
- saving processed results

This project helped me get more hands-on practice with PySpark DataFrames and basic pipeline development.

---

## Project Overview

The project uses two CSV files:

```text
movies.csv
ratings.csv
```

The PySpark script reads both files, validates the data, removes bad records, joins movie information with rating records, and creates three analytical outputs:

```text
average_rating_per_movie
popular_movies
average_rating_by_genre
```

---

## Technologies Used

- Python
- PySpark
- Spark DataFrames
- CSV
- Parquet
- Git and GitHub

---

## Repository Structure

```text
pyspark-movie-ratings-analytics-pipeline/
|
|-- data/
|   |-- movies.csv
|   `-- ratings.csv
|
|-- scripts/
|   `-- data_ingestion_validation.py
|
|-- output/
|   |-- average_rating_per_movie/
|   |-- average_rating_by_genre/
|   `-- popular_movies/
|
|-- requirements.txt
|-- .gitignore
`-- README.md
```
---

## Source Data

### movies.csv

This file contains basic movie information.

| Column | Description |
|---|---|
| movie_id | Unique movie ID |
| movie_name | Name of the movie |
| genre | Movie genre |
| release_year | Year the movie was released |

Example records include movies such as Inception, Titanic, Avengers, Joker, and Batman.

One record includes an invalid release year value (`abc`) so that I could practice validation and cleaning.

---

### ratings.csv

This file contains movie rating records.

| Column | Description |
|---|---|
| user_id | Unique user ID |
| movie_id | Movie ID connected to the movie dataset |
| rating | Rating given by the user |
| timestamp | Rating timestamp |

This file includes some data quality issues on purpose, such as:

- missing rating value
- duplicate rating record
- invalid rating value above 5

These issues are handled in the PySpark pipeline.

---

## Main Script

The main pipeline script is:

```text
scripts/data_ingestion_validation.py
```

This script performs the complete pipeline flow:

1. Creates a Spark session.
2. Defines schemas for movies and ratings data.
3. Reads both CSV files using explicit schemas.
4. Checks null values.
5. Checks duplicate records.
6. Checks invalid rating values.
7. Checks invalid release year values.
8. Checks empty movie names.
9. Cleans invalid and duplicate records.
10. Joins movies and ratings data.
11. Creates analytical aggregations.
12. Writes final outputs in Parquet format.

---

## Data Validation Checks

The pipeline includes several validation checks before transformation.

### Null Value Check

The script checks null values in both datasets.

This is useful because missing values can affect joins, aggregations, and reporting accuracy.

---

### Duplicate Record Check

The script checks duplicate rows in:

```text
ratings.csv
movies.csv
```

The ratings dataset includes a duplicate record, and the script removes duplicates during cleaning.

---

### Rating Range Check

The valid rating range is:

```text
0 to 5
```

Any rating below 0 or above 5 is treated as invalid.

For example, the sample ratings data includes a rating value of `10`, which is removed during cleaning.

---

### Release Year Check

The script checks whether the movie release year is valid.

The valid release year range used in this project is:

```text
1900 to current year
```

The sample movie data includes an invalid release year value, which is filtered out during cleaning.

---

### Empty Movie Name Check

The script also checks whether any movie name is empty after trimming whitespace.

---

## Data Cleaning Logic

After validation, the script creates cleaned DataFrames.

### Ratings Cleaning

The ratings dataset is cleaned by:

- removing duplicate records
- removing records with null ratings
- keeping only ratings between 0 and 5

### Movies Cleaning

The movies dataset is cleaned by:

- removing duplicate records
- removing invalid release years
- removing empty movie names
- trimming extra spaces from movie names

---

## Transformation Logic

After cleaning, the script joins the movies and ratings datasets using:

```text
movie_id
```

The joined dataset combines movie details with user ratings.

```text
movies + ratings
```

This joined dataset is then used for analytics.

---

## Analytical Outputs

The project creates three analytical outputs.

### 1. Average Rating Per Movie

This output calculates:

- average rating for each movie
- total number of ratings for each movie

Output path:

```text
output/average_rating_per_movie
```

---

### 2. Popular Movies

This output sorts movies by the total number of ratings.

Output path:

```text
output/popular_movies
```

---

### 3. Average Rating By Genre

This output calculates:

- average rating by genre
- total number of ratings by genre

Output path:

```text
output/average_rating_by_genre
```

---

## Output Format

The outputs are written in Parquet format.

I used Parquet because it is a common format in data engineering and analytics workflows.

---

## Output Folder

This project includes the generated `output/` folder to show that the PySpark script was executed successfully.

The output is written in Parquet format under:

```text
output/average_rating_per_movie/
output/popular_movies/
output/average_rating_by_genre/
```
## How to Run This Project

### 1. Clone the repository

```bash
git clone https://github.com/avishekbhandari/pyspark-movie-ratings-analytics-pipeline.git
cd pyspark-movie-ratings-analytics-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the PySpark script

```bash
python scripts/data_ingestion_validation.py
```

After the script runs, it creates output folders under:

```text
output/
```

---

## What I Learned

While building this project, I practiced:

- creating a Spark session
- defining schemas in PySpark
- reading CSV files into Spark DataFrames
- checking null values
- checking duplicate records
- filtering invalid data
- cleaning DataFrames
- joining datasets
- grouping and aggregating data
- calculating average ratings
- sorting results
- writing output as Parquet

This project helped me understand how PySpark can be used for simple data validation and analytics workflows.

---

## Current Limitations

This is a small learning project, so it has some limitations:

- the dataset is very small
- the pipeline runs locally
- there is no cloud storage integration
- there is no scheduler or orchestration tool
- there are no automated unit tests
- output paths are hardcoded
- the project does not use a large real-world movie dataset

---

## Future Improvements

Future improvements could include:

- use a larger movie ratings dataset
- add a `data/README.md` file
- rename the script to something clearer like `movie_ratings_pipeline.py`
- add unit tests for validation logic
- parameterize input and output paths
- add logging instead of only using print statements
- add a simple architecture diagram
- remove generated output files from GitHub and keep only sample output documentation
- run the pipeline on Databricks or another Spark environment

---

## Project Note

This project is part of my data engineering learning portfolio. It is focused on practicing PySpark basics, data validation, data cleaning, joins, aggregations, and Parquet output using a small movie ratings dataset.
