# Data Dictionary

This folder contains the sample input datasets used in the PySpark Movie Ratings Analytics Pipeline.

## Files

| File | Description |
|---|---|
| `movies.csv` | Movie metadata including movie ID, name, genre, and release year |
| `ratings.csv` | User movie ratings with user ID, movie ID, rating, and timestamp |

## Data Quality Issues Included

The sample data intentionally includes:

- missing rating value
- duplicate rating record
- invalid rating value above 5
- invalid release year value

These issues are used to practice validation and cleaning in PySpark.