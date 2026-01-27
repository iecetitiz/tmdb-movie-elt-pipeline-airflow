from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

# Airflow'dan gelen değişkenleri okuyoruz
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
endpoint = os.getenv("MINIO_ENDPOINT")

spark = SparkSession.builder \
    .appName("MinIO Delta Write") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read bronze layer datasets from MinIO
df = spark.read.parquet("s3a://tmdb-bronze/credits/")
df_movies = spark.read.parquet("s3a://tmdb-bronze/movies/")

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# -------------------------
# CREW PROCESSING
# -------------------------

# Define schema for crew JSON column
crew_schema = ArrayType(
    StructType([
        StructField("department", StringType(), True),
        StructField("credit_id", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("job", StringType(), True),
    ])
)

from pyspark.sql.functions import from_json, explode, col

# Parse crew JSON string into array of structs
crew_parsed = df.withColumn(
    "crew_array",
    from_json("crew", crew_schema)
)

# Explode crew array so each crew member becomes a separate row
crew_exploded = crew_parsed.withColumn(
    "crew_item",
    explode("crew_array")
)

# Select required crew fields
crew_df = crew_exploded.select(
    col("movie_id"),
    col("title"),
    col("crew_item.id"),
    col("crew_item.name"),
    col("crew_item.department"),
    col("crew_item.job"),
    col("crew_item.credit_id"),
)

# Fill missing credit_id values
crew_df = crew_df.fillna({"credit_id": "0000000000"})

# Write crew data to silver layer as Delta table
crew_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/crew/")

# -------------------------
# CAST PROCESSING
# -------------------------

# Define schema for cast JSON column
cast_schema = ArrayType(
    StructType([
        StructField("cast_id", IntegerType(), True),
        StructField("character", StringType(), True),
        StructField("credit_id", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("order", IntegerType(), True),
    ])
)

# Parse cast JSON string into array of structs
cast_parsed = df.withColumn(
    "cast_array",
    from_json("cast", cast_schema)
)

# Explode cast array
cast_exploded = cast_parsed.withColumn(
    "cast_item",
    explode("cast_array")
)

# Select required cast fields
cast_df = cast_exploded.select(
    col("movie_id"),
    col("title"),
    col("cast_item.cast_id"),
    col("cast_item.character"),
    col("cast_item.credit_id"),
    col("cast_item.gender"),
    col("cast_item.id"),
    col("cast_item.name")
)

# Handle missing credit_id values
cast_df = cast_df.fillna({"credit_id": "0000000000"})
    
# Write cast data to silver layer as Delta table
cast_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/cast/")

# -------------------------
# MOVIES PROCESSING
# -------------------------

# Select and rename movie-level columns
movies = df_movies.select(
    col("id").alias("movie_id"),
    col("title"),
    col("budget"),
    col("homepage"),
    col("original_language"),
    col("original_title"),
    col("overview"),
    col("popularity"),
    col("release_date"),
    col("revenue"),
    col("runtime"),
    col("status"),
    col("tagline"),
    col("vote_average"),
    col("vote_count")
)

# Write movies data to silver layer as Delta table
movies.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/movies/")

# -------------------------
# GENRE PROCESSING
# -------------------------

# Define schema for genres JSON column
genre_schema = ArrayType(
    StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
)

# Parse genres JSON string
movies_parsed = df_movies.withColumn(
    "genre_array",
    from_json("genres", genre_schema)
)

# Explode genres array
movies_exploded = movies_parsed.withColumn(
    "genre_item",
    explode("genre_array")
)

# Select genre fields
movie_genre_df = movies_exploded.select(
    col("id").alias("movie_id"),
    col("genre_item.id").alias("id"),
    col("genre_item.name").alias("name")
)

# Fill missing genre IDs
movie_genre_df = movie_genre_df.fillna({"id": -9999})

# Write genres data to silver layer
movie_genre_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/genre/")

# -------------------------
# KEYWORDS PROCESSING
# -------------------------

# Define schema for keywords JSON column
keyword_schema = ArrayType(
    StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
)

# Parse keywords JSON string
movies_parsed = df_movies.withColumn(
    "keyword_array",
    from_json("keywords", keyword_schema)
)

# Explode keywords array
movies_exploded = movies_parsed.withColumn(
    "keyword_item",
    explode("keyword_array")
)

# Select keyword fields
movie_keyword_df = movies_exploded.select(
    col("id").alias("movie_id"),
    col("keyword_item.id").alias("id"),
    col("keyword_item.name").alias("name")
)

# Fill missing keyword IDs
movie_keyword_df = movie_keyword_df.fillna({"id": -9999})

# Write keywords data to silver layer
movie_keyword_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/keywords/")

# -------------------------
# PRODUCTION COMPANIES PROCESSING
# -------------------------

# Define schema for production_companies JSON column
production_company_schema = ArrayType(
    StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
)

# Parse production companies JSON string
movies_parsed = df_movies.withColumn(
    "production_company_array",
    from_json("production_companies", production_company_schema)
)

# Explode production companies array
movies_exploded = movies_parsed.withColumn(
    "production_company_item",
    explode("production_company_array")
)

# Select production company fields
movie_production_company_df = movies_exploded.select(
    col("id").alias("movie_id"),
    col("production_company_item.id").alias("id"),
    col("production_company_item.name").alias("name")
)

# Fill missing production company IDs
movie_production_company_df = movie_production_company_df.fillna({"id": -9999})

# Write production companies data to silver layer
movie_production_company_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/production_companies/")

# -------------------------
# PRODUCTION COUNTRIES PROCESSING
# -------------------------

# Define schema for production_countries JSON column
production_country_schema = ArrayType(
    StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
    ])
)

# Parse production countries JSON string
movies_parsed = df_movies.withColumn(
    "production_country_array",
    from_json("production_countries", production_country_schema)
)

# Explode production countries array
movies_exploded = movies_parsed.withColumn(
    "production_country_item",
    explode("production_country_array")
)

# Select production country fields
movie_production_country_df = movies_exploded.select(
    col("id").alias("movie_id"),
    col("production_country_item.iso_3166_1").alias("iso_3166_1"),
    col("production_country_item.name").alias("name")
)

# Fill missing country codes
movie_production_country_df = movie_production_country_df.fillna({"iso_3166_1": "XX"})

# Write production countries data to silver layer
movie_production_country_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/production_countries/")

# -------------------------
# SPOKEN LANGUAGES PROCESSING
# -------------------------

# Define schema for spoken_languages JSON column
spoken_language_schema = ArrayType(
    StructType([
        StructField("iso_639_1", StringType(), True),
        StructField("name", StringType(), True)
    ])
)

# Parse spoken languages JSON string
movies_parsed = df_movies.withColumn(
    "spoken_language_array",
    from_json("spoken_languages", spoken_language_schema)
)

# Explode spoken languages array
movies_exploded = movies_parsed.withColumn(
    "spoken_language_item",
    explode("spoken_language_array")
)

# Select spoken language fields
movie_spoken_language_df = movies_exploded.select(
    col("id").alias("movie_id"),
    col("spoken_language_item.iso_639_1").alias("iso_639_1"),
    col("spoken_language_item.name").alias("name")
)

# Fill missing language codes
movie_spoken_language_df = movie_spoken_language_df.fillna({"iso_639_1": "XX"})

# Write spoken languages data to silver layer
movie_spoken_language_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://tmdb-silver/spoken_languages/")
