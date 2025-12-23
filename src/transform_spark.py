import os
print("Script started")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("PySpark imported")

# Base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_PATH = os.path.join(BASE_DIR, "data/raw/books_raw.csv")
PROCESSED_PATH = os.path.join(BASE_DIR, "data/processed/books_clean_parquet")

print("RAW PATH:", RAW_PATH)
print("OUTPUT PATH:", PROCESSED_PATH)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Books ETL Transform") \
    .getOrCreate()

print("Spark session started")

# Read CSV
df = spark.read.csv(RAW_PATH, header=True, inferSchema=True)
print("Records loaded:", df.count())

# Simple transformation
df_clean = (
    df.dropna()
      .withColumn("price", col("price").cast("double"))
)

# Write Parquet
df_clean.write.mode("overwrite").parquet(PROCESSED_PATH)
print("Parquet written successfully")

spark.stop()
print("Spark stopped")
