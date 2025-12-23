import os
import sqlite3
from pyspark.sql import SparkSession

def load_data():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    PARQUET_PATH = os.path.join(BASE_DIR, "data/processed/books_clean_parquet")
    DB_PATH = os.path.join(BASE_DIR, "data/books.db")

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Books Load") \
        .getOrCreate()

    # Read parquet
    df = spark.read.parquet(PARQUET_PATH)

    # Convert to Pandas for SQLite
    pdf = df.toPandas()

    conn = sqlite3.connect(DB_PATH)
    pdf.to_sql("books", conn, if_exists="replace", index=False)
    conn.close()

    spark.stop()
    print("Data loaded into SQLite successfully")

if __name__ == "__main__":
    load_data()
