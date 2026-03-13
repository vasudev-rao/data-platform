from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HN Gold Analytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read Silver data (batch)
silver_df = spark.read.parquet("/opt/spark/lakehouse/silver/hn_news")

# Top authors by number of posts
top_authors = silver_df.groupBy("by") \
    .agg(count("*").alias("total_posts")) \
    .orderBy(desc("total_posts"))

# Write Gold table
top_authors.write \
    .mode("overwrite") \
    .parquet("/opt/spark/lakehouse/gold/top_authors")

print("Gold table created successfully")
