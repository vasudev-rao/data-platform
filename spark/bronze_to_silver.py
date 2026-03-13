from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Schema for scraped data
schema = StructType() \
    .add("title", StringType()) \
    .add("link", StringType()) \
    .add("timestamp", DoubleType())

# Read Bronze data
bronze_df = spark.read.format("delta").load("/tmp/delta/bronze")

# Parse JSON and clean
silver_df = bronze_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Write cleaned data to Silver
silver_df.write.format("delta").mode("append").save("/tmp/delta/silver")
