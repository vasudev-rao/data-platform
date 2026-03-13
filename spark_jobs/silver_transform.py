from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HN Silver Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema (same as Bronze output)
schema = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("url", StringType()),
    StructField("score", IntegerType()),
    StructField("by", StringType()),
    StructField("time", LongType())
])

# Read Bronze streaming data
bronze_df = spark.readStream \
    .schema(schema) \
    .parquet("/opt/spark/lakehouse/bronze/hn_news")

# Transformations
silver_df = bronze_df \
    .filter(col("title").isNotNull()) \
    .filter(col("id").isNotNull()) \
    .withColumn("event_time", from_unixtime(col("time"))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .dropDuplicates(["id"])

# Write Silver layer
query = silver_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/spark/lakehouse/silver/hn_news") \
    .option("checkpointLocation", "/opt/spark/lakehouse/silver/checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
