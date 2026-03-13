from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HN Bronze Streaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
# Kafka source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hn_news") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka value is bytes → convert to string
json_df = kafka_df.selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("url", StringType()),
    StructField("score", IntegerType()),
    StructField("by", StringType()),
    StructField("time", LongType())
])

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Write Bronze layer
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/spark/lakehouse/bronze/hn_news") \
    .option("checkpointLocation", "/opt/spark/checkpoints/bronze_hn") \
    .outputMode("append") \
    .start()

query.awaitTermination()