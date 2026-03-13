from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

schema = StructType() \
    .add("title", StringType()) \
    .add("url", StringType()) \
    .add("scraped_at", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hn_news") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()