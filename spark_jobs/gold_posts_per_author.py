from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

spark = SparkSession.builder \
    .appName("HN Gold Posts Per Author") \
    .getOrCreate()

schema = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("url", StringType()),
    StructField("score", IntegerType()),
    StructField("by", StringType()),
    StructField("time", LongType())
])

silver_df = spark.readStream \
    .schema(schema) \
    .parquet("/opt/spark/lakehouse/silver/hn_news")

author_counts = silver_df.groupBy("by").agg(count("*").alias("total_posts"))

query = author_counts.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/opt/spark/lakehouse/gold/posts_per_author") \
    .option("checkpointLocation", "/opt/spark/lakehouse/gold/checkpoints_author") \
    .start()

query.awaitTermination()
