from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_df = spark.read.format("delta").load("/tmp/delta/silver")

gold_df = silver_df.groupBy("title").agg(count("*").alias("mentions"))

gold_df.write.format("delta").mode("overwrite").save("/tmp/delta/gold")
