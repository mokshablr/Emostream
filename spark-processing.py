from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, expr
from pyspark.sql.types import StringType, StructType, TimestampType
import json
import math

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "emoji_topic"
OUTPUT_TOPIC = "processed_emoji_topic"

spark = SparkSession.builder \
    .appName("EmojiProcessor") \
    .getOrCreate()

schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji_type", StringType()) \
    .add("timestamp", TimestampType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

decoded_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .selectExpr("CAST(json_value AS STRING)") \
    .selectExpr("from_json(json_value, 'user_id STRING, emoji_type STRING, timestamp TIMESTAMP') AS data") \
    .select("data.*")

def scale_down_emoji_count(count):
    return math.ceil(count / 1000)  

spark.udf.register("scale_down_emoji_count", scale_down_emoji_count)


aggregated_stream = decoded_stream \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(
        window(col("timestamp"), "2 seconds"),
        col("emoji_type")
    ).agg(
        count("emoji_type").alias("raw_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("emoji_type"),
        expr("scale_down_emoji_count(raw_count)").alias("scaled_count")
    )

query = aggregated_stream \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
