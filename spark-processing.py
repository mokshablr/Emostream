

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count
from pyspark.sql.types import StringType, StructType, TimestampType

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "emoji_topic"
OUTPUT_TOPIC = "processed_emoji_topic"

# Create a Spark session
spark = SparkSession.builder \
    .appName("EmojiProcessor") \
    .getOrCreate()

# Define schema for incoming Kafka messages
schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji_type", StringType()) \
    .add("timestamp", TimestampType())

# Read stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Decode Kafka messages
decoded_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .selectExpr("CAST(json_value AS STRING)") \
    .selectExpr("from_json(json_value, 'user_id STRING, emoji_type STRING, timestamp TIMESTAMP') AS data") \
    .select("data.*")

# Perform aggregation: count emojis per 1-minute window
aggregated_stream = decoded_stream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),  # 1-minute time windows
        col("emoji_type")
    ).agg(
        count("emoji_type").alias("count")  # Count emojis per type
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("emoji_type"),
        col("count")
    )

# Write aggregated results to the processed Kafka topic
query = aggregated_stream \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .outputMode("append")\
    .start()

# Await termination of the stream
query.awaitTermination()

