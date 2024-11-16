from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define schema for emoji data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EmojiProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read data from Kafka
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_topic") \
    .load()

# Parse Kafka messages
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Aggregate emojis in 2-second windows
aggregated_stream = parsed_stream.groupBy(
    window(col("timestamp"), "2 seconds"),
    col("emoji_type")
).count()

# Scale down emoji counts (1000 = 1)
scaled_stream = aggregated_stream.withColumn(
    "scaled_count", expr("FLOOR(count / 1000) + IF(count % 1000 > 0, 1, 0)")
)

# Output results to console
query = scaled_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()

