from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("server_id", StringType(), True)
])
spark = SparkSession.builder \
    .appName("EmojiProcessing") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_topic") \
    .load()

parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")
aggregated_stream = parsed_stream.groupBy(
    window(col("timestamp"), "2 seconds"),
    col("emoji_type"),
    col("server_id")  
).count()

scaled_stream = aggregated_stream.withColumn(
    "scaled_count", expr("FLOOR(count / 1000) + IF(count % 1000 > 0, 1, 0)")
)
query = scaled_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()

