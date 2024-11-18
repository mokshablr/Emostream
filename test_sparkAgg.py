import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("SparkTest") \
        .master("local[1]") \
        .getOrCreate()

def test_emoji_aggregation(spark):
    test_data = [
        ("1", "❤️", "2024-11-18 10:00:00"),
        ("2", "❤️", "2024-11-18 10:00:01"),
        ("3", "❤️", "2024-11-18 10:00:02"),
    ]
    
    schema = ["user_id", "emoji_type", "timestamp"]

    df = spark.createDataFrame(test_data, schema)
    
    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    aggregated_df = df.groupBy("emoji_type").count()

    result = aggregated_df.collect()
    
    assert len(result) == 1
    assert result[0]["emoji_type"] == "❤️"
    assert result[0]["count"] == 3
