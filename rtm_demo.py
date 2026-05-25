from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

spark = SparkSession.builder \
    .appName("PySpark RTM Event Aggregation") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0-preview5") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.streaming.realTimeMode.allowlistCheck", "false") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spark_stream_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event", StringType(), True),
    StructField("producer_timestamp_ms", LongType(), True),
    StructField("producer_timestamp", DoubleType(), True),
    StructField("value", IntegerType(), True)
])

# Parse JSON and extract event
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ============================================
# AGGREGATION ONLY: Count records per event
# ============================================
agg_df = parsed_df \
    .groupBy(col("event")) \
    .agg(count("*").alias("record_count"))

# Write aggregated results to Kafka output topic
output_df = agg_df.select(
    col("event").cast("string").alias("key"),
    to_json(struct(
        col("event"),
        col("record_count")
    )).alias("value")
)

kafka_query = output_df.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "event_counts_topic") \
    .option("checkpointLocation", "/tmp/checkpoints/event_counts") \
    .trigger(realTime="5 seconds") \
    .start()

# Optional: Also print to console for debugging
# console_query = agg_df.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .trigger(realTime="1 minute") \
#     .start()

spark.streams.awaitAnyTermination()%       