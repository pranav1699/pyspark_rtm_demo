#!/usr/bin/env python3
"""
PySpark Real-Time Mode (RTM) Streaming Pipeline
Enriches transactional data with customer & payment method info via stream-static joins
Pushes enriched data to output Kafka topic with minimal latency

Spark Version: 4.1.1+ (RTM introduced in 4.1.1)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, current_timestamp,
    expr, when, round as spark_round, broadcast, concat, lit as spark_lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, LongType, BooleanType
)

# ============================================
# 1. Spark Session with RTM Optimizations
# ============================================
spark = SparkSession.builder \
    .appName("RTM_TransactionEnrichment_Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0-preview5") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/rtm_pipeline") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.streaming.realTimeMode.allowlistCheck", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    \
    .getOrCreate()

print("=" * 70)
print("PySpark RTM Transaction Enrichment Pipeline Started")
print("=" * 70)
print(f"Spark Version: {spark.version}")
print("Mode: Real-Time Mode (RTM) - Stateless Processing")
print("Features: Stream-Static Joins, Low Latency, Kafka Sink")
print("=" * 70)

# ============================================
# 2. Load STATIC Lookup Tables (CSV Files)
# ============================================
print("\nLoading static lookup tables...")

# Customer lookup table
customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("/home/pranav/Downloads/Projects/pyspark_rtm/data/customers.csv") \
    .select(
        col("customer_id").cast("string"),
        col("customer_name").cast("string"),
        col("customer_email").cast("string"),
        col("customer_tier").cast("string"),
        col("customer_region").cast("string"),
        col("customer_registration_date").cast("string")
    )

print(f"Customers loaded: {customers_df.count()} records")
customers_df.show(3, truncate=False)


payment_methods_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("/home/pranav/Downloads/Projects/pyspark_rtm/data/payment_methods.csv") \
    .select(
        col("payment_method_id").cast("string"),
        col("payment_method_name").cast("string"),
        col("payment_method_type").cast("string"),
        col("payment_method_category").cast("string"),
        col("payment_processing_fee_percent").cast("double")
    )

print(f"Payment methods loaded: {payment_methods_df.count()} records")
payment_methods_df.show(3, truncate=False)

# Broadcast both tables for efficient stream-static joins
broadcast_customers = broadcast(customers_df)
broadcast_payments = broadcast(payment_methods_df)


print("\nConnecting to Kafka stream...")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.consumer.group.id", "rtm_enrichment_consumer") \
    .load()

print("Kafka stream connected")


transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("payment_method_id", StringType(), True),
    StructField("country_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("producer_timestamp_ms", LongType(), True),
    StructField("transaction_reference", StringType(), True)
])

parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

print("\nTransaction Schema:")
parsed_stream.printSchema()


filtered_stream = parsed_stream.filter(col("status") != "cancelled")


print("\nJoining with Customer lookup table...")

enriched_with_customer = filtered_stream.join(
    broadcast_customers,
    on="customer_id",
    how="left_outer"
)


print("Joining with Payment Method lookup table...")

fully_enriched = enriched_with_customer.join(
    broadcast_payments,
    on="payment_method_id",
    how="left_outer"
)


print("\nApplying business transformations...")

final_enriched = fully_enriched \
    .withColumn("processing_timestamp_ms", expr("cast(current_timestamp() as long) * 1000")) \
    .withColumn("e2e_latency_ms", 
        spark_round(col("processing_timestamp_ms") - col("producer_timestamp_ms"), 2)
    ) \
    .withColumn("processing_date", current_timestamp()) \
    .withColumn("processing_fee_amount",
        spark_round(col("amount") * col("payment_processing_fee_percent") / 100.0, 2)
    ) \
    \
    .withColumn("is_high_value", when(col("amount") >= 100.0, lit(True)).otherwise(lit(False))) \
    \
    .withColumn("is_premium_customer", 
        when(col("customer_tier").isin(["premium", "gold"]), lit(True)).otherwise(lit(False))
    ) \
    \
    .withColumn("risk_score",
        when((col("amount") > 500.0) & (col("customer_tier") == "free"), lit("HIGH"))
        .when(col("transaction_type") == "chargeback", lit("HIGH"))
        .when(col("amount") > 200.0, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    ) \
    \
    .withColumn("transaction_category",
        when(col("transaction_type").isin(["purchase", "subscription"]), lit("revenue"))
        .when(col("transaction_type").isin(["refund", "chargeback"]), lit("loss"))
        .otherwise(lit("neutral"))
    )


output_df = final_enriched.select(
    col("transaction_id").cast("string").alias("key"),

    to_json(struct(
        col("transaction_id"),
        col("transaction_reference"),
        col("transaction_type"),
        col("transaction_category"),
        col("amount"),
        col("currency"),
        col("status"),
        col("country_id"),
        col("merchant_id"),
        col("device_type"),

        col("customer_id"),
        col("customer_name"),
        col("customer_email"),
        col("customer_tier"),
        col("customer_region"),
        col("customer_registration_date"),
        col("is_premium_customer"),

        col("payment_method_id"),
        col("payment_method_name"),
        col("payment_method_type"),
        col("payment_method_category"),
        col("payment_processing_fee_percent"),
        col("processing_fee_amount"),

        col("is_high_value"),
        col("risk_score"),

        col("producer_timestamp_ms"),
        col("processing_timestamp_ms"),
        col("e2e_latency_ms"),
        col("processing_date")
    )).alias("value")
)

# ============================================
# 10. Write to Kafka Output Topic (RTM Mode)
# ============================================
print("\nStarting Kafka output stream...")

kafka_output = output_df.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "enriched_transactions_topic") \
    .option("checkpointLocation", "/tmp/checkpoints/rtm_pipeline/kafka_output") \
    .trigger(realTime="5 seconds") \
    .start()

print("Kafka output stream started")


spark.streams.awaitAnyTermination()
