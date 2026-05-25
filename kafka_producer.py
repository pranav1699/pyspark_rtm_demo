#!/usr/bin/env python3
"""
Kafka Producer for Transactional Data
Generates realistic transaction events with customer_id, country_id, payment_method_id
"""

from kafka import KafkaProducer
import json
import time
import random
import uuid

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "transactions_topic"

# Sample data pools
CUSTOMER_IDS = [
    "CUST001", "CUST002", "CUST003", "CUST004", "CUST005",
    "CUST006", "CUST007", "CUST008", "CUST009", "CUST010",
    "CUST011", "CUST012", "CUST013", "CUST014", "CUST015",
    "CUST016", "CUST017", "CUST018", "CUST019", "CUST020"
]

PAYMENT_METHOD_IDS = [
    "PM001", "PM002", "PM003", "PM004", "PM005",
    "PM006", "PM007", "PM008", "PM009", "PM010",
    "PM011", "PM012", "PM013", "PM014", "PM015"
]

COUNTRY_CODES = ["US", "UK", "DE", "FR", "IN", "CA", "AU"]

TRANSACTION_TYPES = [
    "purchase", "refund", "subscription", "withdrawal",
    "deposit", "transfer", "payment", "chargeback"
]

CURRENCY_CODES = ["USD", "EUR", "GBP", "INR", "CAD", "AUD"]

STATUS_CODES = ["completed", "pending", "failed", "cancelled", "processing"]

def generate_transaction():
    """Generate a single transaction event"""
    customer_id = random.choice(CUSTOMER_IDS)
    payment_method_id = random.choice(PAYMENT_METHOD_IDS)
    country_id = random.choice(COUNTRY_CODES)
    transaction_type = random.choice(TRANSACTION_TYPES)

    # Amount based on transaction type
    if transaction_type in ["purchase", "subscription"]:
        amount = round(random.uniform(10.0, 500.0), 2)
    elif transaction_type == "refund":
        amount = round(random.uniform(5.0, 200.0), 2)
    elif transaction_type == "transfer":
        amount = round(random.uniform(50.0, 1000.0), 2)
    else:
        amount = round(random.uniform(1.0, 100.0), 2)

    # Currency based on country
    country_currency_map = {
        "US": "USD", "UK": "GBP", "DE": "EUR", "FR": "EUR",
        "IN": "INR", "CA": "CAD", "AU": "AUD"
    }
    currency = country_currency_map.get(country_id, "USD")

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "payment_method_id": payment_method_id,
        "country_id": country_id,
        "transaction_type": transaction_type,
        "amount": amount,
        "currency": currency,
        "status": random.choice(STATUS_CODES),
        "merchant_id": f"MERCHANT_{random.randint(100, 999)}",
        "device_type": random.choice(["mobile", "web", "pos", "api"]),
        "producer_timestamp_ms": int(time.time() * 1000),
        "transaction_reference": f"TXN-{random.randint(100000, 999999)}"
    }

    return transaction

def main():
    print("=" * 60)
    print("Kafka Transaction Producer Starting")
    print(f"Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Target Topic: {TOPIC_NAME}")
    print("=" * 60)

    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        batch_size=16384,
        linger_ms=5
    )

    print("\nProducer connected successfully")
    print("Starting to send transactions")
    print("Press Ctrl+C to stop\n")

    message_count = 0
    start_time = time.time()

    try:
        while True:
            transaction = generate_transaction()

            # Use customer_id as key for partitioning
            key = transaction["customer_id"]

            producer.send(
                topic=TOPIC_NAME,
                key=key,
                value=transaction
            )

            message_count += 1

            # Print progress every 100 messages
            if message_count % 1000 == 0:
                elapsed = time.time() - start_time
                rate = message_count / elapsed if elapsed > 0 else 0
                print(f"Sent {message_count} messages | Rate: {rate:.1f} msg/sec | "
                      f"Latest: {transaction['transaction_type']} - ${transaction['amount']} "
                      f"({transaction['customer_id']})")

            # Control throughput: ~100 messages per second
            time.sleep(0.01)

    except KeyboardInterrupt:
        print("\n\nStopping producer")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        print(f"\nTotal messages sent: {message_count}")
        print(f"Total time: {elapsed:.2f} seconds")
        print(f"Average rate: {message_count/elapsed:.1f} messages/sec")
        print("Producer closed successfully")

if __name__ == "__main__":
    main()
