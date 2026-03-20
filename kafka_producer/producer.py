import json
import time
import logging
import os
from kafka import KafkaProducer
from data_generator.generator import generate_stream_data

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "energy_topic")

def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks='all',
                retries=5
            )
            logger.info(f"Successfully connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            logger.error(f"Waiting for Kafka at {KAFKA_BROKER}... Error: {e}")
            time.sleep(5)

producer = get_kafka_producer()


def main():
    print(f"Starting Kafka producer on topic: {TOPIC}...", flush=True)
    while True:
        try:
            data = generate_stream_data()
            print(f"Sending data to {TOPIC}: {data}", flush=True)
            producer.send(TOPIC, value=data)
            producer.flush()
            time.sleep(1)  # Simulate real-time data flow
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()
