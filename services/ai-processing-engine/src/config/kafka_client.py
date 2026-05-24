import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from .settings import AppSettings

logger = logging.getLogger("AIProcessingEngine.KafkaFactory")

def initialize_kafka_producer():
    """Retries connection exponentially until the Kafka producer can bind to the cluster."""
    attempt = 1
    delay = 2
    max_delay = 30
    logger.info(f"Connecting Producer to Kafka broker at {AppSettings.KAFKA_BROKER}...")
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[AppSettings.KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all'
            )
        except NoBrokersAvailable:
            logger.warning(f"[Producer Attempt {attempt}] Broker not ready yet. Retrying in {delay}s...")
            time.sleep(delay)
            attempt += 1
            delay = min(delay * 2, max_delay)

def initialize_kafka_consumer():
    """Retries connection exponentially until the Kafka consumer can bind to the cluster."""
    attempt = 1
    delay = 2
    max_delay = 30
    logger.info(f"Connecting Consumer to Kafka broker at {AppSettings.KAFKA_BROKER}...")
    while True:
        try:
            return KafkaConsumer(
                AppSettings.TOPIC_INCOMING,
                bootstrap_servers=[AppSettings.KAFKA_BROKER],
                group_id=AppSettings.CONSUMER_GROUP,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
        except NoBrokersAvailable:
            logger.warning(f"[Consumer Attempt {attempt}] Broker not ready yet. Retrying in {delay}s...")
            time.sleep(delay)
            attempt += 1
            delay = min(delay * 2, max_delay)