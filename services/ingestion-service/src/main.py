import time
import os
import json
import random
import logging
from kafka import KafkaProducer

# Configure logging to look like a production service
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("IngestionService")

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    logger.info("Initializing Ingestion Service...")
    
    # In docker-compose, we resolve the broker via the service name 'kafka'
    # For local testing outside docker, switch this to 'localhost:9092'
    broker_address = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    producer = None
    # Resilience pattern: Retry connection until Kafka broker is live
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker_address],
                value_serializer=json_serializer,
                acks='all' # Ensure high data durability
            )
        except Exception as e:
            logger.warning(f"Kafka not ready yet ({e}). Retrying in 3 seconds...")
            time.sleep(3)

    logger.info("Connected to Kafka. Beginning stream production...")

    # Mock real-world business complaints/tickets
    mock_payloads = [
        {"customer_id": "CUST-101", "ticket_id": "T-801", "issue": "Our API pipeline is throwing 504 Gateway Timeouts during peak sync hours. Fix immediately."},
        {"customer_id": "CUST-102", "ticket_id": "T-802", "issue": "Can I get an extension on our trial period for the database indexing feature?"},
        {"customer_id": "CUST-103", "ticket_id": "T-803", "issue": "Your system UI is sluggish. Also, why am I restricted on data exports?"},
    ]

    try:
        while True:
            payload = random.choice(mock_payloads)
            # Add a timestamp to represent dynamic data streaming
            payload['timestamp'] = time.time()
            
            logger.info(f"Streaming ticket {payload['ticket_id']} to topic 'incoming-tickets'")
            
            # Asynchronous send with a callback for proper delivery confirmation
            future = producer.send('incoming-tickets', value=payload)
            future.get(timeout=10) # Block temporarily to ensure confirmation
            
            time.sleep(10) # Stream a ticket every 10 seconds
            
    except KeyboardInterrupt:
        logger.info("Ingestion Service shutting down cleanly.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()