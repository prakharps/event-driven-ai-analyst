import os

class AppSettings:
    """Centralized, immutable application configurations fetched from system runtime."""
    
    # Kafka Topology Configuration
    KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    TOPIC_INCOMING = 'incoming-tickets'
    TOPIC_RESOLVED = 'resolved-tickets'
    TOPIC_DLQ = 'malformed-tickets'
    CONSUMER_GROUP = 'ai-engine-group'
    
    # Database Layer Settings
    DB_HOST = os.getenv('DB_HOST', 'mysql-db')
    DB_USER = os.getenv('DB_USER', 'analyst_user')
    DB_PASS = os.getenv('DB_PASSWORD', 'analyst_password')
    DB_NAME = os.getenv('DB_NAME', 'business_db')

    # Vector Memory Settings
    CHROMA_PERSIST_DIR = os.getenv('CHROMA_PERSIST_DIR', '/app/chroma_db')
    
    # AI Engine Hyperparameters
    OPENAI_MODEL = "gpt-4o-mini"
    LLM_TEMPERATURE = 0.1