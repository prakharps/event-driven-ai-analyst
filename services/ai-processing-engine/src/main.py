import json
import time
import logging
import traceback
from config.database import fetch_customer_context
from config.settings import AppSettings
from config.kafka_client import initialize_kafka_producer, initialize_kafka_consumer
from services.langchain_agent import LangChainAgent
from services.dlq_router import DLQRouter

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("AIProcessingEngine")


def main():
    logger.info("Starting AI Processing Engine Architecture...")

    producer = initialize_kafka_producer()

    # Initialize Consumer to read messy tickets
    consumer = initialize_kafka_consumer()
    # Initialize our modular LangChain agent
    ai_agent = LangChainAgent()
    dlq_router = DLQRouter(producer)

    logger.info(f"Connected to Broker. Subscribed to topic: {AppSettings.TOPIC_INCOMING}")

    # Main Event Loop
    for message in consumer:
        ticket = message.value
        ticket_id = "UNKNOWN"
        try:
            # Defensive input processing
            if isinstance(ticket, bytes):
                ticket = json.loads(ticket.decode('utf-8'))
            
            ticket_id = ticket.get("ticket_id", "UNKNOWN")
            customer_id = ticket.get("customer_id")
            issue_text = ticket.get("issue")
            
            logger.info(f"Processing Event: Ticket {ticket_id}")
            
            # Step 1: Database Context Enrichment
            try:
                context = fetch_customer_context(customer_id)
                if not context:
                    raise ValueError(f"Customer profile does not exist in corporate database for ID: {customer_id}")
            except Exception as db_err:
                dlq_router.route_to_dead_letter(
                    domain="DATA_ENRICHMENT_FAILURE",
                    error_message=str(db_err),
                    original_payload=ticket
                )
                continue

            # Step 2: AI Prompt Execution
            try:
                logger.info(f"Invoking Modular LLM Chain for ticket {ticket_id}...")
                resolution_result = ai_agent.resolve_ticket(ticket_id, issue_text, context)

                if hasattr(resolution_result, "model_dump"):
                    resolution_data = resolution_result.model_dump()
                elif isinstance(resolution_result, dict):
                    resolution_data = resolution_result
                else:
                    raise TypeError("LLM return type violated structured schema contract parameters.")
                
            except Exception as ai_err:
                dlq_router.route_to_dead_letter(
                    domain="AI_PARSING_FAILURE",
                    error_message=str(ai_err),
                    original_payload=ticket,
                    context=context
                )
                continue
                
            # Step 3: Broadcast Outbound Event
            outbound_payload = {
                "ticket_id": ticket_id,
                "customer_id": customer_id,
                "analysis_timestamp": time.time(),
                "resolution": resolution_data
            }
                
            logger.info(f"AI Resolution Complete. Priority calculated as: {outbound_payload['resolution']['priority_level']}")
            producer.send(AppSettings.TOPIC_RESOLVED, value=outbound_payload).get(timeout=10)
            logger.info(f"Successfully broadcasted payload for {ticket_id} to '{AppSettings.TOPIC_RESOLVED}'.")
                
        except Exception as global_err:
            # --- STAGE 4: Catch-All Global Safety Net ---
            dlq_router.route_to_dead_letter(
                domain="UNEXPECTED_SYSTEM_ERROR",
                error_message=str(global_err),
                original_payload=ticket if 'raw_ticket' in locals() else {"raw_message": str(message.value)},
                stack_trace=traceback.format_exc()
            )

if __name__ == "__main__":
    main()