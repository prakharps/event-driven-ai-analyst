import time
import logging
from config.settings import AppSettings

logger = logging.getLogger("AIProcessingEngine.DLQRouter")

class DLQRouter:
    def __init__(self, kafka_producer):
        """Pass the active, thread-safe producer from the orchestrator."""
        self.producer = kafka_producer
        self.dlq_topic = AppSettings.TOPIC_DLQ

    def route_to_dead_letter(self, domain: str, error_message: str, original_payload: dict, context: dict = None, stack_trace: str = None):
        """
        Standardizes the error schema across the enterprise application 
        and safely pushes the poison pill out of the main execution thread.
        """
        # Construct a unified, scannable data contract for debugging
        dlq_payload = {
            "error_domain": domain,
            "error_message": error_message,
            "failed_at_timestamp": time.time(),
            "original_payload": original_payload
        }
        
        # Optionally append debugging data if it exists
        if context:
            dlq_payload["enrichment_context"] = context
        if stack_trace:
            dlq_payload["stack_trace"] = stack_trace

        ticket_id = original_payload.get("ticket_id", "UNKNOWN") if isinstance(original_payload, dict) else "UNPARSABLE"

        try:
            logger.error(f"[DLQ ROUTER] Routing poison event {ticket_id} to '{self.dlq_topic}' due to {domain}.")
            
            # Send synchronously to guarantee the message is persisted before continuing the loop
            self.producer.send(self.dlq_topic, value=dlq_payload).get(timeout=10)
            
            logger.info(f"[DLQ ROUTER] Event {ticket_id} successfully isolated.")
        except Exception as kafka_err:
            logger.critical(f"[DLQ CRITICAL SYSTEM FAILURE] Could not write to DLQ topic! Error: {kafka_err}")