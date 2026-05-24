import os
import json
import asyncio
import logging
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("LiveDashboard.Backend")

app = FastAPI(title="AI Incident Engine Operations Center")
templates = Jinja2Templates(directory="src/templates")

# Configuration Environment Mapping
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_RESOLVED = 'resolved-tickets'
CONSUMER_GROUP = 'live-dashboard-group'

# Maintain a thread-safe active state pool of connected browser clients
active_connections: Set[WebSocket] = set()

async def broadcast_to_browsers(message: str):
    """Iterates over all active socket descriptors to push a payload down the wire."""
    if not active_connections:
        return
        
    # Gather all socket writes concurrently to avoid head-of-line blocking
    disconnected_clients = []
    for connection in active_connections:
        try:
            await connection.send_text(message)
        except Exception:
            disconnected_clients.append(connection)
            
    # Clean up stale connections
    for client in disconnected_clients:
        active_connections.remove(client)

async def kafka_stream_listener():
    """Asynchronous background worker task that consumes processed event logs."""
    logger.info(f"Initializing async Kafka pipeline for broker: {KAFKA_BROKER}")
    
    # Establish non-blocking stream hook
    consumer = AIOKafkaConsumer(
        TOPIC_RESOLVED,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    # Simple retry handshake loop for cluster readiness
    while True:
        try:
            await consumer.start()
            logger.info(f"Successfully bound stream connection to topic: '{TOPIC_RESOLVED}'")
            break
        except Exception as e:
            logger.warning(f"Kafka cluster connection unready. Retrying in 3 seconds... Details: {e}")
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            ticket_event = msg.value
            logger.info(f"Intercepted resolved ticket artifact down stream: {ticket_event.get('ticket_id')}")
            
            # Stringify raw json data and push it into the WebSocket broker pool
            await broadcast_to_browsers(json.dumps(ticket_event))
    except Exception as stream_err:
        logger.error(f"Critical breakdown in background event listener thread: {stream_err}")
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    """Fires up the asynchronous background consumer thread during container init."""
    asyncio.create_task(kafka_stream_listener())

@app.get("/")
async def serve_dashboard_ui(request: Request):
    """Serves the foundational single-page telemetry app."""
    return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/ws/metrics")
async def websocket_endpoint(websocket: WebSocket):
    """Handles open handshake connections and manages the client tracking registry."""
    await websocket.accept()
    active_connections.add(websocket)
    logger.info(f"New client socket bound. Total active observers: {len(active_connections)}")
    
    try:
        # Keep connection open waiting for client closures
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)
        logger.info(f"Client disconnected gracefully. Remaining active connections: {len(active_connections)}")
    except Exception as e:
        if websocket in active_connections:
            active_connections.remove(websocket)
        logger.error(f"Encountered unexpected network drop on observer socket descriptor: {e}")