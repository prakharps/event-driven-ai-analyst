# Autonomous AI Incident Resolution Engine & Real-Time Telemetry Plane

An enterprise-grade, event-driven microservices architecture that ingests system incidents, runs real-time Retrieval-Augmented Generation (RAG) to determine mitigation strategies, commits learnings to a vector space, and streams live telemetry to an operations dashboard.

---

## 🏗️ System Architecture Topology

The platform consists of five decoupled services coordinating over an internal Docker network bridge via a high-throughput event bus:

1. **Ingestion Service**: Simulates/Ingests incoming raw system failure events.
2. **Apache Kafka Cluster**: Manages immutable log partitions (`incoming-tickets` and `resolved-tickets`).
3. **AI Processing Engine**: Asynchronous worker pool utilizing LangChain/OpenAI to run RAG analytics.
4. **Data Layers**: 
   - **MySQL**: Relational ledger tracking core incident configurations and historical metrics.
   - **ChromaDB**: High-performance vector database housing long-term cognitive semantic memory.
5. **Live Dashboard (FastAPI)**: Non-blocking service utilizing `aiokafka` background workers and bi-directional **WebSockets** to stream live insights to the client UI.

## 🛠️ Tech Stack & Patterns

- **Backend Framework**: FastAPI (Asynchronous Concurrency Loop)
- **Event Streaming**: Apache Kafka / Zookeeper (`aiokafka` client wrapper)
- **AI/LLM Stack**: OpenAI GPT-4 API / Embedding Engine
- **Vector Infrastructure**: ChromaDB (Semantic Memory Embedding Cache)
- **Database**: MySQL 8.0 (Relational Operational Store)
- **Frontend UI**: Native HTML5 WebSockets, Tailwind CSS, Chart.js (Zero-Framework overhead)
- **Containerization**: Docker / Docker Compose (Multi-stage cross-platform build pipelines)

---

## 🚀 Quick Start / Deployment

### Prerequisites
Ensure your host machine has Docker and Docker Compose installed.

### 1. Configure the Environment
Create a `deploy/.env` file and populate your private integration configurations:
```env
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
DB_HOST=mysql-db
DB_USER=analyst_user
DB_PASSWORD=analyst_password
DB_NAME=business_db
OPENAI_API_KEY=your_openai_api_key_here