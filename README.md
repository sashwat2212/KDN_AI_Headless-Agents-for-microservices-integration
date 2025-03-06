# Microservices Integration for Optimized Workflow Orchestration

## Overview
This project implements an **event-driven microservices architecture** for handling **tax and audit operations** using **Kafka** for message brokering and **Redis** for message deduplication. Each microservice operates independently, publishing and consuming events asynchronously to optimize workflow orchestration.

## Key Features
- **Microservices Architecture**: Modular services for audit data collection, fraud detection, and tax validation.
- **Event-Driven Communication**: Utilizes **Apache Kafka** for **asynchronous messaging**.
- **Message Deduplication**: Implements **Redis-based deduplication** to prevent duplicate event processing.
- **Distributed Tracing**: Tracks event flow using **OpenTelemetry/Jaeger**.
- **Dockerized Deployment**: Uses **Docker Compose** for seamless service orchestration.
- **Monitoring**: A dedicated monitoring service for tracking event processing and performance.

---

## Project Structure
```
MICROSERVICES-INTEGRATION/
│── redis_deduplication/          # Redis-based message deduplication logic
│── services/
│   │── audit_data_collector/     # Kafka producer for collecting audit data
│   │   ├── main.py               
│   │── fraud_detector/           # Kafka consumer for detecting fraud
│   │   ├── main.py               
│   │── tax_validator/            # Kafka consumer for validating tax data
│   │   ├── main.py               
│── venv/                         # Virtual environment (if using local Python setup)
│── docker-compose.yml            # Docker Compose for Kafka, Redis, and services
│── monitoring.py                 # Monitoring and logging service
│── test_kafka_producer.py        # Test script for Kafka producer
│── README.md                     # Documentation
```

---

## Prerequisites
Ensure you have the following installed:
- **Docker & Docker Compose**
- **Python 3.x** (if running services locally)
- **Apache Kafka**
- **Redis**

---

## Setup & Installation
### 1. Clone the Repository
```sh
git clone https://github.com/your-repo/microservices-integration.git
cd microservices-integration
```

### 2. Start Kafka, Redis, and Services
```sh
docker-compose up -d
```
This will start:
- Kafka (port **9093**)
- Zookeeper
- Redis (for deduplication)
- Microservices (audit, fraud, tax validation)

### 3. Run the Services Locally (Optional)
If not using Docker, activate the virtual environment and start services manually:
```sh
python services/audit_data_collector/main.py
python services/fraud_detector/main.py
python services/tax_validator/main.py
```

### 4. Test Kafka Producer
To send a test message to Kafka:
```sh
python test_kafka_producer.py
```

---

## How It Works
### 1. **Audit Data Collection**
- Publishes audit data as Kafka events.
- Includes a **unique message ID** to prevent duplicates.

### 2. **Fraud Detector**
- Subscribes to audit data events.
- Detects anomalies using predefined rules.

### 3. **Tax Validator**
- Listens to audit events.
- Validates tax data against standard rules.

### 4. **Message Deduplication**
- Uses **Redis** to check for duplicate messages before processing.
- Ensures **idempotency** in event-driven workflows.

### 5. **Monitoring**
- Logs service health and event processing status.

---

## Future Enhancements
- Implement **gRPC** for inter-service communication.
- Add **real-time dashboards** for monitoring Kafka event flows.
- Enhance **AI-driven fraud detection** using ML models.

---

## Contributors
- **Sashwat Sinha**
- Feel free to contribute by submitting pull requests!

