from fastapi import FastAPI
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError
import json
import os
import time
import uvicorn
import logging
import sys
# OpenTelemetry Imports
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import get_tracer_provider, set_tracer_provider
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from redis_deduplication.redis_client import RedisDeduplication

deduplication = RedisDeduplication()

# Initialize FastAPI
app = FastAPI()

# Kafka Configuration
KAFKA_BROKER = "localhost:9093"
TOPIC_NAME = "audit_data"

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenTelemetry Tracing Setup
tracer_provider = TracerProvider()
set_tracer_provider(tracer_provider)

otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4320")
tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

tracer = tracer_provider.get_tracer("audit_data_collector")

# Function to check Kafka availability before connecting
def wait_for_kafka():
    for _ in range(10):  
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            logger.info("Connected to Kafka")
            admin_client.close()
            return True
        except KafkaError:
            logger.warning("⚠️ Kafka not ready, retrying in 5 seconds...")
            time.sleep(5)
    logger.error("❌ Kafka connection failed.")
    return False

# Wait for Kafka before starting producer
if wait_for_kafka():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 8, 1)
    )
else:
    raise Exception("Kafka is not available!")

@app.post("/send")
async def send_message(data: dict):
    """API Endpoint to send messages to Kafka with tracing."""
    with tracer.start_as_current_span("send_to_kafka"):
        if deduplication.is_duplicate(data):
            return {"error": "Duplicate message detected"}
        try:
            producer.send(TOPIC_NAME, data)
            producer.flush()
            logger.info(f"📨 Message sent: {data}")
            return {"message": "Sent successfully", "data": data}
        except KafkaError as e:
            logger.error(f"❌ Error sending message: {str(e)}")
            return {"error": "Failed to send message"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)