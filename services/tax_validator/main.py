from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json
import logging
import signal
import sys
import time
import sys
import os
# OpenTelemetry Imports
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import get_tracer_provider, set_tracer_provider
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from redis_deduplication.redis_client import RedisDeduplication

deduplication = RedisDeduplication()

# Kafka Configuration
KAFKA_BROKER = "localhost:9093"
INPUT_TOPIC = "audit_data"  # Listening for incoming data
OUTPUT_TOPIC = "validated_tax_data"  # Forward valid messages

# Required fields for validation
REQUIRED_FIELDS = ["tax_id", "amount", "currency"]

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# OpenTelemetry Tracing Setup
tracer_provider = TracerProvider()
set_tracer_provider(tracer_provider)

otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4320")
tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

tracer = tracer_provider.get_tracer("tax_validator")

# Create Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

logger.info("✅ Kafka Consumer connected! Listening for messages...")

def is_valid_tax_data(data):
    """ Validate tax data structure """
    with tracer.start_as_current_span("validate_tax_data"):
        # Check if all required fields are present
        for field in REQUIRED_FIELDS:
            if field not in data:
                logger.warning(f"❌ Invalid Data: Missing field {field} → {data}")
                return False

        # Validate data types
        if not isinstance(data["tax_id"], str) or not data["tax_id"]:
            logger.warning(f"❌ Invalid tax_id format → {data}")
            return False

        if not isinstance(data["amount"], (int, float)) or data["amount"] <= 0:
            logger.warning(f"❌ Invalid amount value → {data}")
            return False

        if not isinstance(data["currency"], str) or len(data["currency"]) != 3:
            logger.warning(f"❌ Invalid currency code → {data}")
            return False

        return True

def process_messages():
    """ Continuously process messages from Kafka """
    for message in consumer:
        data = message.value
        logger.info(f"📥 Received: {data}")

        with tracer.start_as_current_span("process_tax_message"):
            if deduplication.is_duplicate(data):
                logger.warning(f"🚨 Duplicate tax data ignored: {data}")
                continue
            if is_valid_tax_data(data):
                logger.info(f"✅ Valid Data: {data}")
                try:
                    producer.send(OUTPUT_TOPIC, data)
                    producer.flush()
                    logger.info(f"📨 Forwarded to {OUTPUT_TOPIC}")
                except KafkaError as e:
                    logger.error(f"❌ Kafka Producer Error: {e}")
            else:
                logger.warning(f"⚠️ Dropping invalid data: {data}")

def shutdown_handler(signal_received, frame):
    """ Graceful shutdown """
    logger.info("🔻 Shutting down Kafka consumer & producer...")
    consumer.close()
    producer.close()
    sys.exit(0)

# Register shutdown handlers
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

if __name__ == "__main__":
    try:
        process_messages()
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
