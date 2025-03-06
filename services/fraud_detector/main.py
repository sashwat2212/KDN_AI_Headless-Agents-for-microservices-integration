from kafka import KafkaConsumer, KafkaProducer
import json
from opentelemetry.trace import get_tracer
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from redis_deduplication.redis_client import RedisDeduplication  # ✅ Import Redis Deduplication

# Initialize Redis Deduplication
deduplication = RedisDeduplication()

# Initialize OpenTelemetry Tracing
tracer_provider = TracerProvider()
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4320")
tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
tracer = tracer_provider.get_tracer("fraud_detector")

# Kafka Configuration
KAFKA_BROKER = "localhost:9093"
INPUT_TOPIC = "validated_tax_data"
ALERT_TOPIC = "fraud_alerts"

# Fraud Detection Rules
SUSPICIOUS_AMOUNT_THRESHOLD = 10_000  # Transactions above this are flagged
BLACKLISTED_TAX_IDS = {"FRAUD123", "SCAM456"}  # Blacklisted tax IDs

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

print("✅ Kafka Consumer connected! Monitoring for fraud...")

def is_fraudulent(transaction):
    """ Check if transaction is potentially fraudulent """
    tax_id = transaction.get("tax_id")
    amount = transaction.get("amount")

    if tax_id in BLACKLISTED_TAX_IDS:
        print(f"🚨 Blacklisted Tax ID Detected: {transaction}")
        return True

    if amount >= SUSPICIOUS_AMOUNT_THRESHOLD:
        print(f"🚨 High-Value Transaction Detected: {transaction}")
        return True

    return False

# Process Messages with Redis Deduplication
for message in consumer:
    transaction = message.value
    print(f"📥 Received Transaction: {transaction}")

    # ✅ Check for duplicate processing
    if deduplication.is_duplicate(transaction):
        print(f"🚨 Duplicate fraud check ignored: {transaction}")
        continue

    if is_fraudulent(transaction):
        print(f"⚠️ Fraud Alert: {transaction}")
        producer.send(ALERT_TOPIC, transaction)  # Forward fraud alerts
        producer.flush()  # ✅ Ensure message is sent
        print(f"📨 Fraud Alert Sent to {ALERT_TOPIC}")
