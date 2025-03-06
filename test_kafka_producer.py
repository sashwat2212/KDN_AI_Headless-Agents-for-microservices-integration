from kafka import KafkaProducer
import json
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Define test transactions with expected fraud cases
test_transactions = [
    {"tax_id": "USER002", "amount": 20000},   # 🚨 Fraud Alert: High-Value Transaction
    {"tax_id": "FRAUD123", "amount": 5000},   # 🚨 Fraud Alert: Blacklisted Tax ID
    {"tax_id": "USER002", "amount": 2000},    # ✅ Transaction is Clean
    {"tax_id": "USER003", "amount": 9999},    # ✅ Transaction is Clean (Borderline Case)
]

# Send each transaction
for transaction in test_transactions:
    producer.send("validated_tax_data", transaction)
    print(f"✅ Sent transaction: {transaction}")
    time.sleep(0.5)  # Optional: Add delay

# Suspicious Pattern (Repeated Transactions)
suspicious_transaction = {"tax_id": "USER004", "amount": 9500}
print("🚨 Sending Suspicious Repeated Transactions...")
for _ in range(5):
    producer.send("validated_tax_data", suspicious_transaction)
    time.sleep(0.2)  # Small delay to simulate real transactions

# Ensure all messages are sent
producer.flush()
print("✅ All test transactions sent successfully!")
