import json

from kafka import KafkaConsumer

from settings import ENRICHED_TOPIC, KAFKA_BOOTSTRAP_SERVERS

consumer = KafkaConsumer(
    ENRICHED_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
)

print(f"Reading '{ENRICHED_TOPIC}'...")
count = 0
for record in consumer:
    count += 1
    print(json.dumps(json.loads(record.value), ensure_ascii=False))

print(f"{count} messages")
consumer.close()
