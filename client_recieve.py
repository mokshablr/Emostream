from kafka import KafkaConsumer
import json

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'processed_emoji_topic'


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='emoji_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Subscribed to topic: {TOPIC}")
print("Waiting for messages...\n")

try:
    for message in consumer:
        print(f"Processed Emoji Data: {message.value}")
except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()

