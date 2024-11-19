from kafka import KafkaConsumer, KafkaProducer
import json

KAFKA_BROKER = 'localhost:9092'
AGGREGATED_TOPIC = 'processed_emoji_topic'
MAIN_PUBLISHER_TOPIC = 'main_publisher_topic'

print(f"\n\n======== \t MAIN PUBLISHER \t ========\n\n")

consumer = KafkaConsumer(
    AGGREGATED_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='emoji_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    aggregated_data = message.value
    producer.send(MAIN_PUBLISHER_TOPIC, aggregated_data)
    print(f"Main Publisher: Sent {aggregated_data}")
