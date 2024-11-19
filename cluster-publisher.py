from kafka import KafkaConsumer, KafkaProducer
import json
import sys

KAFKA_BROKER = 'localhost:9092'
MAIN_PUBLISHER_TOPIC = 'main_publisher_topic'
CLUSTER_PUBLISHER_TOPIC = 'cluster_publisher_topic_{}'


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python cluster-publisher.py <cluster_id>")
        sys.exit(1)
    CLUSTER_ID = int(sys.argv[1])

    group_id = 'emoji_cluster_'+str(CLUSTER_ID)
    print(f"\n\n======== \t CLUSTER PUBLISHER {CLUSTER_ID} | GROUP_ID: {group_id} \t ========\n\n")

consumer = KafkaConsumer(
    MAIN_PUBLISHER_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group_id,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    data = message.value
    emoji_type = data.get('emoji_type')
    cluster_topic = CLUSTER_PUBLISHER_TOPIC.format(CLUSTER_ID)
    producer.send(cluster_topic, {'emoji_type': emoji_type})
    print(f"Cluster Publisher {CLUSTER_ID}: Sent {emoji_type}")