from kafka import KafkaConsumer
import requests
import json
import sys

KAFKA_BROKER = 'localhost:9092'
CLUSTER_PUBLISHER_TOPIC = 'cluster_publisher_topic_{}'
FLASK_ENDPOINT = 'http://localhost/emoji_update/{}'

def listen_to_cluster(cluster_id):
    consumer = KafkaConsumer(
        CLUSTER_PUBLISHER_TOPIC.format(cluster_id),
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id=group_id
    )
    for message in consumer:
        print(f"Received from Cluster Publisher {cluster_id}: {message.value}. Sending to {group_id}")
        send_to_flask_endpoint(group_id, message.value)


def send_to_flask_endpoint(group_id, data):
    url = FLASK_ENDPOINT.format(group_id)
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            print(f"Successfully sent data to Flask endpoint for group {group_id}")
        else:
            print(f"Failed to send data to Flask endpoint. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending data to Flask endpoint: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python client.py <cluster_id> <sub_id>")
        sys.exit(1)

    cluster_id = int(sys.argv[1])
    sub_id = int(sys.argv[2])
    group_id = 'emoji_cluster_'+str(cluster_id)+'_sub_'+str(sub_id)
    print(f"\n\n======== \t CLUSTER {cluster_id} | SUB {sub_id} | GROUP {group_id} \t ========\n\n")
    listen_to_cluster(cluster_id)
