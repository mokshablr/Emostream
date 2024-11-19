from redis import Redis
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from flask_socketio import SocketIO, emit, join_room, leave_room, send
import json
import time
from flask import Flask, request, jsonify
from threading import Thread
import threading
import signal
import sys
import logging
import eventlet

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
socketio = SocketIO(app, async_mode='eventlet')

redis_client = Redis(host='localhost', port=6379, db=0)

def send_to_kafka(data):
    """Send emoji data to Kafka asynchronously."""
    try:
        producer.send(TOPICS[0], data)
    except KafkaError as e:
        logging.error(f"Failed to send data to Kafka: {e}")

KAFKA_BROKER = 'localhost:9092'
TOPICS = ['emoji_topic', 'processed_emoji_topic', 'main_publisher_topic', 'cluster_publisher_topic_1', 'cluster_publisher_topic_2', 'cluster_publisher_topic_3']

admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

def create_topics(topics):
    try:
        existing_topics = set(admin_client.list_topics())
        new_topics = [
            NewTopic(name=topic, num_partitions=1, replication_factor=1)
            for topic in topics if topic not in existing_topics
        ]
        if new_topics:
            admin_client.create_topics(new_topics=new_topics)
            logging.info(f"Topics created: {[topic.name for topic in new_topics]}")
        else:
            logging.info("All topics already exist.")
    except Exception as e:
        logging.error(f"Error creating topics: {e}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    """API endpoint to accept emoji data."""
    data = request.get_json()
    user_id = data.get('user_id')
    emoji_type = data.get('emoji_type')
    timestamp = data.get('timestamp')
    if not user_id or not emoji_type or not timestamp:
        return jsonify({"error": "Invalid data"}), 400
    emoji_data = {
        "user_id": user_id,
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }
    threading.Thread(target=send_to_kafka, args=(emoji_data,)).start()
    return jsonify({"status": "Emoji data received"}), 200

def flush_producer():
    while not shutdown_flag.is_set():
        producer.flush()
        time.sleep(0.5)


@app.route('/emoji_update/<int:cluster_id>', methods=['POST'])
def publish_to_cluster(cluster_id):
    message = request.get_json()
    subscribers = redis_client.hgetall(f'cluster:{cluster_id}')
    logging.info(f"checking clients {subscribers} and {subscribers.items}")
    for user_id, client_info in subscribers.items():
        client_info = json.loads(client_info)
        logging.info(f"Sending message to client {client_info['user_id']} in room {client_info['cluster_id']}")
        send_message_to_client(client_info['cluster_id'], message)
    return jsonify({'status': 'Message sent to cluster'}), 200


def send_message_to_client(cluster_id, message):
    """Send message to client using WebSockets."""
    try:
        print("EMOJI UPDATE: ", message)
        logging.info("EMOJI Update", message)
        socketio.emit('emoji_update', message, room=cluster_id)
    except Exception as e:
        logging.error(f"Error sending message to cluster {cluster_id}: {e}")

pending_subscriptions = {}


@app.route('/register', methods=['POST'])
def register_subscriber():
    data = request.get_json()
    user_id = data.get('user_id')
    cluster_id = data.get('cluster_id')
    
    if not user_id or not cluster_id:
        return jsonify({"error": "Invalid data"}), 400

    redis_client.hset(f'cluster:{cluster_id}', user_id, json.dumps({'user_id':user_id, 'cluster_id': cluster_id}))

    pending_subscriptions[user_id] = cluster_id

    return jsonify({"status": "Client registered successfully"}), 200

@app.route('/unregister', methods=['POST'])
def unregister_subscriber():
    data = request.get_json()
    user_id = data.get('user_id')
    cluster_id = data.get('cluster_id')
    if not user_id or not cluster_id:
        return jsonify({"error": "Invalid data"}), 400

    redis_client.hdel(f'cluster:{cluster_id}', user_id)

    socketio.emit('unsubscribe', {'cluster_id': cluster_id}, room=cluster_id)
    return jsonify({"status": "Client unregistered successfully"}), 200

@socketio.on('connect')
def handle_connect():
    user_id = request.args.get('user_id')
    if user_id in pending_subscriptions:
        cluster_id = pending_subscriptions.pop(user_id)
        join_room(cluster_id)
        socketio.emit('subscribe', {'cluster_id': cluster_id, 'user_id': user_id}, room=cluster_id)
        print(f'User {user_id} has joined cluster {cluster_id}')


@app.route('/emoji')
def emoji():
    return "<p>Send emojis to /send_emoji</p>"

shutdown_flag = threading.Event()
def shutdown_handler(signal_received, frame):
    global shutdown_flag
    logging.info("Shutting down gracefully...")
    shutdown_flag.set()
    producer.flush()
    producer.close()
    admin_client.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

if __name__ == "__main__":

    create_topics(TOPICS)

    flush_thread = Thread(target=flush_producer, daemon=True)
    flush_thread.start()

    socketio.run(app)
