from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import json
import time
from flask import Flask, request, jsonify
from threading import Thread
import threading
import signal
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

def send_to_kafka(data):
    """Send emoji data to Kafka asynchronously."""
    try:
        producer.send(TOPICS[0], data)
    except KafkaError as e:
        logging.error(f"Failed to send data to Kafka: {e}")

# Kafka settings
KAFKA_BROKER = 'localhost:9092'
TOPICS = ['emoji_topic', 'processed_emoji_topic']

# Kafka Admin setup for topic creation
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

# Create the topics used by architecture
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

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

# API Endpoint to handle client requests
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

@app.route('/emoji')
def emoji():
    return "<p>Send emojis to /send_emoji</p>"

# Producer flush loop (runs in a separate thread)
def flush_producer():
    while not shutdown_flag.is_set():
        producer.flush()
        time.sleep(0.5)

# Graceful shutdown for Kafka producer
shutdown_flag = threading.Event()
def shutdown_handler(signal_received, frame):
    global shutdown_flag
    logging.info("Shutting down gracefully...")
    shutdown_flag.set()
    producer.flush()
    producer.close()
    admin_client.close()
    sys.exit(0)

# Attach signal handlers for cleanup on termination
signal.signal(signal.SIGINT, shutdown_handler)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, shutdown_handler)  # Handle termination signals

if __name__ == "__main__":
    # Create topics on startup
    create_topics(TOPICS)

    # Start the flush thread
    flush_thread = Thread(target=flush_producer, daemon=True)
    flush_thread.start()

    app.run(debug=True, host='0.0.0.0')
