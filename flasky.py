from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import json
import time
from flask import Flask, request, jsonify
from threading import Thread
import threading
import signal
import sys
import queue

app = Flask(__name__)

def send_to_kafka(data):
    """Send emoji data to Kafka asynchronously."""
    try:
        producer.send(TOPICS[0], data)
    except KafkaError as e:
        print(f"Failed to send data to Kafka: {e}")

# Flask app setup
app = Flask(__name__)

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
            print(f"Topics created: {[topic.name for topic in new_topics]}")
        else:
            print("All topics already exist.")
    except Exception as e:
        print(f"Error creating topics: {e}")

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

# Kafka Consumer setup
consumer = KafkaConsumer(
    TOPICS[1],
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id="emoji_client_group",
    auto_offset_reset='latest'
)

# Queue to hold processed messages
processed_emojis_queue = queue.Queue()

# Function to consume messages and put them in the queue
def consume_messages():
    while not shutdown_flag.is_set():
        for message in consumer:
            processed_emojis_queue.put(message.value)

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
    return f"<p>Send emojis to /send_emoji</p>"

# Endpoint to consume processed emojis
@app.route('/processed_emojis', methods=['GET'])
def processed_emojis():
    messages = []
    while not processed_emojis_queue.empty() and len(messages) < 10:
        messages.append(processed_emojis_queue.get())
    return jsonify(messages), 200

# Producer flush loop (runs in a separate thread)
def flush_producer():
    global shutdown_flag
    while not shutdown_flag.is_set():
        producer.flush()
        time.sleep(0.5)

# Graceful shutdown for Kafka producer
shutdown_flag = threading.Event()
def shutdown_handler(signal_received, frame):
    global shutdown_flag
    print("Shutting down gracefully...")
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

    # Start the consumer thread
    consumer_thread = Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    app.run(debug=True, host='0.0.0.0')
