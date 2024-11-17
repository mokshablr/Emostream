from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
from flask import Flask, request, jsonify
from threading import Thread
import signal
import sys

# Flask app setup
app = Flask(__name__)

# Kafka settings
KAFKA_BROKER = 'localhost:9092'
TOPICS = ['emoji_topic', 'processed_emoji_topic']

# Kafka Admin setup for topic creation
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
    'processed_emoji_topic',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id="emoji_client_group",
    auto_offset_reset='latest'
)

# API Endpoint to handle client requests
@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    try:
        data = request.json
        # Validate payload
        if not all(k in data for k in ('user_id', 'emoji_type', 'timestamp')):
            return jsonify({'error': 'Invalid data format'}), 400
        
        # Send message to emoji_topic
        producer.send('emoji_topic', data)
        return jsonify({'message': 'Data sent to emoji_topic'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint to consume processed emojis
@app.route('/processed_emojis', methods=['GET'])
def processed_emojis():
    messages = []
    for message in consumer:
        messages.append(message.value)
        if len(messages) >= 10:  # Return 10 messages at a time
            break
    return jsonify(messages), 200

# Producer flush loop (runs in a separate thread)
def flush_producer():
    global shutdown_flag
    while not shutdown_flag:
        producer.flush()
        time.sleep(0.5)

# Graceful shutdown for Kafka producer
shutdown_flag = False
def shutdown_handler(signal_received, frame):
    global shutdown_flag
    print("Shutting down gracefully...")
    shutdown_flag = True
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

    # Run Flask app
    app.run(port=5000)
