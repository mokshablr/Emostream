from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import threading
import time

# Initialize Flask app
app = Flask(__name__)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Replace with your Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500  # Set flush interval for batching messages (0.5 seconds)
)

# Define the Kafka topic
KAFKA_TOPIC = 'emoji-events'

def send_to_kafka(data):
    """Function to send data to Kafka asynchronously."""
    try:
        # Send the data to Kafka topic
        future = producer.send(KAFKA_TOPIC, data)
        
        # Optional: handle success or failure
        future.add_callback(on_send_success).add_errback(on_send_error)
        
    except KafkaError as e:
        print(f"Failed to send data to Kafka: {e}")

def on_send_success(record_metadata):
    print(f"Data sent to Kafka successfully: {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")

def on_send_error(excp):
    print(f"Error sending data to Kafka: {excp}")

@app.route('/send-emoji', methods=['POST'])
def send_emoji():
    # Get JSON data from the request
    data = request.get_json()
    user_id = data.get('user_id')
    emoji_type = data.get('emoji_type')
    timestamp = data.get('timestamp')
    
    if not user_id or not emoji_type or not timestamp:
        return jsonify({"error": "Invalid data"}), 400

    # Prepare the data to send to Kafka
    emoji_data = {
        "user_id": user_id,
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }

    # Send the data to Kafka asynchronously
    threading.Thread(target=send_to_kafka, args=(emoji_data,)).start()
    
    return jsonify({"status": "Emoji data received"}), 200

# Run the Flask application
if __name__ == '__main__':
    app.run(debug=True, port=5000)
