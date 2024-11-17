from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import threading

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  
    batch_size=16384,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    linger_ms=500
)

KAFKA_TOPIC = "emoji_topic"

def send_to_kafka(data):
    """Send emoji data to Kafka asynchronously."""
    try:
        producer.send(KAFKA_TOPIC, data)
    except KafkaError as e:
        print(f"Failed to send data to Kafka: {e}")

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
    return f"<p>You are visiting the endpoint 1</p>"


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
