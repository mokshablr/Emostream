# WHY THREADING? Need to concurrently send and receive emojis

import requests
import random
import time
import json
import threading
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError

EMOJIS = ['👏', '😂', '❤️', '😍', '😭', '😡', '👍', '👎']

SEND_EMOJI_ENDPOINT = "http://localhost/send_emoji"  # load balanced using nginx

KAFKA_BROKER = 'localhost:9092'
PROCESSED_TOPIC = 'processed_emoji_topic'

shutdown_flag = threading.Event()

consumer = KafkaConsumer(
    PROCESSED_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id="emoji_client_group",
    auto_offset_reset='latest'
)

def generate_emoji_data():
    user_id = random.randint(1, 100)
    emoji = random.choice(EMOJIS)
    timestamp = int(time.time())
    return {"user_id": user_id, "emoji_type": emoji, "timestamp": timestamp}

def send_emoji_data():
    while not shutdown_flag.is_set():
        emoji_data = generate_emoji_data()
        try:
            response = requests.post(SEND_EMOJI_ENDPOINT, json=emoji_data)
            if response.status_code == 200:
                print(f"Successfully sent: {emoji_data}")
            else:
                error_message = response.json().get('error', 'Unknown error')
                print(f"Failed to send: {emoji_data}, Status Code: {response.status_code}, response: {error_message}")
        except Exception as e:
            print(f"Error sending data: {e}")
        time.sleep(random.uniform(0.01, 0.05))  # Send data every 10-50ms

def consume_processed_emojis():
    """Fetch processed emojis directly from Kafka."""
    try:
        for message in consumer:
            if shutdown_flag.is_set():
                break
            print("Processed Emojis:", message.value)
    except KafkaError as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()  # Ensure the consumer is closed on shutdown

# def signal_handler(sig, frame):
#     print("Shutdown signal received")
#     shutdown_flag.set()

if __name__ == "__main__":
    # Register the signal handler for graceful shutdown
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)

    # Create threads for sending emoji data and consuming processed emojis
    send_thread = threading.Thread(target=send_emoji_data)
    consume_thread = threading.Thread(target=consume_processed_emojis)

    # Start both threads
    send_thread.start()
    consume_thread.start()

    # Join threads to the main thread to keep the script running
    send_thread.join()
    consume_thread.join()
