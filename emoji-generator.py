# WHY THREADING? Need to concurrently send and recieve emojis

import requests
import random
import time
import json
import threading
from datetime import datetime
import signal
import sys

EMOJIS = ['👏', '😂', '❤️', '😍', '😭', '😡', '👍', '👎']

SEND_EMOJI_ENDPOINT = "http://localhost/send_emoji"  # load balanced using nginx
PROCESSED_EMOJIS_ENDPOINT = "http://localhost/processed_emojis"

shutdown_flag = threading.Event()

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
    """Fetch processed emojis from the Flask server."""
    while not shutdown_flag.is_set():
        try:
            response = requests.get(PROCESSED_EMOJIS_ENDPOINT)
            if response.status_code == 200:
                print("Processed Emojis:", response.json())
            else:
                print("Failed to fetch processed emojis")
        except requests.exceptions.RequestException as e:
            print(f"Request error while fetching processed emojis: {e}")
        time.sleep(1)

def signal_handler(sig, frame):
    print("Shutdown signal received")
    shutdown_flag.set()

if __name__ == "__main__":
    # Register the signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create threads for sending emoji data and consuming processed emojis
    send_thread = threading.Thread(target=send_emoji_data)
    consume_thread = threading.Thread(target=consume_processed_emojis)

    # Start both threads
    send_thread.start()
    consume_thread.start()

    # Join threads to the main thread to keep the script running
    send_thread.join()
    consume_thread.join()

    print("All threads have been stopped")
