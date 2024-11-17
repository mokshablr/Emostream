

import requests
import random
import threading
import time
from datetime import datetime

# API endpoint URLs
SEND_EMOJI_ENDPOINT = "http://localhost:5000/send_emoji"
PROCESSED_EMOJIS_ENDPOINT = "http://localhost:5000/processed_emojis"

# List of sample emojis
EMOJIS = ['👏', '😂', '❤️', '😍', '😭', '😡', '👍', '👎']

def send_emoji(user_id):
    """Simulate a client sending emoji data."""
    while True:
        data = {
            "user_id": f"user_{user_id}",
            "emoji_type": random.choice(EMOJIS),
            "timestamp": datetime.now().isoformat()
        }
        try:
            response = requests.post(SEND_EMOJI_ENDPOINT, json=data)
            if response.status_code == 200:
                print(f"Successfully sent: {data}")
            else:
                print(f"Failed to send: {data} with status code {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Request error from user_{user_id}: {e}")
        time.sleep(random.uniform(0.01, 0.05))  # Send data every 10-50ms

def consume_processed_emojis():
    """Fetch processed emojis from the Flask server."""
    while True:
        try:
            response = requests.get(PROCESSED_EMOJIS_ENDPOINT)
            if response.status_code == 200:
                print("Processed Emojis:", response.json())
            else:
                print("Failed to fetch processed emojis")
        except requests.exceptions.RequestException as e:
            print(f"Request error while fetching processed emojis: {e}")
        time.sleep(1)

# Start multiple threads (clients)
threads = []
for user_id in range(1, 100):  # Reduced to 5 clients for readability
    thread = threading.Thread(target=send_emoji, args=(user_id,))
    threads.append(thread)
    thread.start()

# Start the processed emoji consumer
consumer_thread = threading.Thread(target=consume_processed_emojis, daemon=True)
consumer_thread.start()

for thread in threads:
    thread.join()

