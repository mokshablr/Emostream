import requests
import random
import threading
import time
from datetime import datetime

# API endpoint URL
API_ENDPOINT = "http://localhost:5000/send_emoji"

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
            response = requests.post(API_ENDPOINT, json=data)
            if response.status_code == 200:
                print(f"Successfully sent: {data}")
            else:
                print(f"Failed to send: {data} with status code {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Request error from user_{user_id}: {e}")
        time.sleep(random.uniform(0.01, 0.05))  # Send data every 10-50ms

# Start multiple threads (clients)
threads = []
for user_id in range(1, 100):  # 100 clients
    thread = threading.Thread(target=send_emoji, args=(user_id,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

