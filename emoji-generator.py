import requests
import random
import threading
import time
from datetime import datetime
EMOJIS = ['👏', '😂', '❤️', '😍', '😭', '😡', '👍', '👎']
API_ENDPOINTS = [
    {"url": "http://localhost:5000/send_emoji", "server_id": "server_1"},
    {"url": "http://localhost:5001/send_emoji", "server_id": "server_2"}
]
def send_emoji(user_id):
    """Simulate a client sending emoji data to both Flask servers."""
    while True:
        data = {
            "user_id": f"user_{user_id}",
            "emoji_type": random.choice(EMOJIS),
            "timestamp": datetime.now().isoformat()
        }
        for api_endpoint in API_ENDPOINTS:
            data_with_server = data.copy()  
            data_with_server["server_id"] = api_endpoint["server_id"]  

            try:
                response = requests.post(api_endpoint["url"], json=data_with_server)
                if response.status_code == 200:
                    print(f"Successfully sent to {api_endpoint['server_id']}: {data_with_server}")
                else:
                    print(f"Failed to send to {api_endpoint['server_id']}: {data_with_server} with status code {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Request error from user_{user_id} to {api_endpoint['server_id']}: {e}")
        time.sleep(random.uniform(0.01, 0.05))  
threads = []
for user_id in range(1, 100):  
    thread = threading.Thread(target=send_emoji, args=(user_id,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

