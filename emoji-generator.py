import requests
import random
import time
from datetime import datetime

# API endpoint URL
API_URL = 'http://localhost:5000/send_emoji'

# List of sample emojis and user IDs
EMOJIS = ['👏', '😂', '❤️', '😍', '😭', '😡', '👍', '👎']
USER_IDS = [f'user_{i}' for i in range(1, 101)]

def generate_emoji_data():
    user_id = random.choice(USER_IDS)
    emoji = random.choice(EMOJIS)
    timestamp = datetime.utcnow().isoformat()
    return {
        'user_id': user_id,
        'emoji_type': emoji,
        'timestamp': timestamp
    }

def send_emoji_data():
    for _ in range(1000):  
        emoji_data = generate_emoji_data()
        response = requests.post(API_URL, json=emoji_data)
        if response.status_code == 200:
            print(f"Successfully sent: {emoji_data}")
        else:
            print(f"Failed to send: {emoji_data} with status code {response.status_code}")
        time.sleep(0.01)  # Small delay to simulate real-time sending

if __name__ == '__main__':
    send_emoji_data()

