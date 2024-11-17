import requests
import random
import time
import json

def generate_emoji_data():
    emojis = ['👏', '😂', '❤️', '😍', '😭', '😡', '👍', '👎']
    user_id = random.randint(1, 100)
    emoji = random.choice(emojis)
    timestamp = int(time.time())
    return {"user_id": user_id, "emoji_type": emoji, "timestamp": timestamp}

API_URL = "http://localhost/send_emoji"  # load balanced using nginx

def send_emoji_data():
    while True:
        emoji_data = generate_emoji_data()
        try:
            response = requests.post(API_URL, json=emoji_data)
            if response.status_code == 200:
                print(f"Successfully sent: {emoji_data}")
            else:
                error_message = response.json().get('error', 'Unknown error')
                print(f"Failed to send: {emoji_data}, Status Code: {response.status_code}, response: {error_message}")

        except Exception as e:
            print(f"Error sending data: {e}")

if __name__ == "__main__":
    send_emoji_data()
