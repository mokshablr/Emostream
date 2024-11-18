from locust import HttpUser, task, between
import time

class EmojiUser(HttpUser):
    wait_time = between(0.001, 0.001)  # To simulate high requests per second

    @task
    def send_emoji(self):
        # Realtime timestamp
        timestamp = int(time.time())  # Get the current timestamp in seconds
        self.client.post("/send_emoji", json={
            "user_id": 1,
            "emoji_type": "❤️",
            "timestamp": timestamp
        })

