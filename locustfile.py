from locust import HttpUser, task, between
import time

class EmojiUser(HttpUser):
    wait_time = between(0.001, 0.001)

    @task
    def send_emoji(self):
        # Realtime timestamp
        timestamp = int(time.time())
        self.client.post("/send_emoji", json={
            "user_id": 1,
            "emoji_type": "❤️",
            "timestamp": timestamp
        })

