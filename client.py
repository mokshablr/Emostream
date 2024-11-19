import socketio
import requests
import json
import time
import sys

FLASK_SERVER_URL = 'http://localhost'
REGISTER_ENDPOINT = f'{FLASK_SERVER_URL}/register'
UNREGISTER_ENDPOINT = f'{FLASK_SERVER_URL}/unregister'
SEND_EMOJI_ENDPOINT = f'{FLASK_SERVER_URL}/send_emoji'

sio = socketio.Client()

@sio.event
def connect():
    print("Connected to the server")
    sio.emit('subscribe', {'group_id': group_id})
    sio.emit('join', {'sid': sio.sid})
    print("Joined room")

@sio.event
def disconnect():
    print("Disconnected from the server")

@sio.event
def emoji_update(data):
    print(f"Received emoji update: {data}")

@sio.event
def subscribe(data):
    print(f"New subscription: User {data['user_id']} has joined cluster {data['group_id']}")


def register_to_cluster(user_id, group_id):
    """Send a request to register the client to a specific cluster."""
    data = {
        'user_id': user_id,
        'group_id': group_id,
    }
    response = requests.post(REGISTER_ENDPOINT, json=data)
    if response.status_code == 200:
        print(f"Client {user_id} registered to cluster {group_id} successfully.")
    else:
        print(f"Failed to register. Status code: {response.status_code}")
    sio.connect(f'http://localhost?user_id={user_id}')

def unregister_from_cluster(user_id, group_id):
    """Send a request to unregister the client from a specific cluster."""
    data = {
        'user_id': user_id,
        'group_id': group_id
    }
    response = requests.post(UNREGISTER_ENDPOINT, json=data)
    if response.status_code == 200:
        print(f"Client {user_id} unregistered from cluster {group_id} successfully.")
    else:
        print(f"Failed to unregister. Status code: {response.status_code}")

def send_emoji(user_id, emoji_type):
    """Send emoji data to the server."""
    data = {
        'user_id': user_id,
        'emoji_type': emoji_type,
        'timestamp': int(time.time())
    }
    response = requests.post(SEND_EMOJI_ENDPOINT, json=data)
    if response.status_code == 200:
        print("Emoji sent successfully.")
    else:
        print(f"Failed to send emoji. Status code: {response.status_code}")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python client.py <command> <group_id> <user_id>. EG: Group ID: emoji_cluster_1_sub_1")
        print("Commands: sub, unsub, send")
        sys.exit(1)

    command = sys.argv[1]
    group_id = sys.argv[2]
    user_id = sys.argv[3]

    if command == 'sub':
        register_to_cluster(user_id, group_id)
    elif command == 'unsub':
        unregister_from_cluster(user_id, group_id)
    elif command == 'send':
        if len(sys.argv) < 3:
            print("Usage for send_emoji: python client.py send <user_id> <emoji_type>")
            sys.exit(1)
        user_id = sys.argv[2]
        emoji_type = sys.argv[3]
        send_emoji(user_id, emoji_type)

    print("Waiting...")
    sio.wait()
