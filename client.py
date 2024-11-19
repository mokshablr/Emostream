import socketio
import requests
import json
import time
import sys
import threading

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
    print(f"New subscription: User {data['user_id']} has joined the group {data['group_id']}")

@sio.event
def unsubscribe(data):
    del_group_id = data['group_id']
    del_user_id = data['user_id']
    if del_user_id==user_id:
        print(f"Unsubscribed from group {del_group_id}")
        sio.disconnect()  # Disconnect from the server when unsubscribed

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
        sio.emit('leave', {'user_id': user_id, 'group_id': group_id})
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


def listen_for_unsubscribe():
    """Listen for the 'u' keypress to unsubscribe."""
    while True:
        user_input = input()
        if user_input.lower() == 'u':
            unregister_from_cluster(user_id, group_id)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python client.py <command> <group_id> <user_id>")
        print("Commands: sub, unsub, send")
        sys.exit(1)

    command = sys.argv[1]
    group_id = sys.argv[2]
    user_id = sys.argv[3]

    print(f"\n\n======== \t USER_ID: {user_id} | GROUP_ID: {group_id} \t ========\n\n")

    if command == 'sub':
        register_to_cluster(user_id, group_id)
        print("\n\nPRESS 'u' TO UNSUBSCRIBE and DISCONNECT \n\n")
        threading.Thread(target=listen_for_unsubscribe, daemon=True).start()

    elif command == 'unsub':
        unregister_from_cluster(user_id, group_id)
    elif command == 'send':
        if len(sys.argv) < 4:
            print("Usage for send_emoji: python client.py send <user_id> <emoji_type>")
            sys.exit(1)
        user_id = sys.argv[2]
        emoji_type = sys.argv[3]
        send_emoji(user_id, emoji_type)

    print("Waiting...")
    sio.wait()
