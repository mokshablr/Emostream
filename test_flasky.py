import pytest
from flask import Flask
from flasky import app  
from unittest.mock import patch

# Test the /send_emoji endpoint
@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_send_emoji_valid_data(client):
    data = {
        "user_id": 1,
        "emoji_type": "😂",
        "timestamp": 1234567890
    }
    response = client.post('/send_emoji', json=data)
    assert response.status_code == 200
    assert response.json == {"status": "Emoji data received"}

@patch('flasky.send_to_kafka')
def test_send_emoji_invalid_data(mock_send, client):
    # Test with invalid data (missing fields)
    data = {"emoji_type": "😂"}
    response = client.post('/send_emoji', json=data)
    assert response.status_code == 400
    assert response.json == {"error": "Invalid data"}

    mock_send.assert_not_called()

@patch('flasky.send_to_kafka')
def test_send_emoji_sends_to_kafka(mock_send, client):
    data = {
        "user_id": 1,
        "emoji_type": "❤️",
        "timestamp": 1234567890
    }
    client.post('/send_emoji', json=data)

    mock_send.assert_called_once()
