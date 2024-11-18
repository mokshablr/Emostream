from unittest.mock import MagicMock, patch
from flasky import send_to_kafka

@patch('flasky.producer')
def test_send_to_kafka(mock_producer):
    mock_send = MagicMock()
    mock_producer.send = mock_send

    data = {"user_id": 1, "emoji_type": "❤️", "timestamp": 1234567890}
    
    send_to_kafka(data)

    mock_send.assert_called_once_with('emoji_topic', data)
