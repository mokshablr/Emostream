# EC-Team-38-emostream-concurrent-emoji-broadcast-over-event-driven-architecture

Installs:
`pip install websocket-client eventlet flask-socketio redis`
`sudo apt install redis-server`

Load balancer:
Run `setup-nginx.sh` **once**

1. Run flask: `flask --app flasky run --port 5000`
   - If you need more servers, open new tab and change port. eg: `flask --app flasky run --port 5001`
2. Run spark-processing.py: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark-processing.py`
3. Run emoji-generator

Testing:
`pip install pytest flask pytest-mock locust`

Run test files: `pytest test_*.py`

Locust testing:

1. `locust --host=http://localhost`
2. go to `localhost:8089` and set 1000 and 1000 for users and concurrency
