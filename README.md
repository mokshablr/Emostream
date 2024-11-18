# EC-Team-38-emostream-concurrent-emoji-broadcast-over-event-driven-architecture

Load balancer:
Run `setup-nginx.sh` **once**

1. Run flask: `flask --app flasky run --port 5000`
   - If you need more servers, open new tab and change port. eg: `flask --app flasky run --port 5001`
2. Run spark-processing.py: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark-processing.py`
3. Run emoji-generator
