import json
from kafka import KafkaConsumer

kafka_server = ["127.0.0.1"]
topic = "test_topic"

consumer = KafkaConsumer(
    bootstrap_servers=kafka_server,
    value_deserializer=json.loads,
    enable_auto_commit=False,
    auto_offset_reset="latest",
    group_id="my_consumer_group",
    max_poll_records=1,
)

consumer.subscribe([topic])

try:
    for data in consumer:
        print(data.value)
        consumer.commit()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
