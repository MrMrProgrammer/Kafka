import json
from kafka import KafkaConsumer, KafkaProducer

kafka_server = ["127.0.0.1"]

topic = "test_topic"

consumer = KafkaConsumer(
    bootstrap_servers=kafka_server,
    value_deserializer=json.loads,
    auto_offset_reset="latest",
)

consumer.subscribe(topic)

while True:
    data = next(consumer)
    # print(data)
    print(data.value)