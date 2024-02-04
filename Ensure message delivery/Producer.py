import json
from kafka import KafkaProducer

kafka_server = ["127.0.0.1"]

topic = "test_topic"

producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all',
)

while True:
    data = input("please enter your message : ")
    producer.send(topic, data)
    producer.flush()