from json import dumps
from kafka import KafkaProducer
import json


def send_data():
    """
    Sends data to a Kafka Topic
    :return:
    """
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    with open(r'E:\kafka-docker\producer-consumer\citylots.json') as f:
        data = json.load(f)
        counter = 0
        for zz in data['features']:
            producer.send('example_topic', zz)
            print(counter)
            counter += 1
    producer.flush()


if __name__ == "__main__":
    send_data()
