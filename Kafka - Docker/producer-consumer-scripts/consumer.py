from kafka import KafkaConsumer
from json import loads
from time import sleep


def read_data():
    """
    Reads data from Kafka
    :return:
    """
    consumer = KafkaConsumer(
        'example_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    for event in consumer:
        event_data = event.value
        print(event_data)
        sleep(2)


if __name__ == "__main__":
    read_data()
