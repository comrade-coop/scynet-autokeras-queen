import random
import numpy as np
from confluent_kafka import Consumer, KafkaError
from collections import defaultdict

from Scynet.Shared_pb2 import Blob


def run(x_topic, y_topic, test_size=30 * 24):
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'autokeras:' + str(random.randint(0, 100000000)),
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([x_topic, y_topic])

    data = defaultdict(lambda: (None, None))

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            print("Consumer error: {}".format(msg.error()))
            continue

        blob = Blob()
        blob.ParseFromString(msg.value())
        msg_data = np.array(blob.data)
        msg_data = data.reshape([1] + list(blob.shape.dimension)[::-1])

        current_data = data[msg.key()]
        if msg.topic() == x_topic:
            data[msg.key()] = (msg_data, current_data[1])
        if msg.topic() == y_topic:
            data[msg.key()] = (current_data[0], msg_data)

    consumer.close()

    data = np.array([row for row in data if row[0] is not None and row[1] is not None])

    if data.shape[0] > test_size:
        return (data[:-test_size, 0], data[:-test_size, 1]), (data[-test_size:, 0], data[-test_size:, 1])
    else:
        return (data[:, 0], data[:, 1]), (np.empty([0] + data.shape[1:]), np.empty([0] + data.shape[1:]))
