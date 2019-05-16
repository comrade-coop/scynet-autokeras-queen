import random
import numpy as np
from confluent_kafka import Consumer, KafkaError
from collections import defaultdict

from Scynet.Shared_pb2 import Blob


# https://stackoverflow.com/a/15722507/4168713
def window_stack(a, stepsize=1, width=3):
    return np.hstack([a[i:1 + i - width or None:stepsize] for i in range(0, width)])


def run(x_topic, y_topic, test_size=30 * 24):
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'autokerast:' + str(random.randint(0, 1000000000)),
        'auto.offset.reset': 'earliest'
    })
    x_windowing = 48  # FIXME: Hardcode

    consumer.subscribe([x_topic, y_topic])

    data = defaultdict(lambda: (None, None))

    while True:
        msg = consumer.poll(5.0)

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
        msg_data = msg_data.reshape([1] + list(blob.shape.dimension)[::-1])

        current_data = data[msg.key()]
        if msg.topic() == x_topic:
            data[msg.key()] = (msg_data, current_data[1])
        if msg.topic() == y_topic:
            data[msg.key()] = (current_data[0], msg_data)

    consumer.close()

    result_data = ([], [])
    for row in data.values():
        if row[0] is not None and row[1] is not None:
            result_data[0].append(row[0])
            result_data[1].append(row[1])
    data = None

    result_data = (np.array(result_data[0]), np.array(result_data[1]))

    if result_data[0].shape[0] > test_size:
        result = ((result_data[0][:-test_size], result_data[1][:-test_size]), (result_data[0][-test_size:], result_data[1][-test_size:]))
    else:
        result = (result_data, (np.empty((0,) + result_data[0].shape[1:]), np.empty((0,) + result_data[1].shape[1:])))

    if x_windowing != 0:
        result = ((window_stack(result[0][0], 1, x_windowing), result[0][1][x_windowing - 1:]), (window_stack(result[1][0], 1, x_windowing), result[1][1][x_windowing - 1:]))

    print(len(result[0][0]), len(result[0][1]), len(result[1][0]), len(result[1][1]))

    return result
