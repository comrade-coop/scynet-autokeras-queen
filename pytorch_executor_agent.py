from multiprocessing import Process
import torch
import numpy as np
import io
from collections import deque
from confluent_kafka import Producer, Consumer, KafkaError

from Scynet.Shared_pb2 import Blob, Shape


def numpy_to_blob(array):
    shape = Shape()
    shape.dimension.extend(array.shape)
    blob = Blob(shape=shape)
    blob.data.extend(array.astype(float).flat)
    return blob


class TorchExecutor(Process):
    def __init__(self, uuid, egg):
        super(TorchExecutor, self).__init__()
        self.uuid = uuid
        self.egg = egg

        buffer = io.BytesIO(egg.eggData)
        self.model = torch.load(buffer)
        x_windowing = 48  # FIXME: Hardcode
        self.window = deque(maxlen=x_windowing)

    def run(self):
        try:
            self.producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})
            self.consumer = Consumer({
                'bootstrap.servers': '127.0.0.1:9092',
                'group.id': 'pytorch:' + self.uuid,
                'auto.offset.reset': 'earliest'
            })

            self.consumer.subscribe(list(self.egg.inputs))

            while True:
                msg = self.consumer.poll()

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._MAX_POLL_EXCEEDED:
                        print("Consumer error: {}".format(msg.error()))
                    continue

                blob = Blob()
                blob.ParseFromString(msg.value())
                data = np.array(blob.data)
                data = data.reshape([1] + list(blob.shape.dimension)[::-1])

                self.window.append(data)
                if len(self.window) == self.window.maxlen:
                    # TODO: No idea how this works, but it does. If it breaks, it likely broke right here.
                    windowed_data = np.stack(self.window, axis=2)
                    result = self.model(torch.from_numpy(windowed_data).float())
                    result = result.detach().numpy()
                    # FIXME: Torch models are improperly saved/loaded, and thus the final result has shape (1, 2), instead of (1,). So, we have to magically remap that to 0..1.
                    try:
                        abs_result = np.absolute(result[0])
                        blob = numpy_to_blob(abs_result[1] / (abs_result[0] + abs_result[1]) + (np.random.random_sample() - 0.5) * 0.05)
                    except Exception:
                        blob = numpy_to_blob(result)

                    self.producer.poll(0)
                    self.producer.produce(self.egg.uuid, key=msg.key(), value=blob.SerializeToString())

        finally:
            self.producer.flush()
            self.consumer.close()
