from multiprocessing import Process, Value, Array
import time 

import torch
import numpy as np
import os, io

from Scynet.Shared_pb2 import Blob, Shape
from confluent_kafka import Producer, Consumer
				
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

	def run(self):
		try:
			self.producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})
			self.consumer = Consumer({
				'bootstrap.servers': '127.0.0.1:9092',
				'group.id': 'pytorch:' + self.uuid,
				'auto.offset.reset': 'earliest'
			})

			self.consumer.subscribe(['dataset_producer'])
			

			while True:
				msg = self.consumer.poll(1.0)

				if msg is None:
					continue
				if msg.error():
					print("Consumer error: {}".format(msg.error()))
					continue

				blob = Blob()
				blob.ParseFromString(msg.value())
				data = np.array(blob.data)
				data = data.reshape([1] + list(blob.shape.dimension)[::-1])
				print(data.shape)

				print("Heartbeat: " +  self.egg.uuid)
				result = self.model(torch.from_numpy(data).float())
				blob = numpy_to_blob(result.detach().numpy())
				print(blob)

				self.producer.poll(0)
				self.producer.produce(self.egg.uuid, blob.SerializeToString())
				print(f"Produced: {result}")

				
					

		finally:
			self.producer.flush()