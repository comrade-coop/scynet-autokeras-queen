import pickle
import argparse
from confluent_kafka import Producer
import numpy
from Scynet.Shared_pb2 import Blob, Shape

def numpy_to_blob(array):
    shape = Shape()
    shape.dimension.extend(array.shape)
    blob = Blob(shape=shape)
    blob.data.extend(array.astype(float).flat)
    return blob

if __name__ == '__main__':

    producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})

    with open('./dataset', 'rb') as file:
        dataset = pickle.load(file)

        data = dataset[1]['dataset']
        labels = dataset[1]['labels']

        # I keep that here to show that the first row is all we need.
        # for index, row in enumerate(data):
        #     if index >= 1:
        #         print( numpy.array_equal(data[index - 1][1], row[0] ))
        #     else:
        #         print(row[0])
        #     input()
        try:
            for index, row in enumerate(data):      
                producer.poll(0)
                            
                producer.produce('dataset_producer', numpy_to_blob(row).SerializeToString(), str(index))
                print(row.shape)
                producer.flush()

                input()
        finally:
            producer.flush()
        
