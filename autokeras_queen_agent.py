from multiprocessing import Process

import os
import io

from autokeras import ImageClassifier
from autokeras.search import BayesianSearcher
# from autokeras.preprocessor import OneHotEncoder

import torch
import kafka_dataset

clf = None
producer = None


def fp_fn_metrics(y_test, y):
    assert(y.shape == y_test.shape)

    fp = 0
    fn = 0

    for i in range(y.shape[0]):
        yi = int(round(y[i]))
        y_ti = int(round(y_test[i]))

        if yi != y_ti:
            print(i, yi, y_ti)
            if yi == 1:
                fp += 1
            else:
                fn += 1
    return (fp, fn)


def evaluate(x_test, y_test):
    y_test = y_test.flatten()

    y = clf.predict(x_test)

    sc = clf.evaluate(x_test, y_test)

    fp, fn = fp_fn_metrics(y_test, y)

    print("Diff:", y_test - y)
    print("Score %d (%d fp; %d fn / %d)" % (sc * 100, fp, fn, y_test.shape[0]))


class ObservableSearcher(BayesianSearcher):
    def add_model(self, metric_value, loss, graph, model_id):

        # Save all the models in the controller so we don't have anything here.
        if model_id == 0:
            new_best = True
        else:
            new_best = model_id == self.get_best_model_id()
            print(
                f"Got the model[{model_id}]: {loss} {metric_value} {self.get_best_model_id()}")

        if new_best:
            print("Saving new best!")
            model = graph.produce_model()

            buffer = io.BytesIO()

            torch.save(model, buffer)
            torch.save(model, f"./model.torch")
            self.queen.publish_agent(bytes(buffer.getbuffer()), 1 / loss)
            # model = graph.produce_model().save("./model.h5")

        return super().add_model(metric_value, loss, graph, model_id)


class Queen(Process):
    def __init__(self, hatchery, input, target):
        super(Queen, self).__init__()

        self.hatchery = hatchery
        self.input = input
        self.target = target
        self.path = "res"

        # save the output to a log file

    def publish_agent(self, model, performance):
        self.hatchery.RegisterAgent(model, [self.input], performance)

    def prepare(self, path, x_train, y_train):
        global clf
        # TODO: find a better way to make our own Searcher
        if os.path.isdir(self.path):
            print(f"Resuming")

        # TODO: Fix resume after debug
        ObservableSearcher.queen = self
        clf = self.clf = ImageClassifier(
            verbose=True, augment=False, path=path, resume=False, search_type=ObservableSearcher)

    def run(self):
        while True:
            try:
                (x_train, y_train), (x_test, y_test) = kafka_dataset.run(self.input, self.target)

                self.prepare(self.path, x_train, y_train)

                #clf.fit(x_train, y_train, time_limit=30)
                clf.fit(x_train, y_train, time_limit=12 * 60 * 60)
                clf.final_fit(x_train, y_train, x_test, y_test, retrain=True)
                evaluate(x_test, y_test)
            except KeyboardInterrupt:
                break
            except BaseException:
                pass
