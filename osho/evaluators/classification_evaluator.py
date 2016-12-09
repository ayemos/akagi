from collections import namedtuple

from osho.evaluator import Evaluator


class ClassificationEvaluator(Evaluator):
    CorrectResult = namedtuple('CorrectResult', ['data', 'label'])
    WrongResult = namedtuple('WrongResult', ['data', 'predict', 'actual'])

    def __init__(self, classifier, dataset, extensions=None):
        super(ClassificationEvaluator, self).__init__(extensions)

        self._corrects = []
        self._wrongs = []

        self._classifier = classifier
        self._dataset = dataset
        self._num_processed = 0

    def evaluate(self, force=False):
        for e in self._dataset:
            answer = e[1]
            data = e[0]

            pred = self._classifier.classify(data)

            if answer == pred:
                self._corrects.append(
                        ClassificationEvaluator.CorrectResult(
                            data=data,
                            label=answer
                            )
                        )
            else:
                self._wrongs.append(
                        ClassificationEvaluator.WrongResult(
                            data=data,
                            predict=pred,
                            actual=answer
                            )
                        )

            self._num_processed += 1
            self.step()

    def get_progress(self):
        return self._num_processed / self._dataset.get_length()

    def _tps(self, label):
        # verbose list() call for python2/3 compatibility
        return len(list(filter(lambda e: e.label == label, self._corrects)))

    def _fps(self, label):
        return len(list(filter(lambda e: e.predict == label, self._wrongs)))

    def _fns(self, label):
        return len(list(filter(lambda e: e.actual == label, self._wrongs)))

    def precision(self, label):
        tps = self._tps(label)
        fps = self._fps(label)

        return tps / (tps + fps)

    def recall(self, label):
        tps = self._tps(label)
        fns = self._fns(label)

        return tps / (tps + fns)
