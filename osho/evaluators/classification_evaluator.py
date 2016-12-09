from multiprocessing.pool import Pool
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

    def __getstate__(self):
        """ This is called before pickling. """
        state = self.__dict__.copy()
        del state['_classifier']
        del state['_dataset']
        return state

    def __setstate__(self, state):
        """ This is called while unpickling. """
        self.__dict__.update(state)

    def evaluate(self, force=False, n_process=1):
        if n_process > 1:
            self._evaluate_mp(n_process, force)
        else:
            self._evaluate(force)

    def _evaluate(self, force=False):
        for e in self._dataset:
            data = e[0]
            answer = e[1]

            self._process(data, answer)

    def _evaluate_mp(self, n_process, force=False):
        pool = Pool(n_process)
        args = [[e[0], e[1]] for e in self._dataset]
        pool.map(self._process, args)

    def _process(self, data, answer):

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
