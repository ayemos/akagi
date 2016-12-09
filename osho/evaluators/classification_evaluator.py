from osho.evaluator import Evaluator
from osho.models.classifier import Classifier
from osho.dataset import LabeledDataset

class MultiClassClassificationEvaluator(Evaluator):
    def __init__(self, classifier, labeled_dataset):
        super(MultiClassClassificationEvaluator, self).__init__()

        if not (isinstance(classifier, Classifier) and isinstance(labeled_dataset, LabeledDataset)):
            raise Exception("hogeee")

        initial_counts = dict([[l, 0] for l in labeled_dataset.labels()])
        self._tp_counts = self._fp_counts = self._tn_counts = self._fn_counts = initial_counts

        self._n = len(labeled_dataset)

        self._classifier = classifier
        self._dataset = labeled_dataset

    def evaluate(self, force=False):
        # TODO: type check on the model?
        for d in self._dataset:
            answer = d.label
            pred = self._classifier.classify(d)

            # XXX: multi class classification? (what if get an array as prediction?)
            if answer == pred:
                self._add(self._tp_counts, pred, 1)

                for l in dataset.labels:
                    self._add(self._tn_counts, l, 1)
            else:
                self._add(self._fp_counts, pred, 1)
                self._add(self._fn_counts, answer, 1)

    def precision(self, label):
        return self._tp_counts[label] / (self._tp_counts[label] + self._fp_counts[label])

    def recall(self, label):
        return self._tp_counts[label] / (self._tp_counts[label] + self._fn_counts[label])

    def _add(self, counts, klass, n):
        if klass in self._tp_counts:
            self._tp_counts[klass] += n
        else:
            self._tp_counts[klass] = n
