from osho.model import Model

"""

Base class of osho's Classifier classes.
Use it to implement classifiers.

Example:
    literal blocks::
        class InceptionV3(Classifier):
            def classify(self, blob):
                pass

"""

class Classifier(Model):
    def classify(self, blob):
        raise NotImplementedError
