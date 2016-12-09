from collections import namedtuple


class Evaluator(object):
    def __init__(self, extensions):
        self._extensions = extensions

    def get_progress(self):
        raise NotImplementedError

    def get_extensions(self):
        return self._extensions

    def step(self):
        [e(self) for e in self.get_extensions()]
