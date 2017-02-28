import six


class StaticLabel(object):
    def __init__(self, label_str):
        self._label = label_str

    def get_length(self):
        return six.MAXSIZE  # XXX: Which is the best number for the length?

    def get_example(self, _):
        return self._label
