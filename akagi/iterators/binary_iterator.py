import csv


class BinaryIterator(object):
    def __init__(self, content):
        self.content = content

    def __iter__(self):
        yield self.content
        raise StopIteration
