import csv
from six import StringIO
import six


class CSVIterator(object):
    def __init__(self, content):
        self.content = content

    @classmethod
    def open_file(cls, path):
        return open(path, 'r')

    @classmethod
    def decode(cls, content):
        if six.PY2:
            return content
        else:
            return content.decode('utf-8')

    def __iter__(self):
        return csv.reader(StringIO(self.content))
