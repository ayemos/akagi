import csv
from six import StringIO
import six


class CSVIterator(object):
    def __init__(self, content):
        self.content = content

    def __iter__(self):
        if six.PY2:
            content = self.content
        else:
            content = self.content.decode('utf-8')

        return csv.reader(StringIO(content))
