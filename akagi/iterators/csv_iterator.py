import csv
from io import StringIO


class CSVIterator(object):
    def __init__(self, content):
        self.content = content

    def __iter__(self):
        return csv.reader(StringIO(self.content.decode('utf-8')))
