import csv
from six import StringIO
import six


class CSVIterator(object):
    def __init__(self, content, skip_errors=True):
        self.content = content
        self._skip_erros = skip_errors
        self._iterator = None

    @classmethod
    def open_file(cls, path):
        return open(path, newline='')

    @classmethod
    def decode(cls, content):
        if six.PY2:
            return content
        else:
            return content.decode('utf-8')

    def __next__(self):
        try:
            n = self._iterator.__next__()
            return n
        except StopIteration as e:
            raise e
        except Exception as e:
            if self._skip_errors:
                return self._iterator.__next__()
            else:
                raise e

    def __iter__(self):
        self._iterator = csv.reader(StringIO(self.content), escapechar='\\')
        return self
