class SpreadsheetIterator(object):
    def __init__(self, content, skip_errors=True, delimiter=','):
        self.body = content.decoded_body
        self._skip_errors = skip_errors

        self._iterator = csv.reader(self.body, escapechar='\\', delimiter=delimiter)

    @classmethod
    def open_file(cls, path):
        return open(path, newline='')

    def next(self):
        try:
            return next(self._iterator)
        except StopIteration as e:
            raise e
        except Exception as e:
            if self._skip_errors:
                return next(self._iterator)
            else:
                raise e
    __next__ = next
