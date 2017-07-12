class BinaryIterator(object):
    def __init__(self, content):
        self.body = content.raw_body
        self._stop = False

    @classmethod
    def open_file(self, path):
        return open(path, 'rb')

    def __next__(self):
        if self._stop:
            raise StopIteration
        else:
            self._stop = True
            return self.body
