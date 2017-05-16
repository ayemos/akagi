class BinaryIterator(object):
    def __init__(self, content):
        self.content = content

    @classmethod
    def open_file(self, path):
        return open(path, 'rb')

    @classmethod
    def decode(self, content):
        return content

    def __iter__(self):
        yield self.content
        raise StopIteration
