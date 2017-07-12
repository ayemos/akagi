from akagi.content import Content
from akagi.iterator import Iterator


class LocalFileContent(Content):
    is_local = True

    def __init__(self, path, file_format='csv'):
        self.path = path
        self.file_format = file_format
        self.iterator_class = Iterator.get_iterator_class(file_format)

    def __iter__(self):
        return self.iterator_class(self)

    @property
    def key(self):
        return self.path

    @property
    def _body(self):
        return open(self.path, 'rb')
