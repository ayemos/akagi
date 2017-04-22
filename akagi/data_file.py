import os


class DataFile(object):
    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError

    def _is_gzip(self):
        return os.path.splitext(self.filename)[-1] == '.gz'
