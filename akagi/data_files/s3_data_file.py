import gzip
import io

from akagi.data_file import DataFile


class S3DataFile(DataFile):
    def __init__(self, obj, iterator_class):
        self.obj = obj
        self.iterator_class = iterator_class

    @property
    def filename(self):
        return self.key

    @property
    def key(self):
        return self.obj.key

    @property
    def content(self):
        if self._is_gzip():
            return gzip.decompress(self.obj.get()["Body"].read())
        else:
            return self.obj.get()["Body"].read()

    def __iter__(self):
        return iter(self.iterator_class(self.content))
