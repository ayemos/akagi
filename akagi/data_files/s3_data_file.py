import boto3

from six import BytesIO

from akagi.utils import gzip_decompress
from akagi.data_file import DataFile


class S3DataFile(DataFile):
    def __init__(self, obj, iterator_class):
        self.obj = obj
        self.iterator_class = iterator_class

        self.__s3 = None

    @property
    def filename(self):
        return self.key

    @property
    def key(self): return self.obj.key

    @property
    def raw_content(self):
        out_io = BytesIO()
        self._s3.Object(self.obj.bucket_name, self.obj.key).download_fileobj(out_io)
        out_io.seek(0)

        return out_io.read()

    @property
    def content(self):
        if self._is_gzip():
            return gzip_decompress(self.raw_content)
        else:
            return self.raw_content

    def __iter__(self):
        cls = self.iterator_class
        return iter(cls(cls.decode(self.content)))

    @property
    def _s3(self):
        if self.__s3 is None:
            self.__s3 = boto3.resource('s3')

        return self.__s3
