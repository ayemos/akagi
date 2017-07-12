import os
from six import BytesIO

import akagi
from akagi.content import Content
from akagi.iterator import Iterator


class S3Content(Content):
    is_local = False

    def __init__(self, bucket_name, key, file_format='csv'):
        self._bucket_name = bucket_name
        self._key = key
        self.file_format = file_format
        self.iterator_class = Iterator.get_iterator_class(file_format)

    def __iter__(self):
        return self.iterator_class(self)

    @property
    def key(self):
        return os.path.join(self._bucket_name, self._key)

    @property
    def _body(self):
        out_io = BytesIO()
        akagi.get_resource('s3').Object(self._bucket_name, self._key).download_fileobj(out_io)
        out_io.seek(0)

        return out_io
