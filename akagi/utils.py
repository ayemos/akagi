import re
import six
from six import BytesIO

import gzip


def gzip_decompress(data):
    if six.PY2:
        in_io = BytesIO()
        in_io.write(data.read())
        in_io.seek(0)
        return BytesIO(gzip.GzipFile(fileobj=in_io, mode='rb').read())
    else:
        return BytesIO(gzip.decompress(data.read()))


def normalize_path(path):
    return path and re.sub(r'^/', '', re.sub(r'\/{2,}', '/', path))
