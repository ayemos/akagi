import six
from six import BytesIO

import gzip


def gzip_compress(data):
    if six.PY2:
        out_io = BytesIO()
        gzip.GzipFile(fileobj=out_io, mode='wb').write(data)
        out_io.seek(0)
        return out_io.read()
    else:
        return gzip.compress(data)


def gzip_decompress(data):
    if six.PY2:
        in_io = BytesIO()
        in_io.write(data)
        in_io.seek(0)
        return gzip.GzipFile(fileobj=in_io, mode='rb').read()
    else:
        return gzip.decompress(data)
