import hashlib
import io
import six
import abc

import filetype

from filetype.types.archive import Gz
from akagi.utils import gzip_decompress


@six.add_metaclass(abc.ABCMeta)
class Content(object):
    sig_length = 256

    @property
    def body(self):
        if isinstance(self.sniff(), Gz):
            return gzip_decompress(self._body)
        else:
            return self._body

    @property
    def raw_body(self):
        return self._body

    @property
    def decoded_body(self):
        if six.PY2:
            return self.body
        else:
            if isinstance(self.body, io.TextIOBase):
                return self.body
            else:
                return six.StringIO(self.body.read().decode('utf-8'))

    @abc.abstractproperty
    def _body(self):
        '''Body of the content.'''

    def sniff(self):
        self._body.seek(0)

        if six.PY2:
            sig = bytearray(self.raw_body.read(self.sig_length))
        else:
            sig = self.raw_body.read(self.sig_length)

        self._body.seek(0)

        return filetype.guess(sig)

    @abc.abstractproperty
    def key(self):
        '''Retrieve key for the content'''
