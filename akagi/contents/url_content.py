import re
import os
from six.moves.urllib import parse, request

from akagi.content import Content
from akagi.iterator import Iterator


class URLContent(Content):
    is_local = False

    def __init__(self, urlstr, file_format='csv'):
        self._url = parse.urlparse(urlstr)
        self.file_format = file_format
        self.iterator_class = Iterator.get_iterator_class(file_format)

    def __iter__(self):
        return self.iterator_class(self)

    @property
    def key(self):
        return os.path.join(self._url.netloc, re.sub(r'^/', '', self._url.path))

    @property
    def _body(self):
        return request.urlopen(self._url.geturl())
