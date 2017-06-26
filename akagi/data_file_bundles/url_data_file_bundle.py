from akagi.data_file_bundle import DataFileBundle
from akagi.iterator import Iterator, FileFormat

from six.moves import urllib


class URLDataFileBundle(DataFileBundle):
    def __init__(self, urlstr, file_format=FileFormat.CSV):
        self.url = urllib.parse.urlparse(urlstr)
        self.file_format = file_format

    @property
    def data_files(self):
        if self.url.scheme in ['http', 'https']:
            return URLDataFile(url)
        # elif self.url.scheme in ['ftp', 'sftp']:
            # return
