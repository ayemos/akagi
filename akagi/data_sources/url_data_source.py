from akagi.data_source import DataSource

from akagi.data_file import data_files_for_urls


class URLDataSource(DataSource):
    '''URLDataSource replesents a set of files on remote location.
    '''

    @classmethod
    def for_urls(cls, urls, file_format='csv', no_cache=False):
        return URLDataSource(urls, file_format=file_format, no_cache=no_cache)

    def __init__(self, urls, file_format='csv', no_cache=False):
        self._urls = urls
        self._file_format = file_format

    @property
    def data_files(self):
        return data_files_for_urls(self._urls, self._file_format)
