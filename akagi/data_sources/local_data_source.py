from akagi.data_source import DataSource
from akagi.data_file import data_files_for_dir


class LocalDataSource(DataSource):
    '''LocalDataSource replesents a set of files on local file system.
    '''

    def __init__(self, path, file_format='csv'):
        self._path = path
        self._file_format = file_format

    def __exit__(self, *exc):
        return False

    @property
    def data_files(self):
        return data_files_for_dir(self._path, self._file_format)
