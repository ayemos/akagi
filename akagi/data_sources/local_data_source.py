from akagi.data_source import DataSource
from akagi.data_file_bundles import LocalDataFileBundle
from akagi.iterator import FileFormat


class LocalDataSource(DataSource):
    '''LocalDataSource replesents a set of files on local file system.
    '''

    @classmethod
    def for_path(cls, path, file_format=FileFormat.BINARY):
        bundle = LocalDataFileBundle(path, file_format)

        return LocalDataSource(bundle)

    def __init__(self, bundle):
        self.bundle = bundle

    def __exit__(self, *exc):
        return False
