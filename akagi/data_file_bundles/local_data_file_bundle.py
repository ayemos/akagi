import os
import glob

from akagi.iterator import Iterator, FileFormat
from akagi.data_file_bundle import DataFileBundle
from akagi.data_files import LocalDataFile


class LocalDataFileBundle(DataFileBundle):
    def __init__(self, directory_path, file_format=FileFormat.CSV):
        self.directory_path = os.path.expanduser(directory_path)
        self.iterator_class = Iterator.get_iterator_class(file_format)

    @property
    def data_files(self):
        paths = []

        for root, _, filenames in os.walk(self.directory_path):
            for filename in filenames:
                paths.append(os.path.join(root, filename))

        return [LocalDataFile(path, self.iterator_class) for path in paths]
