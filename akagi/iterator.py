from akagi.iterators import CSVIterator, BinaryIterator


class FileFormat(object):
    CSV = 1
    BINARY = 2


class Iterator(object):
    @classmethod
    def open_file(self, path):
        raise NotImplementedError

    @classmethod
    def decode(self, content):
        raise NotImplementedError

    @classmethod
    def get_iterator_class(cls, file_format):
        if file_format in [FileFormat.CSV, 'csv']:
            return CSVIterator
        elif file_format in [FileFormat.BINARY, 'binary']:
            return BinaryIterator
        else:
            raise Exception("Unsupported file format %(file_format)s." % locals())
