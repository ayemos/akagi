from akagi.data_source import DataSource

from akagi.data_file import data_files_for_s3_prefix, data_files_for_s3_keys


class S3DataSource(DataSource):
    '''S3DataSource replesents a set of files on Amazon S3 bucket.
    '''

    @classmethod
    def for_prefix(cls, bucket_name, prefix, file_format='binary', no_cache=False):
        return S3DataSource(bucket_name, prefix=prefix, keys=None, file_format=file_format, no_cache=no_cache)

    @classmethod
    def for_keys(cls, bucket_name, keys, file_format='binary', no_cache=False):
        return S3DataSource(bucket_name, prefix=None, keys=keys, file_format=file_format, no_cache=no_cache)

    @classmethod
    def for_key(cls, bucket_name, key, file_format='binary', no_cache=False):
        return S3DataSource(bucket_name, prefix=None, keys=[key], file_format=file_format, no_cache=no_cache)

    def __init__(self, bucket_name, prefix=None, keys=None, file_format='binary', no_cache=False):
        self._file_format = file_format
        self._bucket_name = bucket_name
        self._keys = keys
        self._prefix = prefix
        self._no_cache = no_cache
        self._data_files = None

    @property
    def data_files(self):
        if self._data_files is None:
            if self._prefix is not None:
                self._data_files = data_files_for_s3_prefix(self._bucket_name, self._prefix, self._file_format)
            elif self._keys is not None:
                self._data_files = data_files_for_s3_keys(self._bucket_name, self._keys, self._file_format)
        return self._data_files
