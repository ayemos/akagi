from akagi.data_source import DataSource
from akagi.data_file_bundles import S3DataFileBundle
from akagi.iterator import FileFormat


class S3DataSource(DataSource):
    '''S3DataSource replesents a set of files on Amazon S3 bucket.
    '''

    @classmethod
    def for_prefix(cls, bucket_name, prefix, file_format=FileFormat.BINARY):
        bundle = S3DataFileBundle(
                bucket_name,
                prefix=prefix,
                file_format=file_format
                )

        return S3DataSource(bundle)

    @classmethod
    def for_keys(cls, bucket_name, keys, file_format=FileFormat.BINARY):
        bundle = S3DataFileBundle(
                bucket_name,
                keys=keys,
                file_format=file_format
                )

        return S3DataSource(bundle)

    @classmethod
    def for_key(cls, bucket_name, key, file_format=FileFormat.BINARY):
        return S3DataSource.for_prefix(bucket_name, key, file_format)

    def __init__(self, bundle):
        self.bundle = bundle

    def __exit__(self, *exc):
        return False
