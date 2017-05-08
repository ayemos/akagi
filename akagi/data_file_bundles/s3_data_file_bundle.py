import re
import boto3

from datetime import datetime

from akagi.iterator import Iterator, FileFormat
from akagi.data_files.s3_data_file import S3DataFile
from akagi.data_file_bundle import DataFileBundle
from akagi.log import logger


class S3DataFileBundle(DataFileBundle):
    @classmethod
    def for_table(self, bucket_name, schema, table, bucket_prefix='/'):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = "%(bucket_prefix)s/%(schema)s_export/%(table)s/%(timestamp)s/" % locals()

        return S3DataFileBundle(bucket_name, prefix=prefix, file_format=FileFormat.CSV)

    def __init__(self, bucket_name, prefix=None, keys=None, file_format=FileFormat.CSV):
        self.bucket_name = bucket_name
        self.prefix = self._normalize_path(prefix)
        self.keys = keys

        if self.prefix == None and self.keys == None:
            raise Exception("Either prefix or keys must be set.")

        self.file_format = file_format
        self.iterator_class = Iterator.get_iterator_class(file_format)

        self.__s3 = None

    @property
    def data_files(self):
        if self.prefix is not None:
            return [S3DataFile(obj, self.iterator_class)
                    for obj in self._bucket.objects.filter(Prefix=self.prefix)]
        else:
            return [S3DataFile(self._bucket.Object(key), self.iterator_class)
                    for key in self.keys]

    def clear(self):
        for obj in self._bucket.objects.filter(Prefix=self.prefix):
            logger.info("Deleting intermediate object on s3: %(key)s" % ({"key": obj.key}))
            obj.delete()

    @property
    def url(self):
        loc = "%(bucket_name)s/%(prefix)s" % ({
            'bucket_name': self.bucket_name,
            'prefix': self.prefix
            })
        loc = self._normalize_path(loc)

        return "s3://%(loc)s" % locals()

    @property
    def credential_string(self):
        credentials = []

        if self._credential is not None:
            if self._credential.access_key:
                credentials.append("aws_access_key_id=%s" % (self._credential.access_key))

            if self._credential.secret_key:
                credentials.append("aws_secret_access_key=%s" % (self._credential.secret_key))

            if self._credential.token:
                credentials.append("token=%s" % (self._credential.token))

        return ';'.join(credentials)

    @property
    def _credential(self):
        session = boto3.session.Session()
        return session.get_credentials()

    @property
    def _bucket(self):
        return self._s3.Bucket(self.bucket_name)

    @property
    def _s3(self):
        if self.__s3 is None:
            self.__s3 = boto3.resource('s3')

        return self.__s3

    def _normalize_path(self, path):
        if path is None:
            return None
        else:
            return re.sub(r'^/', '', re.sub(r'\/{2,}', '/', path))
