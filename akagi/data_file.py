import os
import akagi

from akagi.log import logger

from akagi.contents import S3Content, LocalFileContent, SpreadsheetContent, URLContent


def data_files_for_s3_prefix(bucket_name, prefix, file_format='csv'):
    return [DataFile.s3(bucket_name, obj.key, file_format) for obj in
            akagi.get_resource('s3').Bucket(bucket_name).objects.filter(Prefix=prefix)]


def data_files_for_s3_keys(bucket_name, keys, file_format='csv'):
    return [DataFile.s3(bucket_name, k, file_format) for k in keys]


def data_files_for_dir(dir_path, file_format='csv'):
    return [DataFile.local_file(os.path.join(dir_path, p), file_format) for p in
            os.listdir(dir_path)]


def data_files_for_urls(urls, file_format='csv'):
    return [DataFile.url(url, file_format) for url in urls]


class DataFile(object):
    def __init__(self, content):
        self._content = content

    def __iter__(self):
        return iter(self.content)

    @classmethod
    def s3(cls, bucket, key, file_format='csv'):
        content = S3Content(bucket, key, file_format)
        return DataFile(content)

    @classmethod
    def url(cls, url, file_format='csv'):
        content = URLContent(url, file_format)
        return DataFile(content)

    @classmethod
    def local_file(cls, path, file_format='csv'):
        content = LocalFileContent(path, file_format)
        return DataFile(content)

    @classmethod
    def spreadsheet(cls, sheet_id, sheet_range='A:Z'):
        content = SpreadsheetContent(sheet_id, sheet_range)
        return DataFile(content)

    def filename(self):
        '''Retrieve filename'''

    @property
    def path(self):
        return os.path.join(akagi.home(), 'cache', self._content.key)

    def _cache_content(self):
        '''Cache its content and return its path on local file system.'''

        dirname = os.path.dirname(self.path)

        try:
            os.makedirs(dirname)
        except OSError:
            if not os.path.isdir(dirname):
                raise

        if os.path.isfile(self.path) or os.path.isdir(self.path):
            logger.debug("Skipped %(key)s" % ({"key": self._content.key}))
        else:
            with open(self.path, 'wb') as f:
                f.write(self._content.raw_body.read())
                logger.debug("Saved %(key)s to %(path)s" % ({"key": self._content.key, "path": self.path}))

        return self.path

    @property
    def content(self):
        if not self._content.is_local:
            self._content = LocalFileContent(self._cache_content(), file_format=self._content.file_format)

        return self._content
