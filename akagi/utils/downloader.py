import os
import hashlib
import urllib.request

from boto3 import resource, client


class Downloader(object):
    def __init__(self, data_directory_path='/tmp'):
        self._data_directory_path = data_directory_path
        self._s3_client = None
        self._s3_resource = None

    def maybe_download(self, url, sha256=None, force=False):
        url_splitted = url.split('/')
        schema = url_splitted[0][:-1]  # http://... => http
        path = os.path.join(*url_splitted[1:])

        local_filepath = self._data_directory_path + '/' + path
        use_cache = False

        if force or not os.path.exists(local_filepath):
            if not os.path.exists(os.path.dirname(local_filepath)):
                os.makedirs(os.path.dirname(local_filepath))

            print('Attempting to download:', url)
            if schema in ['s3']:
                bucket_name = url_splitted[2]
                key = os.path.join(*url_splitted[3:])

                data = self._resource.Object(bucket_name, key).get()['Body'].read()
            elif schema in ['http', 'https']:
                data = urllib.request.urlopen(url).read()
            else:
                raise Exception('Unrecognized schema:' + schema)
            print('Download Complete!')
        else:
            print('Using cache:', local_filepath)
            use_cache = True
            data = open(local_filepath, 'rb').read()

        if sha256 is not None:
            sha256_actual = hashlib.sha256(data).hexdigest()
            if not sha256_actual == sha256:
                raise Exception('''
Failed to verify: %(url)s
Expected: %(sha256)s
Actual:   %(sha256_actual)s
                        ''' % locals())

        if not use_cache:
            open(local_filepath, 'wb').write(data)

        return local_filepath

    @property
    def _resource(self):
        if self._s3_resource is None:
            self._s3_resource = resource('s3')

        return self._s3_resource

    @property
    def _client(self):
        if self._s3_client is None:
            self._s3_client = client('s3')

        return self._s3_client
