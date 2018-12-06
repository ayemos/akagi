import hashlib
from datetime import datetime
import sys
import boto3
import re
import six
import os

import psycopg2

import akagi
from akagi.data_source import DataSource
from akagi.log import logger
from akagi.utils import normalize_path
from akagi.data_file import data_files_for_s3_prefix


class RedshiftDataSource(DataSource):
    '''RedshiftDataSource replesents a set of row data as a result of query param.
    It uses UNLOAD command and intermediate Amazon S3 bucket.
    '''
    def __init__(self, query_str, bucket_name=None, db_conf={}, no_cache=False):
        self._query_str = query_str
        self.__bucket_name = bucket_name
        self.__db_conf = db_conf
        self.__pgpass = None
        self._no_cache = no_cache
        self._data_files = None

    @property
    def _exists_on_s3(self):
        for _ in akagi.get_resource('s3').Bucket(self._bucket_name)\
                .objects.filter(Prefix=self._unload_root_prefix).limit(1):
            return True

        return False

    @property
    def _unload_root_prefix(self):
        p = os.path.join(
            os.getenv('AKAGI_UNLOAD_PREFIX', 'akagi_unload'),
            self._hex_hash)

        if p.endswith('/'):
            return p
        else:
            return p + '/'

    @property
    def data_files(self):
        if self._data_files is None:
            if not self._exists_on_s3:
                unload_prefix = self._unload_root_prefix
                datetime.utcnow().strftime("%Y%m%d_%H%M%f")  # XXX: utcnow()?

                query = UnloadQuery(self._query_str, self._bucket_name, unload_prefix)

                logger.debug("Executing query on Redshift")
                logger.debug("\n" + query.body + "\n")  # avoid logging unload query since it has raw credentials inside
                self._cursor.execute(query.sql)
                logger.debug("Finished")

            self._data_files = data_files_for_s3_prefix(self._bucket_name, self._latest_prefix_on_s3)

        return self._data_files

    @property
    def _latest_prefix_on_s3(self):
        prefixes = list(akagi.get_resource('s3').Bucket(self._bucket_name).objects.filter(Prefix=self._unload_root_prefix))

        if len(prefixes) > 0:
            return prefixes[-1].key
        else:
            None

    @property
    def _connection(self):
        connection = psycopg2.connect(**self._db_conf)
        connection.autocommit = 'REDSHIFT_DISABLE_AUTOCOMMIT' not in os.environ
        return connection

    @property
    def _cursor(self):
        return self._connection.cursor()

    @property
    def _db_conf(self):
        return {
            'host':
            self.__db_conf.get('host') or os.getenv('REDSHIFT_DB_HOST', self._pgpass['db_host']),
            'user':
            self.__db_conf.get('user') or os.getenv('REDSHIFT_DB_USER', self._pgpass['db_user']),
            'dbname':
            self.__db_conf.get('dbname') or os.getenv('REDSHIFT_DB_NAME', self._pgpass['db_name']),
            'password':
            self.__db_conf.get('password') or os.getenv('REDSHIFT_DB_PASS', self._pgpass['db_pass']),
            'port':
            self.__db_conf.get('port') or os.getenv('REDSHIFT_DB_PORT', self._pgpass['db_port'])}

    @property
    def _pgpass(self):
        if self.__pgpass is None:
            self.__pgpass = {
                'db_host': '',
                'db_port': '',
                'db_name': '',
                'db_user': '',
                'db_pass': ''}

            pgpass_path = os.path.expanduser(os.path.join('~', '.pgpass'))
            # Load .pgpass file
            if os.path.isfile(pgpass_path):
                with open(pgpass_path) as f:
                    lines = f.read().split('\n')
                    assert len(lines) > 0, 'Empty .pgpass file'
                    args = [s.strip() for s in lines[0].split(':')]
                    assert len(args) == 5, 'Invalid format .pgpass. Expected `hostname:port:database:username:password`'
                    self.__pgpass['db_host'] = args[0]
                    self.__pgpass['db_port'] = args[1] if args[1] != "*" else ''
                    self.__pgpass['db_name'] = args[2] if args[2] != "*" else ''
                    self.__pgpass['db_user'] = args[3]
                    self.__pgpass['db_pass'] = args[4]

        return self.__pgpass

    def __exit__(self, *exc):
        self.bundle.clear()
        return False

    @property
    def _hex_hash(self):
        return hashlib.sha256(self._query_str.encode('utf-8')).hexdigest()

    @property
    def _bucket_name(self):
        if self.__bucket_name is None:
            try:
                self.__bucket_name = os.environ['AKAGI_UNLOAD_BUCKET']
            except KeyError:
                logger.error('Environment variable AKAGI_UNLOAD_BUCKET must be set when using RedshiftDataSource.')
                sys.exit(1)

        return self.__bucket_name


class Query(object):
    def __init__(self, body):
        self._body = body

    def wrap(self, query):
        raise NotImplementedError

    @property
    def body(self):
        if six.PY2:
            return self._body.decode('utf-8')
        else:
            return self._body

    def __str__(self):
        return self.body


class UnloadQuery(Query):
    def __init__(self, body, bucket_name, prefix, sort=False):
        super(UnloadQuery, self).__init__(body)

        self._bucket_name = bucket_name
        self._prefix = prefix
        self._sort = sort

    @property
    def sql(self):
        s3_url = "%(bucket_name)s/%(prefix)s" % ({
            'bucket_name': self._bucket_name,
            'prefix': self._prefix})

        s3_url = "s3://" + normalize_path(s3_url)

        return """
unload ('%(query)s')
to '%(s3_url)s'
credentials '%(credential_string)s'
gzip
allowoverwrite
parallel %(enable_sort)s
delimiter ',' escape addquotes
            """ % ({
            'query': re.sub(r"'", "\\\\'", self.body),
            's3_url': s3_url,
            'credential_string': self._credential_string,
            'enable_sort': "on" if self._sort else "off"})

    @property
    def _credential_string(self):
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
