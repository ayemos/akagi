import os

import psycopg2

from akagi.data_source import DataSource
from akagi.data_file_bundles import S3DataFileBundle
from akagi.query import UnloadQuery
from akagi.log import logger


class RedshiftDataSource(DataSource):
    '''RedshiftDataSource replesents a set of row data as a result of query param.
    It uses UNLOAD command and intermediate Amazon S3 bucket.
    '''

    @classmethod
    def for_query(cls, query, schema, table, bucket_name, db_conf={}, sort=False, activate=True):
        bundle = S3DataFileBundle.for_table(bucket_name, schema, table)

        query = UnloadQuery.wrap(query, bundle, sort)

        return RedshiftDataSource(bundle, query, db_conf, activate)

    def __init__(self, bundle, query, db_conf={}, activate=True):
        self.bundle = bundle
        self.query = query
        self.__db_conf = db_conf
        self.__pgpass = None

        if activate:
            self.activate()

    def activate(self):
        logger.info("Deleting old files on s3...")
        self.bundle.clear()
        logger.info("Executing query on Redshift")
        logger.debug("\n" + self.query.body + "\n")  # avoid logging unload query since it has raw credentials inside
        self._cursor.execute(self.query.sql)
        logger.info("Finished")

    @property
    def _connection(self):
        return psycopg2.connect(**self._db_conf)

    @property
    def _cursor(self):
        return self._connection.cursor()

    @property
    def _db_conf(self):
        return {
                'host':     self.__db_conf.get('host') or os.getenv('REDSHIFT_DB_HOST', self._pgpass['db_host']),
                'user':     self.__db_conf.get('user') or os.getenv('REDSHIFT_DB_USER', self._pgpass['db_user']),
                'dbname':   self.__db_conf.get('dbname') or os.getenv('REDSHIFT_DB_NAME', self._pgpass['db_name']),
                'password': self.__db_conf.get('password') or os.getenv('REDSHIFT_DB_PASS', self._pgpass['db_pass']),
                'port':     self.__db_conf.get('port') or os.getenv('REDSHIFT_DB_PORT', self._pgpass['db_port'])
                }

    @property
    def _pgpass(self):
        if self.__pgpass is None:
            self.__pgpass = {}

            with open(os.path.expanduser(os.path.join('~', '.pgpass'))) as f:
                args = [s.strip() for s in f.read().split(':')]

                if len(args) == 5:
                    (
                            self.__pgpass['db_host'], self.__pgpass['db_port'],
                            self.__pgpass['db_name'], self.__pgpass['db_user'],
                            self.__pgpass['db_pass']) = args

        return self.__pgpass

    def __exit__(self, *exc):
        self.bundle.clear()
        return False

    def __iter__(self):
        return self.bundle.__iter__()
