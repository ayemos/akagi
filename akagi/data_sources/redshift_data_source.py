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
    def for_query(cls, query, schema, table, bucket_name,
                  db_host=None, db_name=None, db_user=None, db_pass=None, db_port=None, sort=False):
        bundle = S3DataFileBundle.for_table(
                bucket_name,
                schema,
                table
                )

        query = UnloadQuery.wrap(query, bundle, sort)

        return RedshiftDataSource(bundle, query, db_host, db_name, db_user, db_pass, db_port)

    def __init__(self, bundle, query, db_host, db_name, db_user, db_pass, db_port):
        self.bundle = bundle
        self.query = query
        self._db_host = db_host
        self._db_name = db_name
        self._db_user = db_user
        self._db_pass = db_pass
        self._db_port = db_port
        self.__pgpass = None

        self.activate()

    def activate(self):
        logger.info("Deleting old files on s3...")
        self.bundle.clear()
        logger.info("Query sent to Redshift")
        logger.info("\n" + self.query.body + "\n")  # avoid logging unload query since it has raw credentials inside
        self._cursor.execute(self.query.sql)

    def __iter__(self):
        return iter(self.bundle)

    @property
    def _connection(self):
        return psycopg2.connect(
                host=self.db_host, dbname=self.db_name,
                user=self.db_user, password=self.db_pass, port=self.db_port)

    @property
    def _cursor(self):
        return self._connection.cursor()

    @property
    def db_host(self):
        if self._db_host is None:
            self._db_host = os.getenv("REDSHIFT_DB_HOST", self._pgpass['db_host'])

        return self._db_host

    @property
    def db_name(self):
        if self._db_name is None:
            self._db_name = os.getenv("REDSHIFT_DB_NAME", self._pgpass['db_name'])

        return self._db_name

    @property
    def db_pass(self):
        if self._db_pass is None:
            self._db_pass = os.getenv("REDSHIFT_DB_PASS", self._pgpass['db_pass'])

        return self._db_pass

    @property
    def db_user(self):
        if self._db_user is None:
            self._db_user = os.getenv('REDSHIFT_DB_USER', self._pgpass['db_user'])

        return self._db_user

    @property
    def db_port(self):
        if self._db_port is None:
            self._db_port = os.getenv('REDSHIFT_DB_PORT', self._pgpass['db_port'])

        return self._db_port

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
