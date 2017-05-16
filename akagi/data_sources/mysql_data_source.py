import os
import six

import MySQLdb

from akagi.data_source import DataSource
from akagi.query import Query
from akagi.log import logger


class MySQLDataSource(DataSource):
    '''MySQLDataSource replesents a set of row data as a result of query param.
    '''

    @classmethod
    def for_query(cls, query, db_conf={}):
        query = Query(query)

        return MySQLDataSource(query, db_conf)

    def __init__(self, query, db_conf={}):
        self.query = query
        self.__db_conf = db_conf
        self.__cursor = None

    @property
    def _connection(self):
        return MySQLdb.connect(**self._db_conf)

    def __iter__(self):
        self.__result = []
        logger.info("Query sent to Redshift")
        logger.info("\n" + self.query.body + "\n")

        self._cursor.execute(self.query.body)

        return iter(self._cursor.fetchall())

    @property
    def _cursor(self):
        if self.__cursor is None:
            self.__cursor = self._connection.cursor()

        return self.__cursor

    @property
    def _db_conf(self):
        conf = {
                'host':         self.__db_conf.get('host') or os.getenv('MYSQL_DB_HOST', 'localhost'),
                'user':         self.__db_conf.get('user') or os.getenv('MYSQL_DB_USER'),
                'passwd':       self.__db_conf.get('password') or os.getenv('MYSQL_DB_PASS'),
                'db':           self.__db_conf.get('db') or os.getenv('MYSQL_DB_PASS', os.getenv('USER')),
                'port':         self.__db_conf.get('port') or os.getenv('MYSQL_DB_PORT', 3306),
                'unix_socket':  self.__db_conf.get('unix_socket') or os.getenv('MYSQL_DB_SOCKET')
                }
        return {k: v for k, v in six.iteritems(conf) if v is not None}

    def __exit__(self, *exc):
        return False
