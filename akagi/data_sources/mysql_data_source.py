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

    def __init__(self, query, db_conf={}, keep_connection=False):
        self.query = query
        self.__db_conf = db_conf
        self.__connection = None
        self._keep_connection = keep_connection

    @property
    def _connection(self):
        if self._keep_connection:
            if self.__connection is None:
                self.__connection = MySQLdb.connect(**self._db_conf)

            return self.__connection
        else:
            return MySQLdb.connect(**self._db_conf)

    def __iter__(self):
        self.__result = []
        c = self._connection.cursor()
        logger.info("Executing query...")
        c.execute(self.query.body)
        logger.info("Finished.")

        return iter(c.fetchall())

    @property
    def _db_conf(self):
        conf = {
                'host':         self.__db_conf.get('host') or os.getenv('MYSQL_DB_HOST', 'localhost'),
                'user':         self.__db_conf.get('user') or os.getenv('MYSQL_DB_USER'),
                'passwd':       self.__db_conf.get('password') or os.getenv('MYSQL_DB_PASS'),
                'db':           self.__db_conf.get('db') or os.getenv('MYSQL_DB_NAME'),
                'port':         self.__db_conf.get('port') or os.getenv('MYSQL_DB_PORT', 3306),
                'unix_socket':  self.__db_conf.get('unix_socket') or os.getenv('MYSQL_DB_SOCKET')
                }
        return {k: v for k, v in six.iteritems(conf) if v is not None}
