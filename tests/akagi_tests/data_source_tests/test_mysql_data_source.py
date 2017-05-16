#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_mysql_data_file
----------------------------------

Tests for `akagi.mysql_data_source` module.
"""


import unittest

from akagi.data_sources import MySQLDataSource


class TestMySQLDataSource(unittest.TestCase):
    def setUp(self):
        self.query_body = 'select id, title from schema_1.table_1;'
        self.ds_1 = MySQLDataSource.for_query(self.query_body)

    def test_init(self):
        self.assertEqual(self.ds_1.query.body, self.query_body)
