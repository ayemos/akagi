#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_s3_data_file
----------------------------------

Tests for `akagi.s3_data_source` module.
"""


import unittest
import re

from akagi.data_sources import RedshiftDataSource
from akagi.iterator import FileFormat
from akagi.iterators import CSVIterator


class TestS3DataSource(unittest.TestCase):
    def setUp(self):
        self.ds_1 = RedshiftDataSource.for_query(
                'select id, title from schema_1.table_1;',
                'schema_1',
                'table_1',
                'bucket_1', activate=False)

    def test_init(self):
        self.assertEqual(self.ds_1.bundle.bucket_name, 'bucket_1')
        self.assertEqual(self.ds_1.bundle.file_format, FileFormat.CSV)
        self.assertEqual(self.ds_1.bundle.iterator_class, CSVIterator)
        self.assertTrue(re.match(r'^schema_1_export/table_1/[0-9]{8}_[0-9]{6}/$', self.ds_1.bundle.prefix))
