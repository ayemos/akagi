#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_s3_data_file
----------------------------------

Tests for `akagi.s3_data_file_bundle` module.
"""


import unittest
import re

from akagi.data_file_bundles import S3DataFileBundle


class TestS3DataFile(unittest.TestCase):
    def setUp(self):
        self.bundle_1 = S3DataFileBundle('bucket_1', 'prefix_1', 'csv')
        self.bundle_2 = S3DataFileBundle.for_table('bucket_1', 'schema_1', 'table_1', bucket_prefix='/prefix_1')

    def test_init(self):
        self.assertTrue(re.match(
            r'^s3://bucket_1/prefix_1/schema_1_export/table_1/[0-9]{8}_[0-9]{6}/$', self.bundle_2.url))
