#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_s3_data_file
----------------------------------

Tests for `akagi.s3_data_file` module.
"""


import io
import gzip
import unittest

from akagi.iterators import CSVIterator
from akagi.data_files import S3DataFile


class TestS3DataFile(unittest.TestCase):
    class S3ObjMock(object):
        def __init__(self, key, content):
            self.key = key
            self._content = content

        def get(self):
            return {'Body': io.BytesIO(self._content)}

    def setUp(self):
        self.keys = ['key_1', 'key_2.gz']
        self.objs = [b"foo,bar,baz\n1,2,3", gzip.compress(b"foo,bar,baz\n1,2,3")]

        self.obj_1 = TestS3DataFile.S3ObjMock(self.keys[0], self.objs[0])
        self.obj_gzip = TestS3DataFile.S3ObjMock(self.keys[1], self.objs[1])

        self.s3_data_file_csv = S3DataFile(self.obj_1, CSVIterator)
        self.s3_data_file_csv_gzip = S3DataFile(self.obj_gzip, CSVIterator)

    def test_init(self):
        self.assertEqual(self.s3_data_file_csv.key, self.keys[0])
        self.assertEqual(self.s3_data_file_csv.filename, self.keys[0])
        self.assertEqual(self.s3_data_file_csv_gzip.key, self.keys[1])
        self.assertEqual(self.s3_data_file_csv_gzip.filename, self.keys[1])

        expected_rows = [r.split(',') for r in self.objs[0].decode('utf-8').split('\n')]

        for i, r in enumerate(self.s3_data_file_csv):
            for j, d in enumerate(r):
                self.assertEqual(d, expected_rows[i][j])

        for i, r in enumerate(self.s3_data_file_csv_gzip):
            for j, d in enumerate(r):
                self.assertEqual(d, expected_rows[i][j])
