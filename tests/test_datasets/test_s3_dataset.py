#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_s3_dataset
----------------------------------

Tests for `osho.datasets.s3_dataset` module.
"""


import unittest

from osho.datasets import S3Dataset


class TestS3Dataset(unittest.TestCase):

    def setUp(self):
        self.bucket = 'dummy_bucket'
        self.prefix = 'dummy_prefix'
        self.keys = [
                '/dummy_prefix/1.jpg',
                '/dummy_prefix/2.jpg',
                '/dummy_prefix/3.jpg'
                ]

    def testByPrefix(self):
        dataset = S3Dataset(self.bucket, self.keys)
        self.assertEqual(dataset.get_length(), 3)

    def testBucketNotExist(self):
        with self.assertRaises(Exception):
            S3Dataset.by_prefix(self.bucket, self.prefix)

    def tearDown(self):
        pass
