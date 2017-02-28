#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_s3_dataset
----------------------------------

Tests for `osho.datasets.s3_dataset` module.
"""


import unittest
from moto import mock_s3

from osho.datasets import S3Dataset


class TestS3Dataset(unittest.TestCase):

    def setUp(self):
        self.bucket = 'dummy_bucket'
        self.images_prefix = 'dummy_images_prefix'

        self.image_keys = ["%s/%s" % (self.images_prefix, k) for k in [
            'food/1.jpg',
            'food/2.jpg',
            'nonfood/1.jpg',
            'nonfood/2.jpg',
            'nonfood/3.jpg'
            ]]

    @mock_s3
    def testByPrefix(self):
        pass

    def tearDown(self):
        pass

    def _fixture(self):
        for k in self.image_keys:
            s3.resource('s3').Object(self.bucket, k).put(Body='dummy')

