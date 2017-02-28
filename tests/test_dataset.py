#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_dataset
----------------------------------

Tests for `osho.dataset` module.
"""


import unittest

from osho.dataset import Dataset


class TestDataset(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testUnsupportedScheme(self):
        with self.assertRaises(Exception):
            Dataset.from_url('UNSUPPORTED://foo/bar')
