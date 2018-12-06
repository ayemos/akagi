from __future__ import absolute_import

# -*- coding: utf-8 -*-
__author__ = """Yuichiro Someya"""
__email__ = 'ayemos.y@gmail.com'
__version__ = '0.4.1'


import os
import boto3

from akagi import data_file

from akagi import iterators

from akagi import content
from akagi import contents


def home():
    d = os.getenv('AKAGI_HOME', os.path.expanduser('~/.akagi'))

    try:
        os.makedirs(d)
    except OSError:
        if not os.path.isdir(d):
            raise

    return d


_s3 = None


def get_resource(name):
    if name == 's3':
        global _s3

        if _s3 is None:
            _s3 = boto3.resource('s3')

        return _s3
    else:
        raise Exception("Invalid resouce name %(name)s" % locals())
