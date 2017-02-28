import os
from six.moves.urllib.parse import urlparse


from osho.s3_helper import S3Helper
from osho.dataset import Dataset, LinkedDataset
from osho.datasets.label import StaticLabel


class TupleDataset(Dataset):
    def __init__(self, *datasets):
        if not datasets:
            raise ValueError('no datasets are given')

        length = min([dataset.get_length() for dataset in datasets])

        # XXX: Check for the lengths of the datasets?
        self._datasets = datasets
        self._length = length

    def get_example(self, i):
        items = [dataset.get_example(i) for dataset in self._datasets]
        return tuple(items)

    def get_length(self):
        return self._length

    @classmethod
    def from_url(cls, urlstr):
        url = urlparse(urlstr)

        if url.scheme == 's3':
            urls, labels = (cls._crawl_s3_bucket(url.netloc, url.path[1:]))

            ds = [TupleDataset(Dataset.from_url(u), l) for u, l in zip(urls, labels)]

            return LinkedDataset(*ds)
        # elif url.scheme == 'ftp':
        else:
            raise Exception("Unsupported scheme: %s" % url.scheme)

    @classmethod
    def _crawl_s3_bucket(cls, bucket, prefix):
        paginator = S3Helper.client().get_paginator('list_objects_v2')
        result = paginator.paginate(
                Bucket=bucket,
                Delimiter='/',
                Prefix=prefix)

        subdirs = []
        for prefix in result.search('CommonPrefixes'):
            subdirs.append(prefix.get('Prefix'))

        if len(subdirs) < 2:
            raise Exception("Given prefix must result in at least two common prefixes.")

        labels = []

        for subdir in subdirs:
            # trim trailing slash
            if subdir.endswith('/'):
                subdir = subdir[:-1]

            labels.append(StaticLabel(os.path.basename(subdir)))

        urls = ["s3://%s" % (os.path.join(bucket, subdir)) for subdir in subdirs]

        return urls, labels
