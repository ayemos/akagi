import os
import boto3


class S3Helper(object):
    @classmethod
    def resource(cls):
        if getattr(cls, '_S3RESOURCE', None) is None:
            cls._S3RESOURCE = boto3.resource('s3')

        return cls._S3RESOURCE

    @classmethod
    def client(cls):
        if getattr(cls, '_S3ClIENT', None) is None:
            cls._S3CLIENT = boto3.client('s3')

        return cls._S3CLIENT

    @classmethod
    def keys(cls, bucket, prefix, **options):
        pass

    @classmethod
    def crawl_bucket(cls, bucket, prefix):
        paginator = cls.s3client().get_paginator('list_objects_v2')
        result = paginator.paginate(
                Bucket=bucket,
                Delimiter='/',
                Prefix=prefix)

        subdirs = []
        for prefix in result.search('CommonPrefixes'):
            subdirs.append(prefix.get('Prefix'))

        if len(subdirs) < 2:
            raise Exception("Given prefix must result in at least two common prefixes.")

        label_names = []
        labels = []
        keys = []

        for subdir in subdirs:
            # trim trailing slash
            if subdir.endswith('/'):
                subdir = subdir[:-1]

            label_names.append(os.path.basename(subdir))
            label_index = len(label_names) - 1

            objects = cls.s3resource().Bucket(bucket).objects.filter(
                    Prefix=subdir
                    )

            for obj in objects:
                keys.append(obj.key)
                labels.append(label_index)

        return labels, keys, label_names
