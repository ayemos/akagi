from collections import namedtuple

from osho.dataset import LabeledDataset


class LabeledImageDataset(LabeledDataset):
    _LabeledImageData = namedtuple('LabeledImageData', ['image', 'label'])


class S3LabeledImageDataset(LabeledImageDataset):
    def __init__(self, bucket, prefix, options={}):
        super(LabeledImageDataset, self).__init__()

        self._i = 0

        self._bucket = bucket
        self._prefix = prefix

        self._labels = []
        self._image_labels = []
        self._image_keys = []

        self._s3_client = None
        self._s3_resource = None

        if not self._prefix.endswith('/'):
            self._prefix += '/'

        self._crawl_bucket(self._bucket, self._prefix)

    def _crawl_bucket(self, bucket, prefix):
        paginator = self._client.get_paginator('list_objects_v2')
        result = paginator.paginate(
                Bucket=self._bucket,
                Delimiter='/',
                Prefix=self._prefix)

        subdirs = []
        for prefix in result.search('CommonPrefixes'):
            subdirs.append(prefix.get('Prefix'))

        if len(subdirs) < 2:
            raise Exception("Given prefix must result in at least two common prefixes.")

        self._image_labels = []
        self._image_keys = []
        self._labels = []

        for subdir in subdirs:
            # trim trailing slash
            if subdir.endswith('/'):
                subdir = subdir[:-1]

            self._labels.append(os.path.basename(subdir))
            label_index = len(self._labels) - 1

            objects = self._resource.Bucket(bucket).objects.filter(
                    Prefix=subdir
                    )

            for obj in objects:
                self._image_keys.append(obj.key)
                self._image_labels.append(label_index)

    def __len__(self):
        return len(self._image_keys)

    def labels(self):
        return self._labels

    def data_at(self, i):
        image = self._resource.Bucket(self._bucket).Object(self._image_keys[i]).get()
        # TODO: Handle Timeout Errors
        return image['Body'].read()

    def label_at(self, i):
        return self._image_labels[i]

    def __next__(self):
        i = self._i

        if i == len(self._image_keys):
            raise StopIteration()

        self._i += 1

        return self._LabeledImageData(self.data_at(i), self.label_at(i))

    @property
    def _client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client('s3')

        return self._s3_client

    @property
    def _resource(self):
        if self._s3_resource is None:
            self._s3_resource = boto3.resource('s3')

        return self._s3_resource

class LocalLabeledImageDataset(LabeledImageDataset):
    def __init__(self, image_dir, options={}):
        super(LabeledImageDataset, self).__init__()

        self._image_paths = []
        self._image_labels = []
        self._labels = []

        self._i = 0
        self._image_dir = image_dir
        self._load_dir(self._image_dir)

    def _load_dir(self, path):
        if os.path.exists(path) and os.path.isdir(path):
            subdirs = []

            for filename in os.listdir(path):
                subdir = os.path.join(os.path.abspath(path), filename)

                if os.path.isdir(subdir):
                    subdirs.append(subdir)
        else:
            raise Exception("Invalid path: %(path)s" % locals())

        if len(subdirs) < 2:
            raise Exception("Directory must contain at least two subdirectories.")

        self._image_labels = []
        label_index = 0
        lines = []

        for subdir in subdirs:
            self._labels.append(os.path.basename(subdir))
            label_index = len(self._labels) - 1

            for root, dirs, filenames in os.walk(subdir, followlinks=True):
                for filename in filenames:
                    # TODO: filter on image type?
                    self._image_paths.append(os.path.join(path, subdir, root, filename))
                    self._image_labels.append(label_index)

    def __len__(self):
        return len(self._image_paths)

    def labels(self):
        return self._labels

    def data_at(self, i):
        # image preprocessing/filtering?
        f = open(self._image_paths[i])
        return f.read()

    def label_at(self, i):
        return self._image_labels[i]

    def __next__(self):
        i = self._i

        if i == len(self._image_paths):
            raise StopIteration()

        self._i += 1

        return self._LabeledImageData(self.data_at(i), self.label_at(i))
