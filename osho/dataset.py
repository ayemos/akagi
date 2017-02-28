import bisect
from six.moves.urllib.parse import urlparse

from osho.iterator import DatasetIterator


class Dataset(object):

    """Default implementation of all Datasets
    """

    def get_example(self, _):
        """Returns the i-th example.

        All implementations of Dataset have to override it.

        Args:
            i (int): The index of the example.

        Returns:
            The i-th example.

        """
        return NotImplementedError

    def get_length(self):
        """Returns the length of the dataset"""
        return NotImplementedError

    def __len__(self):
        return NotImplementedError

    def __iter__(self):
        return DatasetIterator(self)

    @classmethod
    def from_url(cls, urlstr):
        """Convenient factory method returns an instance of dataset's subclass
        according to a scheme of given `urlstr` argument.
        """
        url = urlparse(urlstr)

        if url.scheme == 's3':
            from osho.datasets import S3Dataset
            return S3Dataset.by_prefix(url.netloc, url.path[1:])
        # elif url.scheme == 'ftp':
        else:
            raise Exception("Unsupported scheme: %s" % url.scheme)

    # TODO: support local fs
    '''
    @classmethod
    def from_filesystem(cls, path):
        print('# TODO: support local fs')
    '''


class LinkedDataset(Dataset):
    def __init__(self, *datasets):
        self._datasets = datasets
        self._length = 0
        self._offsets = [0]

        for d in datasets:
            l = d.get_length()
            self._length += l
            self._offsets.append(self._length)

        self._offsets = self._offsets[:-1]  # last offset is not necessary

    def get_example(self, i):
        j = bisect.bisect(self._offsets, i) - 1

        if j == len(self._offsets):
            return self._datasets[-1].get_example(i - self._offsets[-1])  # Delegating ValueError to actual Dataset
        else:
            return self._datasets[j].get_example(i - self._offsets[j])

    def get_length(self):
        return self._length
