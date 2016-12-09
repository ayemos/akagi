from collections import namedtuple

class Dataset(object):
    def __len__(self):
        raise NotImplementedError

    def data_at(self, i):
        raise NotImplementedError

class LabeledDataset(Dataset):
    def labels(self):
        raise NotImplementedError

    def label_at(self, i):
        raise NotImplementedError

class LabeledImageDataset(LabeledDataset):
    _LabeledImageData = namedtuple('LabeledImageData', ['image', 'label'])

    def __init__(self, images, labels):
        super(LabeledImageDataset, self).__init__()

        if len(images) != len(labels):
            raise Exception("HOGEEEEE")

        self._images = images
        self._labels = labels

        self._i = 0

    def __len__(self):
        return len(self._images)

    def __iter__(self):
        return self

    def labels(self):
        return self._labels

    def data_at(self, i):
        return self._images[i]

    def label_at(self, i):
        return self._labels[i]

    def __next__(self):
        i = self._i

        if i == len(self._images):
            raise StopIteration()

        self._i += 1

        return self._LabeledImageData(self.data_at(i), self.label_at(i))
