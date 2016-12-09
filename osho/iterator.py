class DatasetIterator(object):
    def __init__(self, dataset):
        self.dataset = dataset

        '''
        if shuffle:
            self._order = numpy.random.permutation(len(dataset))
        else:
            self._order = None
        '''

        self.i = 0

    def __next__(self):
        i = self.i
        l = self.dataset.get_length()

        '''
        if self._order is None:
            batch = self.dataset[i:i_end]
        else:
            batch = [self.dataset[index] for index in self._order[i:i_end]]
r       '''

        if i >= l:
            raise StopIteration

        self.i += 1

        return self.dataset.get_example(i)

    next = __next__
