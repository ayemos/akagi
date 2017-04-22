from itertools import chain


class DataFileBundle(object):
    '''DataFileBundle is an base class of all data file bundles
    '''

    def data_files(self):
        raise NotImplementedError

    def __iter__(self):
        return iter(chain(*self.data_files))

    def __enter__(self):
        return self

    def __exit__(self, *exc_type):
        self.clear()
        return False

    def clear(self):
        raise NotImplementedError
