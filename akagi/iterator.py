from abc import ABCMeta, abstractmethod
from akagi.iterators import CSVIterator, BinaryIterator
import six
import collections


@six.add_metaclass(ABCMeta)
class Iterator(collections.abc.Iterator):
    @abstractmethod
    def decode(self, content):
        '''Return decoded content.'''

    @classmethod
    def get_iterator_class(cls, file_format):
        if file_format.lower() == 'csv':
            return CSVIterator
        elif file_format.lower() == 'binary':
            return BinaryIterator
        else:
            raise Exception("Unsupported file format %(file_format)s." % locals())
