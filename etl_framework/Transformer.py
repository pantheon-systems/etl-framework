"""Base class to transform extracted data before loading"""
#pylint: disable=relative-import

import abc

from method_wrappers.check_attr_set import _check_attr_set

class Transformer(object):
    """transforms extracted data and filters"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        """stuff"""

        self.config = config

    @abc.abstractmethod
    def iter_transform(self, row):
        """stuff"""
