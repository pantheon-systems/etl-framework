"""Base class to transform extracted data before loading"""
#pylint: disable=relative-import

import abc

class Transformer(object):
    """transforms extracted data and filters"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        """stuff"""

        self.config = config

    @abc.abstractmethod
    def iter_transform(self, row):
        """stuff"""
