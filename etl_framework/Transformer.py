"""Base class to transform extracted data before loading"""
#pylint: disable=relative-import

import abc

from etl_framework.etl_class import EtlClass

class Transformer(EtlClass, metaclass=abc.ABCMeta):
    """transforms extracted data and filters"""

    @abc.abstractmethod
    def iter_transform(self, row):
        """stuff"""
