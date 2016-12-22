"""Class for data api to extract data"""
#pylint: disable=relative-import

import abc

from etl_framework.etl_class import EtlClass
from etl_framework.mixins.datastore_mixin import DatastoreMixin

class Extractor(
    EtlClass,
    DatastoreMixin
):
    """class for authenticating to api and extracting data"""

    __metaclass__ = abc.ABCMeta

    def extract(self):
        """extracts data"""

        raise NotImplementedError

    @abc.abstractmethod
    def iter_extract(self):
        """stuff"""

