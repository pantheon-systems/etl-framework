"""Class for data api to extract data"""
#pylint: disable=relative-import

import abc

from etl_framework.etl_class import EtlClass

class Extractor(EtlClass):
    """class for authenticating to api and extracting data"""

    __metaclass__ = abc.ABCMeta

    def get_credentials(self):
        """gets credentials"""

        return self.config.get_credentials()

    def extract(self):
        """extracts data"""

        raise NotImplementedError

    @abc.abstractmethod
    def iter_extract(self):
        """stuff"""

