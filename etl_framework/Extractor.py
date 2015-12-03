"""Class for data api to extract data"""
#pylint: disable=relative-import

import abc

from method_wrappers.run_check import _run_check

class Extractor(object):
    """class for authenticating to api and extracting data"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, config, *args, **kwargs):
        """initialize data api object"""

        super(Extractor, self).__init__(*args, **kwargs)

        self.config = config

    def get_credentials(self):
        """gets credentials"""

        return self.config.get_credentials()

    @abc.abstractmethod
    def extract(self):
        """extracts data"""

    @abc.abstractmethod
    def iter_extract(self):
        """stuff"""

