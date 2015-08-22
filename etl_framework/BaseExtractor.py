"""Class for data api to extract data"""
#pylint: disable=relative-import

import abc

from method_wrappers.run_check import _run_check

class BaseExtractor(object):
    """class for authenticating to api and extracting data"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, auth_credentials, *args, **kwargs):
        """initialize data api object"""

        super(BaseExtractor, self).__init__(*args, **kwargs)

        self.auth_credentials = auth_credentials
        self.is_authenticated = False

    def reset_auth_credentials(self, auth_credentials):
        """sets credentials to new values"""

        self.auth_credentials = auth_credentials

    @abc.abstractmethod
    def authenticate(self):
        """authenticates to api object"""

        if not self.is_authenticated:
            #do some stuff
            pass

        self.is_authenticated = True

    @abc.abstractmethod
    @_run_check('authenticate')
    def extract_data(self, params):
        """extracts data with given params"""

