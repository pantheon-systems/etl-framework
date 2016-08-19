"""interface for Datastore class"""

import abc

class DatastoreInterface(object):

    def __init__(self, credentials=None, *args, **kwargs):

        super(DatastoreInterface, self).__init__(*args, **kwargs)

        self.con = None
        self.credentials = None

        if credentials:
            self.set_credentials(credentials)

    @abc.abstractmethod
    def set_credentials(self, credentials):
        """sets credentials"""

    @abc.abstractmethod
    def get_connection(self):
        """retuns a connection (new or old)"""

    @abc.abstractmethod
    def _create_connection(self):
        """returns a new connection object"""
