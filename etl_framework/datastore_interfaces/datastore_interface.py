"""interface for Datastore class"""

import abc

class DatastoreInterface(object):

    def __init__(self, db_credentials=None, *args, **kwargs):

        super(DatastoreInterface, self).__init__(*args, **kwargs)

        self.con = None
        self.db_credentials = None

        if db_credentials:
            self.set_db_credentials(db_credentials)

    @abc.abstractmethod
    def set_db_credentials(self, db_credentials):
        """sets db_credentials"""

    @abc.abstractmethod
    def get_connection(self):
        """retuns a connection (new or old)"""

    @abc.abstractmethod
    def _create_connection(self):
        """returns a new connection object"""
