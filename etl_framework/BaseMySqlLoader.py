"""Base class to load data into mysql database"""
# pylint: disable=too-many-arguments
#for some reason, pylint doesn't pick up MySQLdb attributes
# pylint: disable=no-member
# pylint: disable=relative-import
#for some reason pylint doesn't pick up MySQLdb ProgrammingError attribute
# pylint: disable=no-name-in-module
# pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

import MySQLdb
from MySQLdb import ProgrammingError

from BaseDataLoader import BaseDataLoader

class BaseMySqlLoader(BaseDataLoader):
    """loads data into database"""

    def __init__(self, *args, **kwargs):
        """initializes base loader"""

        #self.host = None
        #self.port = None
        #self.user = None
        #self.passwd = None
        #self.database = None

        #this will run set_db_credentials
        super(BaseMySqlLoader, self).__init__(*args, **kwargs)

    @staticmethod
    def parse_dsn(dsn):
        """
        parse sql_url and outputs credential parameters
        """

        url_segments = dsn.split(':')
        user = url_segments[1].replace('/', '')
        password, host = url_segments[2].split('@')
        port, database = url_segments[3].split('/')

        port = int(port)

        return (host, port, user, password, database)

    @classmethod
    def create_from_dsn(cls, dsn):
        """creates instance of data_loader from dsn (i.e. a sql url)"""

        return cls(cls.parse_dsn(dsn))

    def set_db_credentials(self, db_credentials):
        """sets the credentials"""

        if len(db_credentials) != 5:
            raise Exception('Db credentials must be array of 5 items. %d given'%len(db_credentials))

        self.db_credentials = db_credentials

        #self.host = db_host
        #self.port = db_port
        #self.user = db_user
        #self.passwd = db_password
        #self.database = db_database

    def clear_connection(self):
        """closes existing connection object and resets self.con to None"""

        #if no connection saved, do nothing
        if not self.con:
            return

        try:
            self.con.close()
        except ProgrammingError:
            print 'Saved connection was already closed'

        self.con = None

    def _create_connection(self):
        """
        returns new sql connection object
        """
        con = MySQLdb.connect(host=self.db_credentials[0],
                              port=self.db_credentials[1],
                              user=self.db_credentials[2],
                              passwd=self.db_credentials[3],
                              db=self.db_credentials[4],
                              charset='utf8')

        return con

