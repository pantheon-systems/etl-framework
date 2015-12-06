"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments

import MySQLdb
from MySQLdb import ProgrammingError

from SqlDatabase import SqlDatabase

class MySqlDatabase(SqlDatabase):
    """loads data into database"""

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

        return MySQLdb.connect(host=self.db_credentials[0],
                              port=self.db_credentials[1],
                              user=self.db_credentials[2],
                              passwd=self.db_credentials[3],
                              db=self.db_credentials[4],
                              charset='utf8')
