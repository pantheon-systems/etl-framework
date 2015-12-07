"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments

import MySQLdb

from SqlDatabase import SqlDatabase

class MySqlDatabase(SqlDatabase):
    """loads data into database"""

    CONNECTION_GONE_EXCEPTION = MySQLdb.OperationalError
    CONNECTION_CLOSED_EXCEPTION = MySQLdb.ProgrammingError

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
