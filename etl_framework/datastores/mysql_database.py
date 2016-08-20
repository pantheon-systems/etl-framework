"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments

import MySQLdb

from etl_framework.datastore_interfaces.sql_database_interface import SqlDatabaseInterface

class MySqlDatabase(SqlDatabaseInterface):
    """loads data into database"""

    CONNECTION_GONE_EXCEPTION = MySQLdb.OperationalError
    CONNECTION_CLOSED_EXCEPTION = MySQLdb.ProgrammingError

    def _create_connection(self):
        """
        returns new sql connection object
        """

        return MySQLdb.connect(host=self.credentials[0],
                              port=self.credentials[1],
                              user=self.credentials[2],
                              passwd=self.credentials[3],
                              db=self.credentials[4],
                              charset='utf8')
