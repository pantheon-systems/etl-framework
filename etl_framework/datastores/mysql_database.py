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

        # Unix socket connection
        if len(self.credentials) == 4:

            return MySQLdb.connect(
                user=self.credentials[0],
                passwd=self.credentials[1],
                unix_socket=self.credentials[2],
                db=self.credentials[3],
                charset='utf8'
            )

        # Tcp connection
        elif len(self.credentials) == 5:

            return MySQLdb.connect(
                host=self.credentials[0],
                port=self.credentials[1],
                user=self.credentials[2],
                passwd=self.credentials[3],
                db=self.credentials[4],
                charset='utf8'
            )
