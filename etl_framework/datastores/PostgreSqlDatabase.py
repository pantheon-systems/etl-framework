"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
# pylint: disable=no-init
import psycopg2

from SqlDatabase import SqlDatabase

class PostgreSqlDatabase(SqlDatabase):
    """loads data into database"""

    CONNECTION_GONE_EXCEPTION = psycopg2.OperationalError
    CONNECTION_CLOSED_EXCEPTION = psycopg2.ProgrammingError

    def _create_connection(self):
        """
        returns new sql connection object
        """

        return psycopg2.connect(
            host=self.db_credentials[0],
            port=self.db_credentials[1],
            user=self.db_credentials[2],
            password=self.db_credentials[3],
            database=self.db_credentials[4],
        )
