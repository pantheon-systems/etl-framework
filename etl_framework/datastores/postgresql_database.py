"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
# pylint: disable=no-init
import psycopg2

from etl_framework.datastore_interfaces.sql_database_interface import SqlDatabaseInterface

class PostgreSqlDatabase(SqlDatabaseInterface):
    """loads data into database"""

    CONNECTION_GONE_EXCEPTION = psycopg2.OperationalError
    CONNECTION_CLOSED_EXCEPTION = psycopg2.ProgrammingError

    def _create_connection(self):
        """
        returns new sql connection object
        """

        return psycopg2.connect(
            host=self.credentials[0],
            port=self.credentials[1],
            user=self.credentials[2],
            password=self.credentials[3],
            database=self.credentials[4],
        )
