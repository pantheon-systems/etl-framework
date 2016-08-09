"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.datastores.PostgreSqlDatabase import PostgreSqlDatabase
from etl_framework.BaseLoader import BaseLoader

class PostgreSqlLoader(
    BaseLoader,
    PostgreSqlDatabase,
):
    """loads data into database"""

    def set_db_credentials_from_config(self):
        """stuff"""

        self.set_db_credentials_from_dsn(self.config.get_dsn())

