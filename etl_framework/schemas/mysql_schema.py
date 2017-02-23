"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated
#pylint: disable=super-on-old-class

from etl_framework.datastores.mysql_database import MySqlDatabase
from etl_framework.schemas.schema_interface import SchemaInterface

class MySqlSchema(
    SchemaInterface
):
    """This inherits from a datastore class and uses SetConfigMixin"""

    def __init__(self, config, *args, **kwargs):

        super(MySqlSchema, self).__init__(config, *args, **kwargs)

        self.datastore = MySqlDatabase(config.credentials)

    def delete(self):
        """stuff"""

        self.datastore.run_statement(
            self.config.get_drop_table_statement()[1],
            commit=True
        )

    def create(self):
        """stuff"""

        self.datastore.run_statement(
            self.config.get_create_table_statement()[1],
            commit=True
        )

    def create_if_not_exists(self):
        """stuff"""

        self.datastore.run_statement(
            self.config.get_create_table_statement(if_not_exists=True)[1],
            commit=True
        )

    def delete_if_exists(self):
        """stuff"""

        # Drop statement already only drops if it doesnt exist
        self.datastore.run_statement(
            self.config.get_drop_table_statement()[1],
            commit=True
        )

    def recreate(self):
        """stuff"""

        self.drop_table()
        self.create_table()

    def truncate(self):
        """ stuff """

        self.datastore.run_statement(
            self.config.get_truncate_table_statement()[1],
            commit=True
        )
