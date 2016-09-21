"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.datastores.mysql_database import MySqlDatabase
from loader_mixins.SetConfigMixin import SetConfigMixin

class MySqlSchemaModifier(MySqlDatabase,
                        SetConfigMixin):
    """This inherits from a datastore class and uses SetConfigMixin"""

    def __init__(self, config=None, *args, **kwargs):
        """initializes base data loader"""

        self.config = None

        super(MySqlSchemaModifier, self).__init__(*args, **kwargs)

        self.set_config_and_credentials(config)

    def set_credentials_from_config(self):
        """stuff"""

        self.set_credentials_from_dsn(self.config.get_dsn())

    def drop_table(self):
        """stuff"""

        self.run_statement(self.config.get_drop_table_statement()[1], commit=True)

    def create_table(self):
        """stuff"""

        self.run_statement(self.config.get_create_table_statement()[1], commit=True)

    def create_table_if_not_exists(self):
        """stuff"""

        self.run_statement(self.config.get_create_table_statement(if_not_exists=True)[1],
            commit=True)

    def recreate_table(self):
        """stuff"""

        self.drop_table()
        self.create_table()

    def truncate_table(self):
        """ stuff """

        self.run_statement(self.config.get_truncate_table_statement()[1], commit=True)
