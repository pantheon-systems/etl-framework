"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from MySqlDatabase import MySqlDatabase
from loader_mixins.SetConfigMixin import SetConfigMixin

class MySqlSchemaModifier(MySqlDatabase,
                        SetConfigMixin):
    """stuff"""

    def __init__(self, config=None, *args, **kwargs):
        """initializes base data loader"""

        self.config = None

        super(MySqlSchemaModifier, self).__init__(*args, **kwargs)

        self.set_config(config)

    def drop_table(self):
        """stuff"""

        self.run_statement(self.config.get_drop_table_statement(), commit=True)

    def create_table(self):
        """stuff"""

        self.run_statement(self.config.get_create_table_statement(), commit=True)

    def recreate_table(self):
        """stuff"""

        self.drop_table()
        self.create_table()

