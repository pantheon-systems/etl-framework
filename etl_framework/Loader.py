"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from MySqlDatabase import MySqlDatabase

class Loader(MySqlDatabase):
    """loads data into database"""

    def __init__(self, config, *args, **kwargs):
        """initializes base data loader"""

        self.config = config

        super(Loader, self).__init__(*args, **kwargs)

        self.set_db_credentials_from_config()

    def set_db_credentials_from_config(self):
        """stuff"""

        self.set_db_credentials_from_dsn(self.config.get_dsn())

    def load(self, row):
        """stuff"""

        raise NotImplementedError
