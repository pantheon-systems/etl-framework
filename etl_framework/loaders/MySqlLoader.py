"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated
#pylint: disable=super-on-old-class

from etl_framework.datastores.mysql_database import MySqlDatabase
from etl_framework.BaseLoader import BaseLoader


class MySqlLoader(
    BaseLoader
):
    """loads data into database"""

    def __init__(self, config, *args, **kwargs):

        super(MySqlLoader, self).__init__(config, *args, **kwargs)

        self.datastore = MySqlDatabase(config.credentials)
