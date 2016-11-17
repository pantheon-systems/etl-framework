"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.datastores.mysql_database import MySqlDatabase
from etl_framework.BaseLoader import BaseLoader


class MySqlLoader(
    BaseLoader,
    MySqlDatabase,
):
    """loads data into database"""
