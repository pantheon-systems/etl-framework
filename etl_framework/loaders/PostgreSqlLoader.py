"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.datastores.postgresql_database import PostgreSqlDatabase
from etl_framework.BaseLoader import BaseLoader

class PostgreSqlLoader(
    BaseLoader,
    PostgreSqlDatabase,
):
    """loads data into database"""
