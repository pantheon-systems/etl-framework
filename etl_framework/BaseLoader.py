"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.etl_class import EtlClass
from etl_framework.mixins.datastore_mixin import DatastoreMixin

class BaseLoader(EtlClass, DatastoreMixin):
    """loads data into database"""

    def load(self, row):
        """stuff"""

        raise NotImplementedError
