"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated
#pylint: disable=super-on-old-class

from etl_framework.etl_class import EtlClass
from etl_framework.mixins.datastore_mixin import DatastoreMixin

class SchemaInterface(
    EtlClass,
    DatastoreMixin
):
    """loads data into database"""

    def create(self):
        """creates schema in datastore"""

        raise NotImplementedError("Creates schema in datastore")

    def delete(self):
        """delete schema in datastore"""

        raise NotImplementedError("Delete schema in datastore")

    def create_if_not_exists(self):
        """creates schema in datastore if it doesnt already exist"""

        raise NotImplementedError("Creates schema in datastore if it doesnt already exist")

    def delete_if_exists(self):
        """stuff"""

        raise NotImplementedError("Deletes schema in datastore if it exists")

    def recreate(self):
        """stuff"""

        raise NotImplementedError("First deletes then creates")
