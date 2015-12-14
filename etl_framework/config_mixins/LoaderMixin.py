"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class LoaderMixin(object):
    """parses configuration files"""

    LOADER_FIELDS_ATTR = 'loader_fields'
    LOADER_TABLE_ATTR = 'loader_table'
    LOADER_DELETE_FIELD_ATTR = 'loader_delete_field'
    LOADER_SYNC_FIELD_ATTR = 'loader_sync_field'

    @check_config_attr_default_none
    def get_loader_fields(self):
        """target fields is dict of field names and datatypes"""

        return self.config[self.LOADER_FIELDS_ATTR]

    @check_config_attr_default_none
    def get_loader_table(self):
        """stuff"""

        return self.config[self.LOADER_TABLE_ATTR]

    @check_config_attr_default_none
    def get_loader_delete_field(self):
        """stuff"""

        return self.config[self.LOADER_DELETE_FIELD_ATTR]

    @check_config_attr_default_none
    def get_loader_sync_field(self):
        """stuff"""

        return self.config[self.LOADER_SYNC_FIELD_ATTR]
