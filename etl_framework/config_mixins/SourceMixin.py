"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class SourceMixin(object):
    """parses configuration files"""

    SOURCE_SYNC_FIELD_ATTR = 'source_sync_field'
    SOURCE_TABLE_ATTR = 'source_table'
    SOURCE_DELETE_FIELD_ATTR = 'source_delete_field'
    SOURCE_UNIQUE_FIELDS_ATTR = 'source_unique_fields'

    @check_config_attr_default_none
    def get_source_sync_field(self):
        """gets field that can be used for datetime cutoff"""

        return self.config[self.SOURCE_SYNC_FIELD_ATTR]

    @check_config_attr_default_none
    def get_source_table(self):
        """stuff"""

        return self.config[self.SOURCE_TABLE_ATTR]

    @check_config_attr_default_none
    def get_source_delete_field(self):
        """gets field saying whether row deleted"""

        return self.config[self.SOURCE_DELETE_FIELD_ATTR]

    @check_config_attr_default_none
    def get_source_unique_fields(self):
        """gets fields that must be unique"""

        return self.config[self.SOURCE_UNIQUE_FIELDS_ATTR]

