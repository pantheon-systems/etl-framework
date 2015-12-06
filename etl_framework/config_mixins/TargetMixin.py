"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class TargetMixin(object):
    """parses configuration files"""

    TARGET_FIELDS_ATTR = 'target_fields'
    TARGET_TABLE_ATTR = 'target_table'
    TARGET_DELETE_FIELD_ATTR = 'target_delete_field'
    TARGET_SYNC_FIELD_ATTR = 'target_sync_field'

    @check_config_attr_default_none
    def get_target_fields(self):
        """target fields is dict of field names and datatypes"""

        return self.config[self.TARGET_FIELDS_ATTR]

    @check_config_attr_default_none
    def get_target_table(self):
        """stuff"""

        return self.config[self.TARGET_TABLE_ATTR]

    @check_config_attr_default_none
    def get_target_delete_field(self):
        """stuff"""

        return self.config[self.TARGET_DELETE_FIELD_ATTR]

    @check_config_attr_default_none
    def get_target_sync_field(self):
        """stuff"""

        return self.config[self.TARGET_SYNC_FIELD_ATTR]
