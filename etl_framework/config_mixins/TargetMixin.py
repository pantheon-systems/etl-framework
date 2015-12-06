"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class TargetMixin(object):
    """parses configuration files"""

    TARGET_FIELDS_ATTR = 'target_fields'
    TARGET_TABLE_ATTR = 'target_table'
    TARGET_INDEXES_ATTR = 'target_indexes'
    TARGET_PRIMARY_KEY_ATTR = 'target_primary_key'
    TARGET_UNIQUE_KEYS_ATTR = 'target_unique_keys'
    TARGET_DELETE_FIELD_ATTR = 'target_delete_field'
    TARGET_SYNC_FIELD_ATTR = 'target_sync_field'
    TARGET_RECORD_UPDATED_FIELD_ATTR = 'target_record_updated_field'

    @check_config_attr_default_none
    def get_target_fields(self):
        """target fields is dict of field names and datatypes"""

        return self.config[self.TARGET_FIELDS_ATTR]

    def get_target_field_names(self):
        """stuff"""

        return self.get_target_fields().keys()

    @check_config_attr_default_none
    def get_target_table(self):
        """stuff"""

        return self.config[self.TARGET_TABLE_ATTR]

    @check_config_attr_default_none
    def get_target_indexes(self):
        """stuff"""

        return self.config[self.TARGET_INDEXES_ATTR]

    @check_config_attr_default_none
    def get_target_primary_key(self):
        """stuff"""

        return self.config[self.TARGET_PRIMARY_KEY_ATTR]

    @check_config_attr_default_none
    def get_target_unique_keys(self):
        """stuff"""

        return self.config[self.TARGET_UNIQUE_KEYS_ATTR]

    @check_config_attr_default_none
    def get_target_delete_field(self):
        """stuff"""

        return self.config[self.TARGET_DELETE_FIELD_ATTR]

    @check_config_attr_default_none
    def get_target_sync_field(self):
        """stuff"""

        return self.config[self.TARGET_SYNC_FIELD_ATTR]

    @check_config_attr_default_none
    def get_target_record_updated_field(self):
        """stuff"""

        return self.config[self.TARGET_RECORD_UPDATED_FIELD_ATTR]
