"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class SchemaMixin(object):
    """parses configuration files"""

    FIELDS_ATTR = 'fields'
    TABLE_ATTR = 'table'
    INDEXES_ATTR = 'indexes'
    PRIMARY_KEY_ATTR = 'primary_key'
    UNIQUE_KEYS_ATTR = 'unique_keys'
    DELETE_FIELD_ATTR = 'delete_field'
    SYNC_FIELD_ATTR = 'sync_field'
    RECORD_UPDATED_FIELD_ATTR = 'record_updated_field'

    @check_config_attr_default_none
    def get_fields(self):
        """target fields is dict of field names and datatypes"""

        return self.config[self.FIELDS_ATTR]

    def get_field_names(self):
        """stuff"""

        return self.get_fields().keys()

    @check_config_attr_default_none
    def get_table(self):
        """stuff"""

        return self.config[self.TABLE_ATTR]

    @check_config_attr_default_none
    def get_indexes(self):
        """stuff"""

        return self.config[self.INDEXES_ATTR]

    @check_config_attr_default_none
    def get_primary_key(self):
        """stuff"""

        return self.config[self.PRIMARY_KEY_ATTR]

    @check_config_attr_default_none
    def get_unique_keys(self):
        """stuff"""

        return self.config[self.UNIQUE_KEYS_ATTR]

    @check_config_attr_default_none
    def get_delete_field(self):
        """stuff"""

        return self.config[self.DELETE_FIELD_ATTR]

    @check_config_attr_default_none
    def get_sync_field(self):
        """stuff"""

        return self.config[self.SYNC_FIELD_ATTR]

    @check_config_attr_default_none
    def get_record_updated_field(self):
        """stuff"""

        return self.config[self.RECORD_UPDATED_FIELD_ATTR]