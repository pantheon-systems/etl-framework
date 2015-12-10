"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class ExtractorMixin(object):
    """parses configuration files"""

    EXTRACTOR_FIELDS_ATTR = 'extractor_fields'
    EXTRACTOR_SYNC_FIELD_ATTR = 'extractor_sync_field'
    EXTRACTOR_TABLE_ATTR = 'extractor_table'
    EXTRACTOR_DELETE_FIELD_ATTR = 'extractor_delete_field'
    EXTRACTOR_UNIQUE_FIELDS_ATTR = 'extractor_unique_fields'

    @check_config_attr_default_none
    def get_extractor_fields(self):
        """stuff"""

        return self.config[self.EXTRACTOR_FIELDS_ATTR]

    @check_config_attr_default_none
    def get_extractor_sync_field(self):
        """gets field that can be used for datetime cutoff"""

        return self.config[self.EXTRACTOR_SYNC_FIELD_ATTR]

    @check_config_attr_default_none
    def get_extractor_table(self):
        """stuff"""

        return self.config[self.EXTRACTOR_TABLE_ATTR]

    @check_config_attr_default_none
    def get_extractor_delete_field(self):
        """gets field saying whether row deleted"""

        return self.config[self.EXTRACTOR_DELETE_FIELD_ATTR]

    @check_config_attr_default_none
    def get_extractor_unique_fields(self):
        """gets fields that must be unique"""

        return self.config[self.EXTRACTOR_UNIQUE_FIELDS_ATTR]

