"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfigParser import BaseConfigParser
from method_wrappers.check_attr_set import _check_attr_set

class SourceConfigParser(BaseConfigParser):
    """parses configuration files"""

    SOURCE_MULTIPLE_TABLES_ATTR = 'source_multiple_tables'
    SOURCE_DELETE_ATTR = 'source_delete_field'

    @_check_attr_set('config')
    def get_source_multiple_tables(self):
        """gets source multiple tables for current configuration"""

        return self.config[self.SOURCE_MULTIPLE_TABLES_ATTR]

    @_check_attr_set('config')
    def get_source_delete_field(self):
        """gets source delete fields for current configuration"""

        return self.config[self.SOURCE_DELETE_ATTR]

    @_check_attr_set('config')
    def get_target_delete_field(self):
        """gets target delete field for current configuration"""
        field_mappings = self.get_field_mappings()
        return field_mappings[self.get_source_delete_field()]
