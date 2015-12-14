"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class FieldsMixin(object):
    """parses configuration files"""

    FIELDS_ATTR = 'fields'

    @check_config_attr_default_none
    def get_fields(self):
        """gets identifier for current configuration"""

        return self.config[self.FIELDS_ATTR]
