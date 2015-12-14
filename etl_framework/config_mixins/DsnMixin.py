"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class DsnMixin(object):
    """parses configuration files"""

    DSN_ATTR = 'dsn'

    @check_config_attr_default_none
    def get_dsn(self):
        """yup"""

        return self.config[self.DSN_ATTR]

    @check_config_attr_default_none
    def set_dsn(self, dsn):
        """yup"""

        self.config[self.DSN_ATTR] = dsn
