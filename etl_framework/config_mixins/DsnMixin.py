"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none,\
                                                            check_config_attr

class DsnMixin(object):
    """parses configuration files"""

    DSN_ATTR = 'dsn'
    DSN_TYPE = 'dsn_type'

    @check_config_attr_default_none
    def get_dsn(self):
        """yup"""

        return self.config[self.DSN_ATTR]

    @check_config_attr_default_none
    def set_dsn(self, dsn):
        """yup"""

        self.config[self.DSN_ATTR] = dsn

    @check_config_attr
    def set_dsn_from_dsn_type(self, etl_setup):
        """
        :summary: use the config to set the dsn
        :param etl_setup(object): any object which has a DSN_TYPE attribute set
        """
        self.set_dsn(getattr(etl_setup, self.config[self.DSN_TYPE]))
