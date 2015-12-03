"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.method_wrappers.check_config_attr import check_config_attr

class CredentialsMixin(object):
    """parses configuration files"""

    CREDENTIALS_ATTR = 'credentials'

    @check_config_attr
    def get_credentials(self):
        """yup"""

        return self.config[self.CREDENTIALS_ATTR]

    @check_config_attr
    def set_credentials(self, credentials):
        """yup"""

        self.config[self.CREDENTIALS_ATTR] = credentials
