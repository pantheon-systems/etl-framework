"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class CredentialsMixin(object):
    """parses configuration files"""

    CREDENTIALS_ATTR = 'credentials'
    CREDENTIALS_TYPE = 'cred_type'

    @check_config_attr_default_none
    def get_credentials(self):
        """yup"""

        return self.config[self.CREDENTIALS_ATTR]

    @check_config_attr_default_none
    def set_credentials(self, credentials):
        """yup"""

        self.config[self.CREDENTIALS_ATTR] = credentials

    def set_credentials_from_credential_type(self, etl_setup):
        self.set_credentials(getattr(etl_setup, self.config[self.CREDENTIALS_TYPE]))
