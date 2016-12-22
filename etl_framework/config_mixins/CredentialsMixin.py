"""parses configuration and returns useful things"""

class CredentialsMixin(object):
    """parses configuration files"""

    CREDENTIALS_ATTR = 'credentials'

    @property
    def credentials_attribute(self):

        return self.config["credentials_attribute"]

    @property
    def credentials(self):

        credentials_attribute = self.credentials_attribute

        return getattr(self.environment, credentials_attribute)
