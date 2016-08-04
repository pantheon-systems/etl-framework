"""adds Buffer functionality to Loader"""
# NOTE: this requires clear_connection method on Loader, which
# should be included as part of datastore interface(yet to be enforced)

class SetConfigMixin(object):
    """stuff"""

    def set_config(self, config):
        """THIS DOESNT CHANGE THE CONNECTION"""

        self.config = config

    def set_config_and_db_credentials(self, config):
        """sets config"""

        self.set_config(config)

        if config:
            self.set_db_credentials_from_config()
            self.clear_connection()

    def set_db_credentials_from_config(self):
        """stuff"""

        raise NotImplementedError

