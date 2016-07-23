"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from loader_mixins.SetConfigMixin import SetConfigMixin

class BaseLoader(SetConfigMixin):
    """loads data into database"""

    def __init__(self, config, *args, **kwargs):
        """initializes base data loader"""

        self.config = None

        super(BaseLoader, self).__init__(*args, **kwargs)

        self.set_config_and_db_credentials(config)

    def load(self, row):
        """stuff"""

        raise NotImplementedError
