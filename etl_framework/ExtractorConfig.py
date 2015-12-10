"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfig import BaseConfig
from config_mixins.DestinationMixin import DestinationMixin
from config_mixins.ExtractorMixin import ExtractorMixin
from config_mixins.CredentialsMixin import CredentialsMixin

class ExtractorConfig(BaseConfig,
                    ExtractorMixin,
                    CredentialsMixin):
    """parses configuration files"""
