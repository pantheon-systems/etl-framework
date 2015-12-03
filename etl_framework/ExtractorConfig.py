"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfig import BaseConfig
from config_mixins.DestinationMixin import DestinationMixin
from config_mixins.FieldMappingsMixin import FieldMappingsMixin
from config_mixins.SourceMixin import SourceMixin
from config_mixins.CredentialsMixin import CredentialsMixin

class ExtractorConfig(BaseConfig,
                    FieldMappingsMixin,
                    SourceMixin,
                    CredentialsMixin):
    """parses configuration files"""
