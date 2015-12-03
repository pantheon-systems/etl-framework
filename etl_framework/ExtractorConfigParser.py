"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from config_mixins.ConfigurationParser import ConfigurationParser
from config_mixins.DestinationConfigMixin import DestinationConfigMixin
from config_mixins.FieldMappingsConfigMixin import FieldMappingsConfigMixin
from config_mixins.SourceConfigMixin import SourceConfigMixin

class ExtractorConfigParser(ConfigurationParser,
                            FieldMappingsConfigMixin,
                            SourceConfigMixin):
    """parses configuration files"""
