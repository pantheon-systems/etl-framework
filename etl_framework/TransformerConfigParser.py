"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from config_mixins.ConfigurationParser import ConfigurationParser
from config_mixins.DestinationConfigMixin import DestinationConfigMixin
from config_mixins.FieldSplitsConfigMixin import FieldSplitsConfigMixin
from config_mixins.FieldMappingsConfigMixin import FieldMappingsConfigMixin

class TransformerConfigParser(ConfigurationParser,
                            DestinationConfigMixin,
                            FieldSplitsConfigMixin,
                            FieldMappingsConfigMixin):
    """parses configuration files"""

