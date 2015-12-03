"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from config_mixins.ConfigurationParser import ConfigurationParser
from config_mixins.DestinationConfigMixin import DestinationConfigMixin
from config_mixins.TargetConfigMixin import TargetConfigMixin
from config_mixins.FieldMappingsConfigMixin import FieldMappingsConfigMixin

class LoaderConfigParser(ConfigurationParser,
                            DestinationConfigMixin,
                            FieldMappingsConfigMixin,
                            TargetConfigMixin):
    """parses configuration files"""

