"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from ConfigurationParser import ConfigurationParser
from DestinationConfigMixIn import DestinationConfigMixIn
from TargetConfigMixIn import TargetConfigMixIn
from FieldMappingsConfigMixIn import FieldMappingsConfigMixIn

class LoaderConfigParser(ConfigurationParser,
                            DestinationConfigMixIn,
                            FieldMappingsConfigMixIn,
                            TargetConfigMixIn):
    """parses configuration files"""

