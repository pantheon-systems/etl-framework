"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from ConfigurationParser import ConfigurationParser
from DestinationConfigMixIn import DestinationConfigMixIn
from FieldMappingsConfigMixIn import FieldMappingsConfigMixIn
from SourceConfigMixIn import SourceConfigMixIn

class ExtractorConfigParser(ConfigurationParser,
                            FieldMappingsConfigMixIn,
                            SourceConfigMixIn):
    """parses configuration files"""
