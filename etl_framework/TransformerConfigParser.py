"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from ConfigurationParser import ConfigurationParser
from DestinationConfigMixIn import DestinationConfigMixIn
from FieldSplitsConfigMixIn import FieldSplitsConfigMixIn
from FieldMappingsConfigMixIn import FieldMappingsConfigMixIn

class TransformerConfigParser(ConfigurationParser,
                            DestinationConfigMixIn,
                            FieldSplitsConfigMixIn,
                            FieldMappingsConfigMixIn):
    """parses configuration files"""

