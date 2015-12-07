"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfig import BaseConfig
from config_mixins.DestinationMixin import DestinationMixin
from config_mixins.FieldSplitsMixin import FieldSplitsMixin
from config_mixins.FieldMappingsMixin import FieldMappingsMixin
from config_mixins.FiltersMixin import FiltersMixin

class TransformerConfig(BaseConfig,
                            DestinationMixin,
                            FieldSplitsMixin,
                            FieldMappingsMixin,
                            FiltersMixin):
    """parses configuration files"""
