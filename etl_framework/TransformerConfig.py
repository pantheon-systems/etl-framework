"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint: disable=too-many-ancestors

from BaseConfig import BaseConfig
from config_mixins.AddFiltersMixin import AddFiltersMixin
from config_mixins.DestinationMixin import DestinationMixin
from config_mixins.FieldSplitsMixin import FieldSplitsMixin
from config_mixins.FieldMappingsMixin import FieldMappingsMixin
from config_mixins.FiltersMixin import FiltersMixin

class TransformerConfig(BaseConfig,
                            DestinationMixin,
                            FieldSplitsMixin,
                            FieldMappingsMixin,
                            FiltersMixin,
                            AddFiltersMixin):
    """parses configuration files"""

