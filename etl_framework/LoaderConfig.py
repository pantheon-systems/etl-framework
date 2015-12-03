"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfig import BaseConfig
from config_mixins.DestinationMixin import DestinationMixin
from config_mixins.TargetMixin import TargetMixin
from config_mixins.FieldsMixin import FieldsMixin
from config_mixins.BufferMixin import BufferMixin
from config_mixins.DsnMixin import DsnMixin

class LoaderConfig(BaseConfig,
                    DestinationMixin,
                    FieldsMixin,
                    TargetMixin,
                    BufferMixin,
                    DsnMixin):
    """parses configuration files"""

