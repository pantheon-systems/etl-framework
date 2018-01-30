"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint: disable=too-many-ancestors

from .BaseConfig import BaseConfig
from .config_mixins.AddFiltersMixin import AddFiltersMixin
from .config_mixins.LoaderMixin import LoaderMixin
from .config_mixins.BufferMixin import BufferMixin
from .config_mixins.DsnMixin import DsnMixin
from .config_mixins.SqlStatementMixin import SqlStatementMixin
from .config_mixins.CredentialsMixin import CredentialsMixin

class LoaderConfig(BaseConfig,
                    CredentialsMixin,
                    LoaderMixin,
                    BufferMixin,
                    SqlStatementMixin,
                    DsnMixin,
                    AddFiltersMixin):
    """parses configuration files"""

