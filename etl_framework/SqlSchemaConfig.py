"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfig import BaseConfig
from config_mixins.DropTableStatementMixin import DropTableStatementMixin
from config_mixins.CreateTableStatementMixin import CreateTableStatementMixin
from config_mixins.DsnMixin import DsnMixin

class SqlSchemaConfig(BaseConfig,
                    DropTableStatementMixin,
                    CreateTableStatementMixin,
                    DsnMixin):
    """parses configuration files"""

