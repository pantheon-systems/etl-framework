"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfig import BaseConfig
from config_mixins.SchemaMixin import SchemaMixin
from config_mixins.DropTableStatementMixin import DropTableStatementMixin
from config_mixins.CreateTableStatementMixin import CreateTableStatementMixin
from config_mixins.TruncateTableStatementMixin import TruncateTableStatementMixin
from config_mixins.DsnMixin import DsnMixin
from config_mixins.postgresql_create_table_statement_mixin import PostgreSqlCreateTableStatementMixin
from config_mixins.postgresql_drop_table_statement_mixin import PostgreSqlDropTableStatementMixin

class SqlSchemaConfig(BaseConfig,
    SchemaMixin,
    DropTableStatementMixin,
    CreateTableStatementMixin,
    DsnMixin,
    TruncateTableStatementMixin):
    """parses configuration files"""

class PostgreSqlSchemaConfig(BaseConfig,
    SchemaMixin,
    PostgreSqlDropTableStatementMixin,
    PostgreSqlCreateTableStatementMixin,
    DsnMixin,
    TruncateTableStatementMixin):
    """stuff"""

