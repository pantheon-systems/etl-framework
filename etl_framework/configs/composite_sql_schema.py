"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint: disable=super-on-old-class

from etl_framework.SqlSchemaConfig import SqlSchemaConfig
from etl_framework.interfaces.configs import CompositeConfigInterface

class CompositeSqlSchemaConfig(SqlSchemaConfig, CompositeConfigInterface):
    """parses configuration files"""

    def configure(self, builder):

        super(CompositeSqlSchemaConfig, self).configure(builder)

        self.compose_config(builder)

    @staticmethod
    def compose_config(builder):

        return self.config
