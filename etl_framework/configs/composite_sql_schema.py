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

    @property
    def component_schemas(self):

        return self.config["component_schemas"]

    def compose_config(self, builder):

        self._compose_fields(builder)
        self._compose_indexes(builder)
        self._compose_unique_keys(builder)

    def _compose_unique_keys(self, builder):

        for schema_config in self.component_schemas:
            schema = builder.get_config(schema_config["schema_id"])
            ignored_unique_keys = set(schema_config["ignored_unique_keys"])
            for index in schema.unique_keys:
                if unique_key not in ignored_unique_keys:
                    self.unique_keys.append(unique_key)

    def _compose_indexes(self, builder):

        for schema_config in self.component_schemas:
            schema = builder.get_config(schema_config["schema_id"])
            ignored_indexes = set(schema_config["ignored_indexes"])
            for index in schema.indexes:
                if index not in ignored_indexes:
                    self.indexes.append(index)

    def _compose_fields(self, builder):

        for schema_config in self.component_schemas:
            schema = builder.get_config(schema_config["schema_id"])
            ignored_fields = set(schema_config["ignored_fields"])
            field_renames = schema_config["field_renames"]

            for field_name, value in schema.fields:
                if field_name not in ignored_fields:
                    field_name = field_renames.get(field_name, field_name)
                    self.fields[field_name] = value
