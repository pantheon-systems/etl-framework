"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint: disable=super-on-old-class
#pylint: disable=too-many-ancestors

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

        self._compose_schemas(builder)
        self._compose_fields(builder)
        self._compose_indexes(builder)
        self._compose_unique_keys(builder)

    def _compose_schemas(self, builder):

        for schema_id, schema_config in self.component_schemas.iteritems():
            schema_config["config"] = builder.get_config(schema_id)

    def _compose_unique_keys(self, builder):

        for schema_id, schema_config in self.component_schemas.iteritems():
            schema = builder.get_config(schema_id)
            ignored_unique_keys = schema_config["ignored_unique_keys"]
            field_renames = schema_config["field_renames"]
            for unique_key in schema.unique_keys:
                if unique_key not in ignored_unique_keys:
                    unique_key[0] = field_renames.get(unique_key[0], unique_key[0])
                    unique_key[1] = [field_renames.get(field, field) for field in unique_key[1]]
                    self.unique_keys.append(unique_key)

    def _compose_indexes(self, builder):

        for schema_id, schema_config in self.component_schemas.iteritems():
            schema = builder.get_config(schema_id)
            ignored_indexes = schema_config["ignored_indexes"]
            field_renames = schema_config["field_renames"]
            for index in schema.indexes:
                if index not in ignored_indexes:
                    index[0] = field_renames.get(index[0], index[0])
                    index[1] = [field_renames.get(field, field) for field in index[1]]
                    self.indexes.append(index)

    def _compose_fields(self, builder):

        for schema_id, schema_config in self.component_schemas.iteritems():
            schema = builder.get_config(schema_id)
            ignored_fields = set(schema_config["ignored_fields"])
            field_renames = schema_config["field_renames"]

            for field_name, value in schema.fields.iteritems():
                if field_name not in ignored_fields:
                    field_name = field_renames.get(field_name, field_name)
                    self.fields[field_name] = value
