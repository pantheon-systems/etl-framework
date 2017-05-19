""" test the environment config """
#pylint: disable=protected-access
#pylint: disable=too-many-ancestors

import os

import unittest

from etl_framework.SqlSchemaConfig import SqlSchemaConfig
from etl_framework.configs.composite_sql_schema import CompositeSqlSchemaConfig
from etl_framework.config_mixins.composite_sql_statements import CompositeMySqlStatementsConfigMixin
from etl_framework.configs.tests.fixtures import etl_module
from etl_framework.builders import Builder

class TestConfig(CompositeSqlSchemaConfig, CompositeMySqlStatementsConfigMixin):

    pass

class CompositeSqlStatementsConfigTestCases(unittest.TestCase):
    """ class for test cases """

    SCHEMA_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        '../../configs/tests/fixtures/schema.sql.json'
    )

    SCHEMA_CONFIG2_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        '../../configs/tests/fixtures/schema.sql2.json'
    )

    COMPOSITE_SCHEMA_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        '../../configs/tests/fixtures/schema.multiple_composite_sql.json'
    )

    def setUp(self):

        self.builder = Builder(etl_module=etl_module)

        self.schema_config = SqlSchemaConfig.create_from_filepath(
            self.SCHEMA_CONFIG_FILEPATH
        )

        self.schema_config2 = SqlSchemaConfig.create_from_filepath(
            self.SCHEMA_CONFIG2_FILEPATH
        )

        self.config = TestConfig.create_from_filepath(
            self.COMPOSITE_SCHEMA_CONFIG_FILEPATH
        )

        self.builder.configs[self.schema_config.identifier] = self.schema_config
        self.builder.configs[self.schema_config2.identifier] = self.schema_config2

        self.config._compose_schemas(self.builder)

    def test_create_composite_sql_table_delete_statement(self):

        fields, statement = self.config.create_composite_sql_table_delete_statement(
            ["schema.sql", "schema.sql2"],
            "id"
        )

        self.assertEqual(fields, [])

        # This is a pretty lame test
        self.assertTrue("DELETE" in statement.get_sql_clause())

    def test_create_composite_sql_table_delete_statement_with_where_clause(self):

        fields, statement = self.config.create_composite_sql_table_delete_statement(
            ["schema.sql", "schema.sql2"],
            "id",
            where_phrases=["table.field1 IS NOT NULL", "table.field2 LIKE '%a%'"]
        )

        self.assertEqual(fields, [])
        # This is a pretty lame test
        self.assertTrue("WHERE" in statement.get_sql_clause())

    def test_create_composite_sql_table_upsert_statement(self):

        fields, statement = self.config.create_composite_sql_table_upsert_statement(
            ["schema.sql", "schema.sql2"],
            "id"
        )

        self.assertEqual(fields, [])

        # This is a pretty lame test
        self.assertTrue("INSERT INTO" in statement.get_sql_clause())

    def test_create_composite_sql_table_upsert_statement_with_where_phrase(self):

        fields, statement = self.config.create_composite_sql_table_upsert_statement(
            ["schema.sql", "schema.sql2"],
            "id",
            where_phrases=["table.field1 IS NOT NULL", "table.field2 LIKE '%a%'"]
        )

        self.assertEqual(fields, [])
        # This is a pretty lame test
        self.assertTrue("WHERE" in statement.get_sql_clause())

    def test_create_composite_sql_table_upsert_statement_with_cutoff(self):

        fields, statement = self.config.create_composite_sql_table_upsert_statement(
            ["schema.sql", "schema.sql2"],
            "id",
            time_cutoff_field="test_cutoff"
        )

        self.assertEqual(fields, ["test_cutoff"])

        # This is a pretty lame test
        self.assertTrue("INSERT INTO" in statement.get_sql_clause())

    def test_create_composite_sql_table_update_statement(self):

        fields, statement = self.config.create_composite_sql_table_update_statement(
            ["schema.sql", "schema.sql2"],
            "id"
        )

        self.assertEqual(fields, [])
 
        # This is a pretty lame test
        self.assertTrue("UPDATE" in statement.get_sql_clause())

    def test_create_composite_sql_table_update_statement_with_where_phrase(self):

        fields, statement = self.config.create_composite_sql_table_update_statement(
            ["schema.sql", "schema.sql2"],
            "id",
            where_phrases=["table.field1 IS NOT NULL", "table.field2 LIKE '%a%'"]
        )

        self.assertEqual(fields, [])
        # This is a pretty lame test
        self.assertTrue("WHERE" in statement.get_sql_clause())

    def test_create_composite_sql_table_update_statement_with_cutoff(self):

        fields, statement = self.config.create_composite_sql_table_update_statement(
            ["schema.sql", "schema.sql2"],
            "id",
            time_cutoff_field="test_cutoff"
        )

        self.assertEqual(fields, ["test_cutoff"])

        # This is a pretty lame test
        self.assertTrue("UPDATE" in statement.get_sql_clause())

