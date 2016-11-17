"""test cases for PostgreSqlCreateTableStatementMixin"""
import re

import unittest

from etl_framework.config_mixins.postgresql_create_table_statement_mixin import \
    PostgreSqlCreateTableStatementMixin
class PostgreSqlCreateTableStatementMixinTestCases(unittest.TestCase):
    """TestCases"""


    def test_create_create_table_statement_output_is_string(self):
        """
        tests get_create_table_statement method

        NOTE we need more unittests to full cover every branch of logic
        for this method
        """

        table = "test_table"
        primary_key = ["field1"]
        unique_keys = [["u", ["field2"]]]
        fields = {
            "field1": "character varying (255)",
            "field2": "boolean"
        }

        expected_statement = \
        """
        CREATE TABLE test_table (
            field2 boolean,
            field1 character varying (255),
            PRIMARY KEY (field1),
            CONSTRAINT u UNIQUE (field2)
        )
        """

        fields, statement = \
            PostgreSqlCreateTableStatementMixin.create_create_table_statement(
                table=table,
                fields=fields,
                primary_key=primary_key,
                unique_keys=unique_keys,
                statement_string=True,
                if_not_exists=False
            )

        # Ignore superfluous whitespace
        expected_statement = re.sub(r'\s+', ' ', expected_statement).strip()
        statement = re.sub(r'\s+', ' ', statement).strip()
        self.assertEqual(statement, expected_statement)
        self.assertEqual(fields, [])
