"""test cases for PostgreSqlCreateTableStatementMixin"""
import re

import unittest

from etl_framework.config_mixins.postgresql_drop_table_statement_mixin import \
    PostgreSqlDropTableStatementMixin
class PostgreSqlDropTableStatementMixinTestCases(unittest.TestCase):
    """TestCases"""


    def test_create_drop_table_statement_output_is_string(self):
        """
        tests get_create_table_statement method

        NOTE we need more unittests to full cover every branch of logic
        for this method
        """

        table = "test_table"

        expected_statement = \
        """
        DROP TABLE IF EXISTS test_table
        """

        fields, statement = \
            PostgreSqlDropTableStatementMixin.create_drop_table_statement(
                table=table,
                statement_string=True,
            )

        # Ignore superfluous whitespace
        expected_statement = re.sub(r'\s+', ' ', expected_statement).strip()
        statement = re.sub(r'\s+', ' ', statement).strip()
        self.assertEqual(statement, expected_statement)
        self.assertEqual(fields, [])
