"""Tests UpdateStatementMixin"""
# pylint: disable=no-self-use

import unittest

from mock import patch
from mock import MagicMock

from etl_framework.config_mixins.UpdateStatementMixin import UpdateStatementMixin

class UpdateStatementMixinTestCases(unittest.TestCase):
    """TestCases for UpdateStatmentMixin"""

    def test_create_update_statement_with_where_fields(self):

        table = "test"
        fields = ["field1", "field2"]
        where_fields = [
            {
                "field_name": "field3",
                "operator": ">",
            },
            {
                "field_name": "field4",
                "operator": "=",
            },
        ]

        statement_fields, statement = UpdateStatementMixin.create_update_statement(
            table=table,
            fields=fields,
            where_fields=where_fields,
            statement_string=True,
        )

        expected_statement_fields = fields + [field["field_name"] for field in where_fields]
        self.assertTrue(statement_fields == expected_statement_fields)

        for word in ["test", "field1", "field2", "field3", "field4", ">", "=", "UPDATE", "SET", "WHERE"]:
            self.assertTrue(word in statement)

    def test_create_update_statement_without_where_fields(self):

        table = "test"
        fields = ["field1", "field2"]

        statement_fields, statement = UpdateStatementMixin.create_update_statement(
            table=table,
            fields=fields,
            where_fields=None,
            statement_string=True,
        )

        expected_statement_fields = fields
        self.assertTrue(statement_fields == expected_statement_fields)

        for word in ["test", "field1", "field2", "UPDATE", "SET"]:
            self.assertTrue(word in statement)

    @patch("etl_framework.config_mixins.SqlStatementMixin.SqlStatementMixin.get_sql_where_fields")
    @patch("etl_framework.config_mixins.LoaderMixin.LoaderMixin.get_loader_table")
    @patch("etl_framework.config_mixins.LoaderMixin.LoaderMixin.get_loader_fields")
    def test_get_update_statement(self,
        mocked_get_loader_fields,
        mocked_get_loader_table,
        mocked_get_sql_where_fields):

        mocked_get_loader_fields.return_value = "mock_fields"
        mocked_get_loader_table.return_value = "mock_table"
        mocked_get_sql_where_fields.return_value = "mock_where_fields"

        UpdateStatementMixin.create_update_statement = MagicMock()

        update_statement_mixin = UpdateStatementMixin()
        update_statement_mixin.get_update_statement()

        UpdateStatementMixin.create_update_statement.assert_called_with(
            table="mock_table",
            fields="mock_fields",
            where_fields="mock_where_fields",
            statement_string=True
        )
