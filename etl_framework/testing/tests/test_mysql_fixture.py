"""tests mysql_fixture"""
import os
import json

import unittest
from mock import MagicMock

from etl_framework.datastores.mysql_database import MySqlDatabase
from etl_framework.config_mixins.InsertStatementMixin import MySqlInsertStatementMixin
from etl_framework.configs.environment import EnvironmentConfig
from etl_framework.SqlSchemaConfig import SqlSchemaConfig
from etl_framework.testing.mysql_fixture import MySqlFixture
from etl_framework.schemas import mysql_schema

class MySqlFixtureTestCases(unittest.TestCase):
    """class for fixtures tests"""

    FIXTURES_DIR = os.path.join(
        os.path.dirname(__file__),
        'fixtures/'
    )

    SCHEMA_FILEPATH = os.path.join(
        FIXTURES_DIR,
        'schema.mysql.json'
    )

    ENVIRONMENT_FILEPATH = os.path.join(
        FIXTURES_DIR,
        'environment.test.json'
    )

    DATA_FILEPATH = os.path.join(
        FIXTURES_DIR,
        'data.mysql.json'
    )

    def setUp(self):
        """stuff"""

        # NOTE these lines could just be performed by builder once that's
        # set up

        # Setup environment
        self.env = EnvironmentConfig.create_from_filepath(
            self.ENVIRONMENT_FILEPATH
        )
        self.env.set_environment()

        self.data = json.load(open(self.DATA_FILEPATH))

        # Setup config
        config = SqlSchemaConfig.create_from_filepath(self.SCHEMA_FILEPATH)
        config.set_dsn(self.env.environment['mysql_dsn'])

        self.schema = config.create(etl_classes=mysql_schema)

    def test_load(self):
        """tests load method"""

        mock_run_statement = MagicMock()
        MySqlDatabase.run_statement = mock_run_statement

        config = MagicMock()
        config.schema = self.schema
        config.data = self.data
        fixture = MySqlFixture(
            config=config
        )

        fixture.load()

        # Brittle way of testing that insert statement is run
        # Assumes the keys of first row in self.data are the fields to insert
        data_row = self.data[0]
        stmnt_fields, expected_statement = MySqlInsertStatementMixin.create_insert_statement(
            table=self.schema.config.get_table(),
            fields=data_row.keys(),
            statement_string=True
        )

        expected_params = [data_row[field] for field in stmnt_fields]

        mock_run_statement.assert_called_with(
            expected_statement,
            params=expected_params,
            commit=True
        )
