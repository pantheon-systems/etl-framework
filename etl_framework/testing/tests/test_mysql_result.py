"""tests mysql_result"""
import os

import unittest
from mock import MagicMock

from etl_framework.datastores.mysql_database import MySqlDatabase
from etl_framework.configs.environment import EnvironmentConfig
from etl_framework.SqlSchemaConfig import SqlSchemaConfig
from etl_framework.testing.mysql_result import MySqlResult
from etl_framework.schemas import mysql_schema

class MySqlResultTestCases(unittest.TestCase):
    """class for results tests"""

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

    def setUp(self):
        """stuff"""

        # NOTE these lines could just be performed by builder once that's
        # set up

        # Setup environment
        self.env = EnvironmentConfig.create_from_filepath(
            self.ENVIRONMENT_FILEPATH
        )
        self.env.set_environment()

        # Setup config
        config = SqlSchemaConfig.create_from_filepath(self.SCHEMA_FILEPATH)
        config.set_dsn(self.env.environment['mysql_dsn'])
        self.schema = config.create(etl_classes=mysql_schema)

    def test_raw_result(self):
        """tests raw result method"""

        mock_run_statement = MagicMock()
        MySqlDatabase.run_statement = mock_run_statement
        mock_run_statement.return_value = (
            ((2L, u'value1'), ),
            ['field2', 'field1']
        )

        result = MySqlResult(
            schema=self.schema,
            expected_result=None,
            match_type='exact'
        )

        raw_result = result.raw_result()

        self.assertEqual([{'field1': 'value1', 'field2': 2}], raw_result)
