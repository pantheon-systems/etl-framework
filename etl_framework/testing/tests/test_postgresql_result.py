"""tests postgresql_result"""
import os

import unittest
from mock import MagicMock

from etl_framework.datastores.postgresql_database import PostgreSqlDatabase
from etl_framework.configs.environment import EnvironmentConfig
from etl_framework.SqlSchemaConfig import SqlSchemaConfig
from etl_framework.testing.postgresql_result import PostgreSqlResult
from etl_framework.schemas import postgresql_schema

class PostgreSqlResultTestCases(unittest.TestCase):
    """class for results tests"""

    FIXTURES_DIR = os.path.join(
        os.path.dirname(__file__),
        'fixtures/'
    )

    SCHEMA_FILEPATH = os.path.join(
        FIXTURES_DIR,
        'schema.postgresql.json'
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
        config = SqlSchemaConfig.create_from_filepath(self.SCHEMA_FILEPATH, environment=self.env)
        self.schema = config.create(etl_classes=postgresql_schema)

    def test_raw_result(self):
        """tests raw result method"""

        mock_run_statement = MagicMock()
        PostgreSqlDatabase.run_statement = mock_run_statement
        mock_run_statement.return_value = (
            ((2L, u'value1'), ),
            ['field2', 'field1']
        )

        config = MagicMock()
        config.schema = self.schema
        config.expected_result = None
        config.match_type = "exact"
        result = PostgreSqlResult(
            config=config
        )

        raw_result = result.raw_result()

        self.assertEqual([{'field1': 'value1', 'field2': 2}], raw_result)
