"""tests bigquery loader"""
import os

import unittest
from mock import MagicMock

from gcloud.schemas.bigquery import BigquerySchema
from gcloud.configs.bigquery_schema import BigquerySchemaConfig

class BigQuerySchemaModifierTestCases(unittest.TestCase):
    """test cases for bigquery loader"""

    TEST_SCHEMA_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        'fixtures/schema_configurations/schema.test_bigquery.json'
    )

    def setUp(self):

        self.schema_config = BigquerySchemaConfig.create_from_filepath(
            self.TEST_SCHEMA_CONFIG_FILEPATH
        )

        self.schema = BigquerySchema(config=self.schema_config)

    def test_create_table(self):
        """tests create_table method"""

        self.schema.datastore.insert_table = MagicMock()
        self.schema.create()

        # NOTE attributes like table_id are defined in schema_configuration fixture
        self.schema.datastore.insert_table.assert_called_with(
            table_id="test",
            schema=self.schema.bigquery_schema,
            expiration_time=None,
            time_partitioning=True,
            time_partitioning_expiration=None,
        )

    def test_drop_table(self):
        """tests drop_table method"""

        self.schema.datastore.delete_table = MagicMock()
        self.schema.delete()

        # NOTE table_id is defined in schema_configuration fixture
        self.schema.datastore.delete_table.assert_called_with(
            table_id="test"
        )
