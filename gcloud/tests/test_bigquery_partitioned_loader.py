"""tests bigquery loader"""
import os

import unittest
from mock import MagicMock
from mock import ANY

from etl_framework.gcloud.loaders.BigqueryPartitionedLoader import BigqueryPartitionedLoader
from etl_framework.gcloud.configs.BigqueryLoaderConfig import BigqueryLoaderConfig
from etl_framework.gcloud import filter_functions

class BigQueryLoaderTestCases(unittest.TestCase):
    """test cases for bigquery loader"""

    TEST_LOADER_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        'fixtures/loader_configurations/loader.test_bigquery.0.json'
    )

    def setUp(self):

        self.loader_config = BigqueryLoaderConfig.create_from_filepath(
            self.TEST_LOADER_CONFIG_FILEPATH
        )

        # Set a dummy partition_chooser for config to pick up
        filter_functions.choose_current_date_partition = MagicMock(
            return_value="$20160101"
        )
        self.loader_config.add_filters_from_module(filter_functions)

        self.loader = BigqueryPartitionedLoader(config=self.loader_config)

        # Dont actually insert data
        self.mock_insert_data = MagicMock()
        self.loader.insert_data = self.mock_insert_data

    def test_load_buffered(self):
        """tests load_buffered method"""

        row = {
            "field1": "test",
            "field2": 2,
        }

        expected_row = {
            "insertId": ANY,
            "json": row,
        }

        self.loader.load(row)
        self.loader.flush_buffer()

        # table_id is specified in fixture loader_configuration!!
        self.mock_insert_data.asert_called_with(
            table_id="test$20160101",
            rows=[expected_row],
        )
