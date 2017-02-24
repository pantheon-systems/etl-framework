"""tests postgresql_result"""

import unittest
from mock import MagicMock

from gcloud.results.bigquery import BigqueryResult
from gcloud.results.bigquery import bigquery_response_to_rows

class BigqueryResultTestCases(unittest.TestCase):
    """class for results tests"""

    def setUp(self):
        """stuff"""

        self.mocked_config = MagicMock()
        self.mocked_schema = MagicMock()
        self.mocked_config.schema = self.mocked_schema

        self.result = BigqueryResult(config=self.mocked_config)

    def test_raw_result(self):
        """tests raw result method"""

        self.result.raw_result()

        self.mocked_schema.datastore.query.assert_called()

    def test_bigquery_response_to_rows(self):

        mock_response = {
            "schema": {
                "fields": [
                    {"name": "field1"},
                    {"name": "field2"}
                ]
            },
            "rows": [{
                "f": [
                    {"v": "a"},
                    {"v": 0}
                ]
            }]
        }

        expected_output = [{"field1": "a", "field2": 0}]
        output = bigquery_response_to_rows(mock_response)

        self.assertEqual(expected_output, output)
