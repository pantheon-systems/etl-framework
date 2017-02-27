"""tests postgresql_result"""

import unittest
from mock import MagicMock

from gcloud.fixtures.bigquery import BigqueryFixture

class BigqueryFixtureTestCases(unittest.TestCase):
    """class for results tests"""

    def setUp(self):
        """stuff"""

        self.mocked_config = MagicMock()
        self.mocked_schema = MagicMock()
        self.mocked_schema.bigquery_table_id = "mock_table"
        self.mocked_config.data = "data"
        self.mocked_config.schema = self.mocked_schema

        self.fixture = BigqueryFixture(config=self.mocked_config)

    def test_load(self):
        """tests raw result method"""

        self.fixture.load()

        self.mocked_schema.datastore.insert_data.assert_called_with(
            table_id="mock_table",
            rows="data"
        )
