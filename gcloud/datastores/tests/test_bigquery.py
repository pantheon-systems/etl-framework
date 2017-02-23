"""tests bigquery client"""

import unittest

from gcloud.datastores.bigquery import BigqueryClient

class BigqueryClientTestCases(unittest.TestCase):
    """stuff"""

    @classmethod
    def setUpClass(cls):

        cls.project_id = 'pantheon-dev'
        cls.dataset_id = 'etl_test'
        cls.table_id = 'etl_test'
        cls.table_schema = {
            "fields": [
                {
                    "type": "STRING",
                    "name": "a_key",
                    "mode": "REQUIRED",
                }
            ]
        }

        cls.rows = [
            {
                "insertId": "some_uuid",
                "json": {
                    "a_key": "a_value"
                },
            },
        ]

        cls.query = "SELECT a_key FROM [{}:{}.{}]".format(
            cls.project_id,
            cls.dataset_id,
            cls.table_id,
        )

        cls.client = BigqueryClient(
            project_name=cls.project_id,
            dataset_id=cls.dataset_id
        )

        # Create a dataset and table (this indirectly tests create and delete)
        cls.client.insert_dataset(cls.dataset_id)
        cls.client.insert_table(
            table_id=cls.table_id,
            schema=cls.table_schema
        )

    @classmethod
    def tearDownClass(cls):

        # Remove table and dataset (this indirectly tests create and delete)
        cls.client.delete_table(cls.table_id)
        cls.client.delete_dataset(cls.dataset_id)

    def test_get_dataset(self):

        self.client.get_dataset(self.dataset_id)

    def test_get_table(self):

        self.client.get_table(self.table_id)

    def test_insert_data(self):

        self.client.insert_data(
            table_id=self.table_id,
            rows=self.rows
        )

    def test_list_data(self):

        self.client.list_data(
            table_id=self.table_id
        )

    def test_list_datasets(self):

        self.client.list_datasets()

    def test_list_tables(self):

        self.client.list_tables(
            dataset_id=self.dataset_id
        )

    def test_patch_table(self):

        self.client.patch_table(
            table_id=self.table_id,
            schema=self.table_schema,
        )

    def test_query(self):

        self.client.query(
            query=self.query,
        )

    def test_update_table(self):

        self.client.update_table(
            table_id=self.table_id,
            schema=self.table_schema,
        )
