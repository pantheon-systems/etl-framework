from etl_framework.testing.fixtures import FixtureInterface

class BigqueryFixture(FixtureInterface):

    def load(self):

        self.schema.datastore.insert_data(
            table_id=self.schema.bigquery_table_id,
            rows=self.data
        )
