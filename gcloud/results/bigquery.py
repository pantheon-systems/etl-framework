"""result for mysql data"""

from etl_framework.testing.results import ResultInterface

class BigqueryResult(ResultInterface):
    """result for mysql tables"""

    def raw_result(self):
        """returns raw result"""

        # Hack to access db object of schema
        # Gets all the rows from a table
        response = self.schema.datastore.query(
            "SELECT * FROM [{dataset}.{table}]".format(
	    dataset=self.schema.bigquery_dataset_id,
            table=self.schema.bigquery_table_id,
        ))

        return bigquery_response_to_rows(response)

def bigquery_response_to_rows(response):

    rows = response["rows"]
    fields = response["schema"]["fields"]

    return [
        {field['name']: value["v"]
            for field, value in zip(fields, row["f"])
        }
        for row in rows
    ]
