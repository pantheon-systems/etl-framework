"""result for mysql data"""

from etl_framework.testing.results import ResultInterface

class MySqlResult(ResultInterface):
    """result for mysql tables"""

    def raw_result(self):
        """returns raw result"""

        table = self.schema.config.get_table()

        # Hack to access db object of schema
        # Gets all the rows from a table
        values, columns = self.schema.run_statement(
            "SELECT * FROM {}".format(table),
            fetch_data=True,
        )

        return [{key: val for key, val in zip(columns, value)} for value in values]
