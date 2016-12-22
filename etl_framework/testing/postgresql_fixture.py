"""fixture for mysql data"""

from etl_framework.testing.fixtures import FixtureInterface
from etl_framework.config_mixins.InsertStatementMixin import \
    PostgreSqlInsertStatementMixin

class PostgreSqlFixture(FixtureInterface):
    """fixture for mysql tables"""

    def load(self):
        """insert data"""

        table = self.schema.config.get_table()

        # Inserts each row separately because we assume fields can be different
        # in each row.  If we don't want to allow this, we can get rid of this
        # for loop.
        for row in self.data:
            fields, values = zip(*row.iteritems())
            stmnt_fields, statement = PostgreSqlInsertStatementMixin.create_insert_statement(
                table,
                fields,
                statement_string=True,
            )

            # Hack to access db object of schema
            self.schema.datastore.run_statement(
                statement,
                params=[row[field] for field in stmnt_fields],
                commit=True
            )
