"""parses configuration and returns useful things"""
#pylint: disable=redefined-variable-type
#pylint: disable=relative-import
from etl_framework.utilities.SqlClause import SqlClause

class TruncateTableStatementMixin(object):
    """requires SchemaMixin"""

    @staticmethod
    def create_truncate_table_statement(table, statement_string=False):
        """stuff"""

        sql_clause = SqlClause(header='TRUNCATE TABLE `{0}`'.format(table))

        # Convert to string
        if statement_string:
            sql_clause = sql_clause.get_sql_clause()

        return [], sql_clause

    def get_truncate_table_statement(self):
        """stuff"""

        return  self.create_truncate_table_statement(
            table=self.get_table(),
            statement_string=True
        )
