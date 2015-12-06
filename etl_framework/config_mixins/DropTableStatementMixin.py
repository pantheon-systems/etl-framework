"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.SqlClause import SqlClause

class DropTableStatementMixin(object):
    """requires TargetMixin and FieldsMixin"""

    @staticmethod
    def create_drop_table_statement(table, statement_string=False):
        """stuff"""

        if statement_string:
            return SqlClause(header='DROP TABLE IF EXISTS `{}`'.format(table)).get_sql_clause()
        else:
            return SqlClause(header='DROP TABLE IF EXISTS `{}`'.format(table))

    def get_drop_table_statement(self):
        """stuff"""

        return  self.create_drop_table_statement(table=self.get_target_table(),
                                                statement_string=True)
