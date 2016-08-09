"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.SqlClause import SqlClause

class UpdateStatementMixin(object):
    """requires LoaderMixin"""

    @staticmethod
    def create_update_statement(table, fields, where_fields, statement_string=False):
        """returns update statement"""

        update_field_phrases = [field + ' = %s' for field in fields]
        update_clause = SqlClause(header='UPDATE {}'.format(table))
        update_values = SqlClause(header='SET', phrases=update_field_phrases)
        where_phrase = SqlClause(
            header='WHERE',
            phrases=[field + ' = %s' for field in where_fields],
            phrase_separator=' AND\n'
        )

        # Statement fields are fields + where_fields
        statement_fields = fields + where_fields

        if statement_string:
            return statement_fields, SqlClause(phrases=[update_clause, update_values, where_phrase],
                                    phrase_indents=0,
                                    phrase_separator='\n').get_sql_clause()
        else:
            return statement_fields, SqlClause(phrases=[update_clause, update_values, where_phrase],
                                    phrase_indents=0,
                                    phrase_separator='\n')

    def get_update_statement(self):
        """returns statement to update data"""

        return self.create_insert_statement(table=self.get_loader_table(),
                                            fields=self.get_loader_fields(), statement_string=True)
