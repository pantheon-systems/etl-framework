"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.SqlClause import SqlClause
from etl_framework.config_mixins.LoaderMixin import LoaderMixin
from etl_framework.config_mixins.SqlStatementMixin import SqlStatementMixin

class UpdateStatementMixin(LoaderMixin, SqlStatementMixin):
    """requires LoaderMixin and SqlStatement Mixin"""

    @staticmethod
    def create_update_statement(table, fields, where_fields=None, statement_string=False):
        """
        returns update statement

        where_fields : list of dictionaries with format:
            dictionary {"field_name": "field1", "operator": "<"}
        """

        update_field_phrases = [field + ' = %s' for field in fields]
        update_clause = SqlClause(header='UPDATE {}'.format(table))
        update_values = SqlClause(header='SET', phrases=update_field_phrases)

        if where_fields is not None:
            where_phrase = SqlClause(
                header='WHERE',
                phrases=[field["field_name"] + ' {} %s'.format(field["operator"])
                    for field in where_fields],
                phrase_separator=' AND\n'
            )

            # Statement fields are fields + where_fields
            statement_fields = fields + [field["field_name"] for field in where_fields]
            phrases = [update_clause, update_values, where_phrase]

        else:
            statement_fields = fields
            phrases = [update_clause, update_values]

        if statement_string:
            return statement_fields, SqlClause(phrases=phrases,
                                    phrase_indents=0,
                                    phrase_separator='\n').get_sql_clause()
        else:
            return statement_fields, SqlClause(phrases=phrases,
                                    phrase_indents=0,
                                    phrase_separator='\n')

    def get_update_statement(self):
        """returns statement to update data"""

        return self.create_update_statement(
            table=self.get_loader_table(),
            fields=self.get_loader_fields(),
            where_fields=self.get_sql_where_fields(),
            statement_string=True
        )
