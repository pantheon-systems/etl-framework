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
        phrases = [update_clause, update_values]

        # Statement fields are fields + where_fields
        statement_fields = list(fields)

        if where_fields is not None:
            where_phrase = list()
            for field in where_fields:
                # Field values is an explicit override in a where clause,
                # used for more fine grained control of where_fields
                # Does not add a field to statement_fields.
                where_phrase.append(' '.join([field['field_name'], field['operator'], field.get('field_value', '%s')]))
            where_phrase = SqlClause(
                header='WHERE',
                phrases=where_phrase,
                phrase_separator=' AND\n'
            )
            for field in where_fields:
                if field.get('field_value') is None:
                    statement_fields.append(field["field_name"])
            phrases.append(where_phrase)


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
