"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.SqlClause import SqlClause



class MySqlInsertStatementMixin(object):
    """renamed from InsertStatementMixin"""

    @staticmethod
    def create_insert_statement(table, fields, statement_string=False):
        """returns insert statement"""

        insert_clause = SqlClause(header='INSERT INTO {} ('.format(table),
                                    footer=')',
                                    phrases=['`{0}`'.format(field) for field in fields])
        insert_values = 'VALUES (' + ', '.join(['%s']*len(fields)) + ')'

        if statement_string:
            return fields, SqlClause(phrases=[insert_clause, insert_values],
                                    phrase_indents=0,
                                    phrase_separator='\n').get_sql_clause()
        else:
            return fields, SqlClause(phrases=[insert_clause, insert_values],
                                    phrase_indents=0,
                                    phrase_separator='\n')

    def get_insert_statement(self):
        """returns statement to insert data"""

        return self.create_insert_statement(table=self.get_loader_table(),
                                            fields=self.get_loader_fields(), statement_string=True)


class PostgreSqlInsertStatementMixin(object):
    """Insert statement creator for PostgreSql"""

    @staticmethod
    def create_insert_statement(table, fields, statement_string=False):
        """returns insert statement"""

        insert_clause = SqlClause(header='INSERT INTO {} ('.format(table),
                                    footer=')',
                                    phrases=['{0}'.format(field) for field in fields])
        insert_values = 'VALUES (' + ', '.join(['%s']*len(fields)) + ')'

        if statement_string:
            return fields, SqlClause(phrases=[insert_clause, insert_values],
                                    phrase_indents=0,
                                    phrase_separator='\n').get_sql_clause()
        else:
            return fields, SqlClause(phrases=[insert_clause, insert_values],
                                    phrase_indents=0,
                                    phrase_separator='\n')

    def get_insert_statement(self):
        """returns statement to insert data"""

        return self.create_insert_statement(table=self.get_loader_table(),
                                            fields=self.get_loader_fields(), statement_string=True)
