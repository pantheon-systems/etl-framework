"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.SqlClause import SqlClause
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none
from InsertStatementMixin import MySqlInsertStatementMixin, PostgreSqlInsertStatementMixin


class MySqlUpsertStatementMixin(MySqlInsertStatementMixin):
    """requires LoaderMixin"""

    COALESCE_MAP = {
        None: 'VALUES(`{0}`)',
        'new_values': 'COALESE(VALUES(`{0}`), `{0}`)',
        'old_values': 'COALESCE(`{0}`, VALUES(`{0}`))'
    }

    @classmethod
    def _create_on_duplicate_key_update_statement(cls, fields, coalesce=None):
        """returns update statement for upsert"""

        coalesce_string = cls.COALESCE_MAP[coalesce]
        sql_obj = SqlClause(header='ON DUPLICATE KEY UPDATE',
            phrases=['{} = '.format(field) + coalesce_string.format(field) for field in fields])

        return sql_obj

    def create_upsert_statement(self, table, fields, statement_string=False):
        """returns update statement"""

        insert_fields, insert_statement = self.create_insert_statement(table, fields)
        update_statement = self._create_on_duplicate_key_update_statement(fields)

        sql = SqlClause(phrases=[insert_statement, update_statement],
                        phrase_indents=0,
                        phrase_separator='\n')

        if statement_string:
            sql = sql.get_sql_clause()

        return insert_fields, sql

    def get_upsert_statement(self):
        """returns statement to upsert data"""

        return self.create_upsert_statement(table=self.get_loader_table(),
                                            fields=self.get_loader_fields(),
                                            statement_string=True)


class PostgreSqlUpsertStatementMixin(PostgreSqlInsertStatementMixin):
    """requires LoaderMixin, BaseConfig"""

    COALESCE_MAP = {
        None: 'EXCLUDED.{0}',
        'new_values': 'COALESE(EXCLUDED.{0}, {table}.{0})',
        'old_values': 'COALESCE({0}, EXCLUDED.{0})'
    }

    UPSERT_CONSTRAINT_FIELDS = 'upsert_constraint_fields'


    @classmethod
    def _create_on_duplicate_key_update_statement(cls, fields, constraint_fields,
                                                  coalesce=None, table=None):
        """returns update statement for upsert"""

        coalesce_string = cls.COALESCE_MAP[coalesce]
        header = ','.join(constraint_fields)
        header = 'ON CONFLICT (' + header + ') DO UPDATE SET '
        sql_obj = SqlClause(header=header,
            phrases=['{} = '.format(field) + coalesce_string.format(field, table=table) for field in fields])

        return sql_obj

    def create_upsert_statement(self, table, fields, constraint_fields,
                                statement_string=False,
                                coalesce=None):
        """returns update statement"""

        insert_fields, insert_statement = self.create_insert_statement(table, fields)
        update_statement = self._create_on_duplicate_key_update_statement(
            fields,
            constraint_fields,
            table=table,
            coalesce=coalesce,
        )

        sql = SqlClause(phrases=[insert_statement, update_statement],
                        phrase_indents=0,
                        phrase_separator='\n')

        if statement_string:
            sql = sql.get_sql_clause()

        return insert_fields, sql

    def get_upsert_statement(self, coalesce=None):
        """returns statement to upsert data"""

        return self.create_upsert_statement(self.get_loader_table(),
                                            self.get_loader_fields(),
                                            self.get_upsert_constraint_fields(),
                                            statement_string=True,
                                            coalesce=coalesce)

    @check_config_attr_default_none
    def get_upsert_constraint_fields(self):
        """
        :summary: getter for upsert_constraint_fields
        :returns: upsert constraint fields
        """

        return self.config[self.UPSERT_CONSTRAINT_FIELDS]
