"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.SqlClause import SqlClause

class SchemaStatementMixin(object):
    """requires TargetMixin and FieldsMixin"""

    COALESCE_MAP = {
                    None: 'VALUES(`{0}`)',
                    'new_values': 'COALESE(VALUES(`{0}`), `{0}`)',
                    'old_values': 'COALESCE(`{0}`, VALUES(`{0}`))'
                    }

    def create_on_duplicate_key_update_statement(self, table, fields, coalesce=None, statement_string=False):
        """returns update statement for upsert"""

        coalesce_string = self.COALESCE_MAP[coalesce]

        if statement_string:
            return [], SqlClause(sql_keyword='ON DUPLICATE KEY UPDATE',
                phrases=['`{}` = '.format(field) + coalesce_string.format(field) for field in fields]).get_sql_clause()
        else:
            return [], SqlClause(sql_keyword='ON DUPLICATE KEY UPDATE',
                phrases=['`{}` = '.format(field) + coalesce_string.format(field) for field in fields])

    def create_schema_statement(self,
                                table,
                                fields,
                                primary_key=None,
                                unique_keys=None,
                                indexes=None,
                                statement_string=False):
        """returns schema statement"""

        insert_fields, insert_statement = self.create_insert_statement(table, fields)
        update_fields, update_statement = self.create_on_duplicate_key_update_statement(table, fields)

        if statement_string:
            return insert_fields + update_fields, SqlClause(phrases=[insert_statement, update_statement],
                                                            phrase_indents=0,
                                                            phrase_separator='\n').get_sql_clause()
        else:
            return insert_fields + update_fields, SqlClause(phrases=[insert_statement, update_statement],
                                                            phrase_indents=0,
                                                            phrase_separator='\n')

    def get_schema_statement(self):
        """returns statement to upsert data"""

        return  self.create_schema_statement(table=self.get_target_table(),
                                            fields=self.get_fields(),
                                            primary_key=self.get_primary_key(),
                                            unique_keys=self.get_unqiue_keys(),
                                            indexes=self.get_indexes(),
                                            statement_string=True)
