"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint: disable=too-many-arguments

from etl_framework.utilities.SqlClause import SqlClause

class CreateTableStatementMixin(object):
    """requires SchemaMixin"""

    CONSTRAINTS_MAP = {
                      'primary': 'PRIMARY KEY',
                      'unique': 'UNIQUE KEY',
                      'index': 'INDEX'
                      }

    @classmethod
    def create_constraint_lines(cls, constraint_type, constraints):
        """constraints should be list of lists in format [[name, [fields]], ...]`"""

        if constraints is None:
            return []

        #im sorry
        return [
                cls.CONSTRAINTS_MAP[constraint_type] + ' `{0}` ({1})'.format(constraint_name,
                                        ', '.join(['`{}`'.format(field) for field in fields]))
                if constraint_name else
                cls.CONSTRAINTS_MAP[constraint_type] + ' ({0})'
                                        .format(', '.join(['`{}`'.format(field) for field in fields]))
                for constraint_name, fields in constraints
                ]

    @classmethod
    def create_create_table_statement(cls,
                                table,
                                fields,
                                primary_key=None,
                                unique_keys=None,
                                indexes=None,
                                statement_string=False,
                                if_not_exists=False):
        """returns create table statement"""

        schema_lines = ['`{0}` {1}'.format(field, datatype) for field, datatype in list(fields.items())]

        if primary_key:
            #primary key is list, but pass dictionary since create_constraints requires list of lists
            schema_lines.extend(cls.create_constraint_lines('primary', [['', primary_key]]))

        schema_lines.extend(cls.create_constraint_lines('unique', unique_keys))
        schema_lines.extend(cls.create_constraint_lines('index', indexes))

        if if_not_exists:
            create_table_header = 'CREATE TABLE IF NOT EXISTS `{}` ('
        else:
            create_table_header = 'CREATE TABLE `{}` ('

        if statement_string:
            return [], SqlClause(header=create_table_header.format(table),
                                phrases=schema_lines,
                                footer=') ENGINE=InnoDB DEFAULT CHARSET=utf8').get_sql_clause()

        else:
            return [], SqlClause(header=create_table_header.format(table),
                                phrases=schema_lines,
                                footer=') ENGINE=InnoDB DEFAULT CHARSET=utf8')

    def get_create_table_statement(self, if_not_exists=False):
        """returns statement to upsert data"""

        return  self.create_create_table_statement(table=self.get_table(),
                                            fields=self.get_fields(),
                                            primary_key=self.get_primary_key(),
                                            unique_keys=self.get_unique_keys(),
                                            indexes=self.get_indexes(),
                                            statement_string=True,
                                            if_not_exists=if_not_exists)
