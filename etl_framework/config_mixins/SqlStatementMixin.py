"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class SqlStatementMixin(object):
    """parses configuration files"""

    SQL_FIELDS_AND_STATEMENT_LIST_ATTR = 'sql_fields_and_statement_list'
    SQL_STATEMENT_ATTR = 'sql_statement'
    SQL_FIELDS_ATTR = 'sql_fields'
    SQL_WHERE_FIELDS_ATTR = 'sql_where_fields'

    @check_config_attr_default_none
    def get_sql_statement(self):
        """stuff"""

        return self.config[self.SQL_STATEMENT_ATTR]

    @check_config_attr_default_none
    def get_sql_fields(self):
        """stuff"""

        return self.config[self.SQL_FIELDS_ATTR]

    def get_sql_fields_and_statement(self):
        """stuff"""

        return self.get_sql_fields(), self.get_sql_statement()

    @check_config_attr_default_none
    def set_sql_statement(self, sql_statement):
        """stuff"""

        self.config[self.SQL_STATEMENT_ATTR] = sql_statement

    @check_config_attr_default_none
    def set_sql_fields(self, sql_fields):
        """stuff"""

        self.config[self.SQL_FIELDS_ATTR] = sql_fields

    def set_sql_fields_and_statement(self, sql_fields, sql_statement):
        """stuff"""

        self.set_sql_fields(sql_fields)
        self.set_sql_statement(sql_statement)

    def set_loader_fields_and_statement(self):
        """stuff"""

        raise NotImplementedError

    @check_config_attr_default_none
    def set_sql_fields_and_statement_list(self,
        sql_fields_and_statement_list):
        """Sets a list of (fields, statement) tuples"""

        self.config[self.SQL_FIELDS_AND_STATEMENT_LIST_ATTR] =\
            sql_fields_and_statement_list

    @check_config_attr_default_none
    def get_sql_where_fields(self):
        """
        Returns where_fields attribute, which is in format:
            [
                {
                    "field_name": "field1",
                    "operator": "<"
                },
                ...
            ]
        """

        return self.config[self.SQL_WHERE_FIELDS_ATTR]

    @check_config_attr_default_none
    def get_sql_fields_and_statement_list(self):
        """Returns a list of (fields, statement) tuples"""

        return self.config[self.SQL_FIELDS_AND_STATEMENT_LIST_ATTR]
