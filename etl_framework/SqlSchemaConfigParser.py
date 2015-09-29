"""parses configuration and can return sql schemas from configs"""
#pylint: disable=relative-import

from SourceConfigParser import SourceConfigParser
from method_wrappers.check_attr_set import _check_attr_set
from method_wrappers.add_grave_quotes import _add_grave_quotes

class SqlSchemaConfigParser(SourceConfigParser):
    """parses configuration files and creates sql schema query for target table"""

    AUTOSET_FIELDS = set(['id', 'record_updated'])

    TARGET_COLUMNS_ATTR = 'target_fields'
    TARGET_TABLE_ATTR = 'target_table'
    TARGET_INDEXES_ATTR = 'target_indexes'
    TARGET_MULTIPLE_TABLES_ATTR = 'target_multiple_tables'
    TARGET_PRIMARY_KEY_ATTR = 'target_primary_key'
    TARGET_UNIQUE_KEYS_ATTR = 'target_unique_keys'

    UNFORMATTED_DROP_TABLE_STATEMENT = 'DROP TABLE IF EXISTS `%s`;\n\n'
    UNFORMATTED_MYSQL_SCHEMA_STATEMENT = 'CREATE TABLE `%s` (\n' +\
                              '%s\n' +\
                              ') ENGINE=InnoDB DEFAULT CHARSET=utf8;'

    UNFORMATTED_MYSQL_UPSERT_STATEMENT = 'INSERT INTO {0} (\n{1}\n)\nVALUES ({2})\nON DUPLICATE KEY UPDATE\n{3};'

    @staticmethod
    def get_keys_and_values(dictionary):
        """returns a list of keys and a list of values, in order"""

        return zip(*dictionary.iteritems())

    @staticmethod
    def create_equality_clause_body(left_entries, right_entries, left_prefix='', right_prefix='', sep=',\n'):
        """returns clause with list of equalities"""

        clause_body = ['\t{0}{1} = {2}{3}'.format(left_prefix, left_entry, right_prefix, right_entry)
                for left_entry, right_entry in zip(left_entries, right_entries)]

        return sep.join(clause_body)

    @staticmethod
    def _create_schema_body(columns, unique_keys, indexes, primary_key):
        """returns string containing schema body"""

        schema_body = ',\n'.join(
                    SqlSchemaConfigParser._create_schema_columns(columns) +
                    SqlSchemaConfigParser._create_schema_unique_keys(unique_keys) +
                    SqlSchemaConfigParser._create_schema_indexes(indexes) +
                    SqlSchemaConfigParser._create_schema_primary_key(primary_key)
                    )

        return schema_body

    @staticmethod
    def _create_columns_clause(columns, prefix=None):
        """helper method to return columns clauses"""

        if prefix:
            pass
        else:
            prefix = ''

        return ['\t%s`%s` %s'%(prefix, column_name, datatype)
                for column_name, datatype in columns.iteritems()]

    @staticmethod
    def _create_unique_keys_clause(unique_keys, prefix=None):
        """helper method to return columns clauses"""

        if prefix:
            pass
        else:
            prefix = ''

        unique_keys = SqlSchemaConfigParser._stringify_keys(unique_keys)
        return ['\t%sUNIQUE KEY (%s)'%(prefix, unique_key, ) for unique_key in unique_keys]

    @staticmethod
    def _create_indexes_clause(indexes, prefix=None):
        """helper method to return columns clauses"""

        if prefix:
            pass
        else:
            prefix = ''

        indexes = SqlSchemaConfigParser._stringify_keys(indexes)
        return ['\tINDEX(%s)'%(index, ) for index in indexes]

    @staticmethod
    def _create_schema_columns(columns):
        """returns list of strings (1 for each column in sql query)"""

        return SqlSchemaConfigParser._create_columns_clause(columns)

    @staticmethod
    def _create_schema_unique_keys(unique_keys):
        """
        returns list of strings (1 for each index)
        unique_keys should be a list of sublists where
        each sublist contains all fields to include in the unique key
        """

        return SqlSchemaConfigParser._create_unique_keys_clause(unique_keys)

    @staticmethod
    def _create_schema_indexes(indexes):
        """
        returns list of strings (1 for each index)
        unique_keys should be a list of sublists where
        each sublist contains all fields to include in the unique key
        """

        return SqlSchemaConfigParser._create_indexes_clause(indexes)

    @staticmethod
    def _create_schema_primary_key(primary_key):
        """returns list with primary key"""

        return ['\tPRIMARY KEY (`%s`)'%(primary_key, )]

    @classmethod
    def _create_drop_table_statement(cls, table_name):
        """returns sql statement to drop a table if exists"""

        return cls.UNFORMATTED_DROP_TABLE_STATEMENT%(table_name, )

    @classmethod
    def _create_schema_statement(cls, table_name, schema_body, drop_existing=True, db_type='mysql'):
        """returns schema statement with given parameters"""

        if db_type == 'mysql':
            statement = cls.UNFORMATTED_MYSQL_SCHEMA_STATEMENT%(table_name, schema_body)
        else:
            raise Exception('%s is not a supported db_type for schema statement creation'%(db_type, ))

        if drop_existing:
            statement = cls._create_drop_table_statement(table_name) + statement

        return statement

    @staticmethod
    def _stringify_keys(keys):
        """helper method to turn sublists of keys into strings"""

        return [', '.join(['`' + field + '`' for field in key])for key in keys]

    @classmethod
    def _create_update_clause(cls, param_names, coalesce=False, target_table=None):
        """returns update clause of upsert statement"""

        left_update_entries = param_names

        #if target_table is specified, append as suffix to new update field value
        if target_table:
            target_table_str = target_table + '.'
        else:
            target_table_str = ''

        if coalesce:
            right_update_str = 'COALESCE(VALUES({0}), %s{0})'%(target_table_str, )
        else:
            right_update_str = 'VALUES({0})'

        right_update_entries = [right_update_str.format(column) for column in param_names]

        return cls.create_equality_clause_body(left_entries=left_update_entries,
                                        right_entries=right_update_entries)

    @classmethod
    @_add_grave_quotes(2, 'param_names')
    def _create_upsert_statement(cls, table_name, param_names, coalesce=False, db_type='mysql'):
        """returns upsert statement with given parameters"""

        if db_type == 'mysql':
            pass
        else:
            raise Exception('%s is not a supported db_type for upsert statement creation'%(db_type, ))

        columns_clause = ',\n'.join(['\t' + column for column in param_names])
        values_clause = ', '.join(['%s']*len(param_names))

        update_clause = cls._create_update_clause(param_names, coalesce=coalesce)

        return cls.UNFORMATTED_MYSQL_UPSERT_STATEMENT.format(table_name,
                                                            columns_clause,
                                                            values_clause,
                                                            update_clause)

    @classmethod
    def remove_autoset_fields(cls, fields):
        """removes autoset_fields from list"""

        autoset_fields = cls.get_autoset_fields()

        return list(set(fields) - autoset_fields)

    @classmethod
    def get_autoset_fields(cls):
        """returns sets of auto set fields"""

        return cls.AUTOSET_FIELDS

    @_check_attr_set('config')
    def get_target_table_name(self):
        """returns sql table name"""

        return self.config[self.TARGET_TABLE_ATTR]

    @_check_attr_set('config')
    def get_target_multiple_tables(self, include_autoset_fields=True):
        """gets target multiple tables for current configuration"""

        target_tables = self.config[self.TARGET_MULTIPLE_TABLES_ATTR]

        if include_autoset_fields:
            pass

        else:
            target_tables = {table: self.remove_autoset_fields(fields) for table, fields in target_tables.iteritems()}

        return target_tables

    @_check_attr_set('config')
    def get_stringified_target_unique_keys(self):
        """returns stringified target unique keys"""

        return self._stringify_keys(self.get_target_unique_keys())

    @_check_attr_set('config')
    def get_stringified_target_indexes(self):
        """returns stringified target indexes"""

        return self._stringify_keys(self.get_target_indexes())

    @_check_attr_set('config')
    def get_target_indexes(self):
        """returns target indexes"""

        return self.config[self.TARGET_INDEXES_ATTR]

    @_check_attr_set('config')
    def get_target_unique_keys(self):
        """returns target unique keys"""

        return self.config[self.TARGET_UNIQUE_KEYS_ATTR]

    def get_target_unique_key(self):
        """returns single target unique key"""

        unique_keys = self.get_target_unique_keys()

        if len(unique_keys) != 1:
            raise Exception('This method can only be called if there is exactly 1 unique key')
        else:
            return unique_keys[0]

    @_check_attr_set('config')
    def get_target_fields(self):
        """returns sql columns dict"""

        return self.config[self.TARGET_COLUMNS_ATTR]

    def remove_target_unique_keys(self, fields):
        """removes target unique keys from provided list and returns"""\

        unique_key = set(self.get_target_unique_key())

        return [field for field in fields if field not in unique_key]

    @_check_attr_set('config')
    def get_upsert_statement(self, param_names=None, coalesce=False, db_type='mysql'):
        """returns statement to upsert data"""

        if param_names is None:
            param_names = self.config[self.COLUMN_MAPPINGS_ATTR].values()

        table_name = self.get_target_table_name()

        statement = self._create_upsert_statement(table_name=table_name,
                                    param_names=param_names,
                                    coalesce=coalesce,
                                    db_type=db_type)

        return statement, param_names

    def get_drop_table_statement(self, table_name=None):
        """returns statement to drop table"""

        if table_name:
            pass
        else:
            table_name = self.get_target_table_name()

        return self._create_drop_table_statement(table_name)

    @_check_attr_set('config')
    def get_schema_statement(self, target_table=None, drop_existing=True, db_type='mysql'):
        """returns schema statement to rewrite table schema"""

        #get keys and indexes regardless of target_table
        target_unique_keys = self.get_target_unique_keys()
        target_indexes = self.get_target_indexes()

        #get all target fields
        target_fields = self.config[self.TARGET_COLUMNS_ATTR]

        #assume primary key is same for all target tables
        target_primary_key = self.config[self.TARGET_PRIMARY_KEY_ATTR]

        if target_table:
            multiple_target_tables = self.config[self.TARGET_MULTIPLE_TABLES_ATTR]
            try:
                specific_target_field_names = set(multiple_target_tables[target_table])
            except KeyError:
                raise Exception('%s not a valid target table'%(target_table, ))

            #filter out unneeded indexes, keys, and target_fields
            target_fields = {field_name: value for field_name, value in target_fields.iteritems()
                                if field_name in specific_target_field_names}
            #target_unique_keys should ALWAYS be in every target table
            target_indexes = [index for index in target_indexes if specific_target_field_names.issuperset(index)]

        else:
            target_table = self.config[self.TARGET_TABLE_ATTR]

        schema_body = self._create_schema_body(target_fields,
                                        target_unique_keys,
                                        target_indexes,
                                        target_primary_key)

        return self._create_schema_statement(table_name=target_table,
                                            schema_body=schema_body,
                                            drop_existing=drop_existing,
                                            db_type=db_type)
