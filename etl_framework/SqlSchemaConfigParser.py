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
    def create_equality_phrase(left_entries, right_entries, left_suffix='', right_suffix='', sep=',\n'):
        """returns phrase with list of equalities"""

        phrase = ['\t{0}{1} = {2}{3}'.format(left_suffix, left_entry, right_suffix, right_entry)
                for left_entry, right_entry in zip(left_entries, right_entries)]

        return sep.join(phrase)

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
    def _create_schema_columns(columns):
        """returns list of strings (1 for each column in sql query)"""

        return ['\t`%s` %s'%(column_name, datatype)
                for column_name, datatype in columns.iteritems()]

    @staticmethod
    def _create_schema_unique_keys(unique_keys):
        """returns list of strings (1 for each index)"""

        return ['\tUNIQUE KEY (`%s`)'%(unique_key, ) for unique_key in unique_keys]

    @staticmethod
    def _create_schema_indexes(indexes):
        """returns list of strings (1 for each index)"""

        return ['\tINDEX(`%s`)'%(index, ) for index in indexes]

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

    @classmethod
    @_add_grave_quotes(2, 'param_names')
    def _create_upsert_statement(cls, table_name, param_names, coalesce=False, db_type='mysql'):
        """returns upsert statement with given parameters"""

        if db_type == 'mysql':
            pass
        else:
            raise Exception('%s is not a supported db_type for upsert statement creation'%(db_type, ))

        columns_phrase = ',\n'.join(['\t' + column for column in param_names])
        values_phrase = ', '.join(['%s']*len(param_names))

        left_update_entries = param_names

        if coalesce:
            right_update_str = 'COALESCE(VALUES({0}), {0})'
        else:
            right_update_str = 'VALUES({0})'

        right_update_entries = [right_update_str.format(column) for column in param_names]

        update_phrase = cls.create_equality_phrase(left_entries=left_update_entries,
                                                right_entries=right_update_entries)

        return cls.UNFORMATTED_MYSQL_UPSERT_STATEMENT.format(table_name,
                                                            columns_phrase,
                                                            values_phrase,
                                                            update_phrase)

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
    def get_target_unique_keys(self):
        """returns target unique keys"""

        return self.config[self.TARGET_UNIQUE_KEYS_ATTR]

    @_check_attr_set('config')
    def get_target_fields(self):
        """returns sql columns dict"""

        return self.config[self.TARGET_COLUMNS_ATTR]

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

    @_check_attr_set('config')
    def get_schema_statement(self, target_table=None, drop_existing=True, db_type='mysql'):
        """returns schema statement to rewrite table schema"""

        #get keys and indexes regardless of target_table
        target_unique_keys = self.config[self.TARGET_UNIQUE_KEYS_ATTR]
        target_indexes = self.config[self.TARGET_INDEXES_ATTR]

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
            target_unique_keys = list(specific_target_field_names.intersection(target_unique_keys))
            target_indexes = list(specific_target_field_names.intersection(target_indexes))

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
