"""parses configuration and can return derived table statements from configs"""
#pylint: disable=relative-import
#pylint: disable=too-many-arguments
#pylint: disable=too-many-function-args
#pylint: disable=too-many-locals

from SqlSchemaConfigParser import SqlSchemaConfigParser

class NoParentConfigException(Exception):
    """exception to raise if parent config attribute not set"""

    pass

class DerivedTableConfigParser(SqlSchemaConfigParser):
    """parses configuration files and creates statements to update derived tables"""


    PARENT_CONFIG_FILENAME_ATTR = 'config_filename'
    PARENT_CONFIG_DIR_ATTR = 'config_dir'
    PARENT_CONFIG_ATTR = 'parent_config'

    UNFORMATTED_DERIVED_TABLE_MYSQL_UPSERT_STATEMENT = '\n'.join([
                                    'INSERT INTO {target_table} (',
                                    '{target_columns_clause}',
                                    '\t)',
                                    'SELECT',
                                    '{source_columns_clause}',
                                    'FROM',
                                    '\t{source_table}',
                                    '{join_clause}',
                                    '{where_clause}',
                                    '{group_by_clause}',
                                    'ON DUPLICATE KEY UPDATE',
                                    '{update_clause}'
                                    ])

    UNFORMATTED_ALTER_TABLE_STATEMENT = '\n'.join([
                                    'ALTER TABLE {target_table}',
                                    '{alter_table_body}'
                                    ])

    UNFORMATTED_DELETE_STATEMENT = '\n'.join([
                                    'DELETE',
                                    '\t{target_table}.*',
                                    'FROM',
                                    '\t{target_table}',
                                    'INNER JOIN',
                                    '\t{source_table}',
                                    '{on_clause}',
                                    'WHERE',
                                    '\t{delete_field} = 1'
                                    ])

    @staticmethod
    def _create_add_columns_clause(columns):
        """creates add columns clause"""

        return SqlSchemaConfigParser._create_columns_clause(columns, prefix='ADD COLUMN ')

    @staticmethod
    def _create_add_unique_keys_clause(unique_keys):
        """creates add unique keys clause"""

        return SqlSchemaConfigParser._create_unique_keys_clause(unique_keys, prefix='ADD ')

    @staticmethod
    def _create_add_indexes_clause(indexes):
        """creates add indexes clause"""

        return SqlSchemaConfigParser._create_indexes_clause(indexes, prefix='ADD ')

    @staticmethod
    def _create_cutoff_phrase(source_sync_field, min_datetime=None, max_datetime=None):
        """returns source sync field datetime cutoff clause"""

        #if source_sync_field is empty, return empty string
        if not source_sync_field:
            return ''

        cutoff_phrase = ''

        if min_datetime:
            cutoff_phrase = DerivedTableConfigParser.join_phrases([cutoff_phrase,
                            '{source_sync_field} >= \'{min_datetime}\''])

        if max_datetime:
            cutoff_phrase = DerivedTableConfigParser.join_phrases([cutoff_phrase,
                                    '{source_sync_field} < \'{max_datetime}\''])

        cutoff_phrase = cutoff_phrase.format(
                            source_sync_field=source_sync_field,
                            min_datetime=min_datetime,
                            max_datetime=max_datetime)

        if cutoff_phrase:
            cutoff_phrase += '\t'

        return cutoff_phrase

    @staticmethod
    def _create_row_deleted_phrase(row_deleted_field):
        """returns phrase to filter out deleted rows from sql statement"""

        return '%s IS NULL'%(row_deleted_field, )

    @classmethod
    def _get_derived_table_upsert_statement(cls, target_table, target_columns, source_table, source_columns,
                                            join_clause, where_clause, group_by_clause, update_clause):
        """helper method to return derived_table_upsert_statement"""

        target_columns_clause = ',\n'.join(['\t' + column for column in target_columns])
        source_columns_clause = ',\n'.join(['\t' + column for column in source_columns])

        return cls.UNFORMATTED_DERIVED_TABLE_MYSQL_UPSERT_STATEMENT.format(
                                                                        target_table=target_table,
                                                                        target_columns_clause=target_columns_clause,
                                                                        source_table=source_table,
                                                                        source_columns_clause=source_columns_clause,
                                                                        join_clause=join_clause,
                                                                        where_clause=where_clause,
                                                                        group_by_clause=group_by_clause,
                                                                        update_clause=update_clause
                                                                            )

    def get_derived_table_remove_statement(self):
        """returns statement to delete removed rows"""

        delete_field = self.get_source_delete_field()
        target_table = self.get_target_table_name()
        source_table = self.get_source_table_name()
        field_mappings = self.get_field_mappings()
        source_match_fields = self.get_source_match_fields()
        left_on_entries, right_on_entries = zip(*[(field, field_mappings[field]) for field in source_match_fields])
        on_right_suffix = target_table + '.'
        on_clause = self.create_equality_clause_body(left_on_entries,
                                        right_on_entries,
                                        right_prefix=on_right_suffix,
                                        sep=' AND\n')
        on_clause = 'ON\n' + on_clause
        delete_statement = self.UNFORMATTED_DELETE_STATEMENT.format(
                                                        target_table=target_table,
                                                        delete_field=delete_field,
                                                        source_table=source_table,
                                                        on_clause=on_clause)

        return delete_statement

    def get_alter_table_statement(self):
        """returns alter table statement"""

        target_fields = self.get_target_fields()
        target_indexes = self.get_target_indexes()
        target_table = self.get_target_table_name()

        alter_table_body = ',\n'.join(
                            DerivedTableConfigParser._create_add_columns_clause(target_fields) +
                            DerivedTableConfigParser._create_add_indexes_clause(target_indexes)
                            )

        alter_table_statement = self.UNFORMATTED_ALTER_TABLE_STATEMENT.format(
                                                        target_table=target_table,
                                                        alter_table_body=alter_table_body
                                                        )

        return alter_table_statement

    def get_derived_table_upsert_statement(self, min_datetime=None, max_datetime=None,
                                            coalesce=True, db_type='mysql'):
        """returns derived table upsert statement"""

        if db_type == 'mysql':
            pass
        else:
            raise Exception('%s is not a supported db_type for upsert statement creation'%(db_type, ))

        field_mappings = self.get_field_mappings()

        source_table = self.get_source_table_name()
        target_table = self.get_target_table_name()

        try:
            source_sync_field = self.get_source_sync_field()
        except KeyError:
            source_sync_field = None

        try:
            row_deleted_field = self.get_source_delete_field()
        except KeyError:
            row_deleted_field = None

        #make sure target_columns are in same order as source_columns
        source_columns, target_columns = self.get_keys_and_values(field_mappings)

        #get where phrases
        try:
            where_phrases = self.get_source_where_phrases()
        except KeyError:
            where_phrases = []

        #get join clause
        joins_clause = self.get_source_joins_clause()

        #get groupby clause
        group_by_clause = self.get_source_group_by_clause()

        #get update clause
        update_columns = self.remove_target_unique_keys(target_columns)
        update_clause = self._create_update_clause(update_columns, coalesce=coalesce, target_table=target_table)

        #get datetime cutoff phrase
        if source_sync_field:
            cutoff_phrase = self._create_cutoff_phrase(source_sync_field, min_datetime, max_datetime)
        else:
            cutoff_phrase = ''

        #get row deleted cutoff phrase
        if row_deleted_field:
            row_deleted_phrase = self._create_row_deleted_phrase(row_deleted_field)
        else:
            row_deleted_phrase = ''

        where_phrases.append(cutoff_phrase)
        where_phrases.append(row_deleted_phrase)
        where_clause = DerivedTableConfigParser.join_phrases(where_phrases)

        if where_clause:
            where_clause = 'WHERE' + '\n\t' + where_clause

        return self._get_derived_table_upsert_statement(target_table, target_columns, source_table, source_columns,
                                                        joins_clause, where_clause, group_by_clause, update_clause)

    def get_parent_config(self):
        """gets parent config from self.config"""

        try:
            parent_config_info = self.config[self.PARENT_CONFIG_ATTR]
        except KeyError:
            raise NoParentConfigException

        config_dir = parent_config_info[self.PARENT_CONFIG_DIR_ATTR]
        config_filename = parent_config_info[self.PARENT_CONFIG_FILENAME_ATTR]

        return self.get_config(config_dir, config_filename)

    def _update_target_fields(self, new_target_fields):
        """updates target fields with new fields"""

        self.get_target_fields().update(new_target_fields)

    def _update_field_mappings(self, new_fields, source_table=None):
        """updates field mappings with new fields"""

        if source_table:
            prefix = source_table + '.'
        else:
            prefix = ''

        new_field_mappings = {prefix + field: field for field in new_fields}
        self.get_field_mappings().update(new_field_mappings)

    def set_config(self, *args, **kwargs):
        """sets the config"""

        super(DerivedTableConfigParser, self).set_config(*args, **kwargs)

        #check if parent config exists
        try:
            parent_config = self.get_parent_config()
        except NoParentConfigException:
            return

        parent_field_mapping_values = set(parent_config[self.COLUMN_MAPPINGS_ATTR].values())
        parent_target_fields = parent_config[self.TARGET_COLUMNS_ATTR]
        parent_target_table = parent_config[self.TARGET_TABLE_ATTR]

        #get target fields that are mapped from field mappings
        mapped_target_fields = {field: parent_target_fields[field] for field in parent_field_mapping_values}

        #update current config target fields and field mappings  with parent config's target fields
        self._update_target_fields(mapped_target_fields)
        self._update_field_mappings(mapped_target_fields.keys(), source_table=parent_target_table)

