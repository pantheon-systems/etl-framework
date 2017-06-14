#pylint: disable=too-many-locals
#pylint: disable=used-before-assignment

from etl_framework.utilities.SqlClause import SqlClause

class CompositeMySqlStatementsConfigMixin(object):
    """This Mixin assumes class has a component_schemas and table attributes"""


    @staticmethod
    def _create_where_clauses(
        all_tables,
        time_cutoff_field,
        where_phrases
    ):

        where_clauses = list()
        output_fields = list()

        if time_cutoff_field:
            greatest_phrase = ", ".join(table + "."+ time_cutoff_field for table in all_tables)
            where_phrases.append("GREATEST({greatest_phrase}) >= (%s)".format(greatest_phrase=greatest_phrase))
            output_fields.append(time_cutoff_field)

        if where_phrases:
            where_clause = SqlClause(
                header="WHERE",
                phrases=["({})".format(phrase) for phrase in where_phrases],
                phrase_separator=' AND\n'
            )
            where_clauses.append(where_clause)

        return output_fields, where_clauses

    @staticmethod
    def _create_join_clauses(
        all_tables,
        all_join_keys
    ):

        first = True
        join_clauses = list()
        for table, schema_join_key in zip(all_tables, all_join_keys):

            if not first:
                next_clause = SqlClause(
                    header="INNER JOIN",
                    phrases=["{}".format(
                        table
                    )]
                )

                join_clauses.append(next_clause)

                next_clause = SqlClause(
                    header="ON",
                    phrases=["{}.{} = {}.{}".format(
                        previous_table,
                        previous_schema_join_key,
                        table,
                        schema_join_key)
                    ]
                )

                join_clauses.append(next_clause)

            else:
                next_clause = SqlClause(
                    header="FROM",
                    phrases=["{}".format(table)]
                )
                first = False
                previous_table = table
                previous_schema_join_key = schema_join_key
                join_clauses.append(next_clause)

        return join_clauses

    def create_composite_sql_table_delete_statement(
        self,
        component_schema_ids,
        join_key,
        row_deleted_field,
        time_cutoff_field=None,
        where_phrases=None
    ):

        all_tables = list()
        all_join_keys = list()
        all_clauses = list()
        output_fields = list()
        if where_phrases is None:
            where_phrases = list()

        all_join_keys.append(join_key)

        for schema_id in component_schema_ids:
            component = self.component_schemas[schema_id]
            schema = component["config"]
            field_renames = component["field_renames"]
            reverse_field_renames = {value: key for key, value in field_renames.iteritems()}
            schema_join_key = reverse_field_renames.get(join_key, join_key)

            table = schema.table

            all_tables.append(table)
            all_join_keys.append(schema_join_key)

        delete_clause = SqlClause(
            header="DELETE",
            phrases=["{table}.*".format(table=self.table)]
        )

        all_clauses.append(delete_clause)
        all_clauses.extend(self._create_join_clauses([self.table] + all_tables, all_join_keys))

        if row_deleted_field:
            where_phrases.extend(["{}.{} = 1".format(table, row_deleted_field) for table in all_tables])

        where_fields, where_clauses = self._create_where_clauses(all_tables, time_cutoff_field, where_phrases)
        output_fields.extend(where_fields)
        all_clauses.extend(where_clauses)

        return output_fields, SqlClause(phrases=all_clauses, phrase_indents=0, phrase_separator="\n")

    def create_composite_sql_table_upsert_statement(
        self,
        component_schema_ids,
        join_key,
        time_cutoff_field=None,
        where_phrases=None
    ):

        all_fields = list()
        all_mapped_fields = list()
        all_tables = list()
        all_join_keys = list()
        all_clauses = list()
        output_fields = list()
        if where_phrases is None:
            where_phrases = list()

        for schema_id in component_schema_ids:
            component = self.component_schemas[schema_id]
            schema = component["config"]
            ignored_fields = set(component["ignored_fields"])
            field_renames = component["field_renames"]
            reverse_field_renames = {value: key for key, value in field_renames.iteritems()}
            schema_join_key = reverse_field_renames.get(join_key, join_key)

            table = schema.table

            # NOTE we ignore the join key in the fields
            fields, mapped_fields = zip(*[
                (
                    "{}.{}".format(table, field),
                    field_renames.get(field, field)
                )
                for field in schema.fields if field not in ignored_fields
                and field != schema_join_key
            ])

            all_fields.extend(fields)
            all_mapped_fields.extend(mapped_fields)
            all_tables.append(table)
            all_join_keys.append(schema_join_key)

        # Add the join key to fields ONCE
        all_fields.append("{}.{}".format(table, schema_join_key))
        all_mapped_fields.append(join_key)

        insert_clause = SqlClause(
            header="INSERT INTO {table} (".format(table=self.table),
            footer=')',
            phrases=all_mapped_fields
        )

        all_clauses.append(insert_clause)

        select_clause = SqlClause(
            header="SELECT",
            phrases=all_fields
        )

        all_clauses.append(select_clause)
        all_clauses.extend(self._create_join_clauses(all_tables, all_join_keys))

        where_fields, where_clauses = self._create_where_clauses(all_tables, time_cutoff_field, where_phrases)
        output_fields.extend(where_fields)
        all_clauses.extend(where_clauses)

        duplicate_update_clause = SqlClause(
            header="ON DUPLICATE KEY UPDATE",
            phrases=["{} = VALUES({})".format(mapped_field, field)
                for field, mapped_field in zip(all_fields, all_mapped_fields)
            ]
        )

        all_clauses.append(duplicate_update_clause)

        return output_fields, SqlClause(phrases=all_clauses, phrase_indents=0, phrase_separator="\n")

    def create_composite_sql_table_update_statement(
        self,
        component_schema_ids,
        join_key,
        time_cutoff_field=None,
        where_phrases=None
    ):

        all_fields = list()
        all_mapped_fields = list()
        all_tables = list()
        all_join_keys = list()
        all_clauses = list()
        output_fields = list()
        if where_phrases is None:
            where_phrases = list()

        for schema_id in component_schema_ids:
            component = self.component_schemas[schema_id]
            schema = component["config"]
            ignored_fields = set(component["ignored_fields"])
            field_renames = component["field_renames"]
            table = schema.table

            # NOTE we dont update the schema_join_key
            reverse_field_renames = {value: key for key, value in field_renames.iteritems()}
            schema_join_key = reverse_field_renames.get(join_key, join_key)

            fields, mapped_fields = zip(*[
                (
                    "{}.{}".format(table, field),
                    field_renames.get(field, field)
                )
                for field in schema.fields if field not in ignored_fields
                and field != schema_join_key
            ])

            all_fields.extend(fields)
            all_mapped_fields.extend(mapped_fields)
            all_tables.append(table)
            all_join_keys.append(schema_join_key)

        update_clause = SqlClause(
            header="UPDATE {table}".format(table=self.table),
            phrases=all_mapped_fields
        )

        all_clauses.append(update_clause)

        for table, schema_join_key in zip(all_tables, all_join_keys):

            next_clause = SqlClause(
                header="INNER JOIN",
                phrases=["{}".format(
                    table
                )]
            )

            all_clauses.append(next_clause)

            next_clause = SqlClause(
                header="ON",
                phrases=["{}.{} = {}.{}".format(
                    self.table,
                    join_key,
                    table,
                    schema_join_key)
                ]
            )

            all_clauses.append(next_clause)

        set_clause = SqlClause(
            header="SET",
            phrases=["{}.{} = VALUES({})".format(self.table, mapped_field, field)
                for field, mapped_field in zip(all_fields, all_mapped_fields)
            ]
        )

        all_clauses.append(set_clause)
        where_fields, where_clauses = self._create_where_clauses(all_tables, time_cutoff_field, where_phrases)
        output_fields.extend(where_fields)
        all_clauses.extend(where_clauses)

        return output_fields, SqlClause(phrases=all_clauses, phrase_indents=0, phrase_separator="\n")
