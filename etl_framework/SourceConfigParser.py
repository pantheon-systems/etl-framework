"""
parses configuration and returns useful things
"""

# NOTE: this is deprecated. Dont use.

#pylint: disable=relative-import
#pylint: disable=too-many-arguments
#pylint: disable=too-many-locals

from BaseConfigParser import BaseConfigParser
from method_wrappers.check_attr_set import _check_attr_set

class SourceConfigParser(BaseConfigParser):
    """parses configuration files"""

    SOURCE_SYNC_FIELD_ATTR = 'source_sync_field'
    SOURCE_TABLE_ATTR = 'source_table'
    SOURCE_MATCH_FIELDS_ATTR = 'source_match_fields'
    SOURCE_MULTIPLE_TABLES_ATTR = 'source_multiple_tables'
    SOURCE_DELETE_ATTR = 'source_delete_field'
    SOURCE_JOINS_ATTRIBUTE = 'source_joins'
    JOIN_TABLE_ATTRIBUTE = 'table'
    JOIN_TYPE_ATTRIBUTE = 'type'
    JOIN_ON_ATTRIBUTE = 'on'
    SOURCE_GROUP_BY_ATTRIBUTE = 'source_groups'
    SOURCE_WHERE_CLAUSES_ATTRIBUTE = 'source_where_phrases'

    SUBSENTENCE_FIELDS_ATTRIBUTE = 'fields'
    SUBSENTENCE_GROUPS_ATTRIBUTE = 'groups'
    SUBSENTENCE_JOINS_ATTRIBUTE = 'joins'
    SUBSENTENCE_TABLE_ATTRIBUTE = 'table'
    SUBSENTENCE_TABLE_ALIAS_ATTRIBUTE = 'table_alias'
    SUBSENTENCE_WHERE_CLAUSES_ATTRIBUTE = 'where_phrases'

    class SqlClause(object):
        """class for holding sql_phrase info"""

        INDENT_CHAR = '\t'

        def __init__(self, phrases, sql_keyword='', phrase_prefix='',
                    phrase_separator=',\n', phrase_suffix='', phrase_indents=1,
                    indents=0, header='', footer='', fixify_clauses=False):
            """initializes object"""

            self.sql_keyword = sql_keyword
            self.phrase_prefix = phrase_prefix
            self.phrase_separator = phrase_separator
            self.phrase_suffix = phrase_suffix
            self.header = header
            self.footer = footer
            self.phrase_indents = phrase_indents
            self.indents = indents
            self.phrases = []
            self.child_clauses = []
            self.fixify_clauses = fixify_clauses

            #this sets phrases and child_clauses
            self.set_phrases(phrases)

        def _clean_phrases(self, phrases):
            """returns only non-trivial phrases"""

            return [self.create_subsentence(phrase)for phrase in phrases if phrase]

        def create_subsentence(self, subsentence):
            """returns phrase or SqlClause list for subsentences"""

            if isinstance(subsentence, dict):

                cleaned_fields = self._clean_phrases(
                                        subsentence[SourceConfigParser.SUBSENTENCE_FIELDS_ATTRIBUTE])
                cleaned_groups = self._clean_phrases(
                                        subsentence[SourceConfigParser.SUBSENTENCE_GROUPS_ATTRIBUTE])
                joins = subsentence[SourceConfigParser.SUBSENTENCE_JOINS_ATTRIBUTE]
                cleaned_wheres = self._clean_phrases(
                                        subsentence[SourceConfigParser.SUBSENTENCE_WHERE_CLAUSES_ATTRIBUTE])

                table_alias = subsentence[SourceConfigParser.SUBSENTENCE_TABLE_ALIAS_ATTRIBUTE]
                table = subsentence[SourceConfigParser.SUBSENTENCE_TABLE_ATTRIBUTE]

                join_clauses = list()
                for join in joins:
                    join_type = join[SourceConfigParser.JOIN_TYPE_ATTRIBUTE]
                    join_table = join[SourceConfigParser.JOIN_TABLE_ATTRIBUTE]
                    join_on = join[SourceConfigParser.JOIN_ON_ATTRIBUTE]

                    cleaned_join = self._clean_phrases([join_table])
                    cleaned_on = self._clean_phrases(join_on)

                    join_clause = SourceConfigParser.SqlClause(
                                            sql_keyword=join_type + ' ' + 'JOIN', phrases=[join_table])
                    on_clause = SourceConfigParser.SqlClause(
                                            sql_keyword='ON', phrases=cleaned_on, phrase_separator=' AND\n')
                    join_clauses.append(join_clause)
                    join_clauses.append(on_clause)

                select_clauses = SourceConfigParser.SqlClause(sql_keyword='SELECT', phrases=cleaned_fields)
                from_clause = SourceConfigParser.SqlClause(sql_keyword='FROM', phrases=[table])
                where_clauses = SourceConfigParser.SqlClause(
                                            sql_keyword='WHERE', phrases=cleaned_wheres, phrase_separator=' AND\n')
                group_clauses = SourceConfigParser.SqlClause(sql_keyword='GROUP BY', phrases=cleaned_groups)

                all_clauses = [select_clauses, from_clause] +  join_clauses + [where_clauses, group_clauses]

                return SourceConfigParser.SqlClause(phrases=all_clauses,
                                                    header='(', footer=') AS ' + table_alias,
                                                    phrase_separator='\n',
                                                    phrase_indents=0)

            else:
                return subsentence

        def set_phrase_separator(self, phrase_separator):
            """sets phrase_separator"""

            self.phrase_separator = phrase_separator

        def set_indents(self, indents):
            """sets indents"""

            self.indents = indents

        def set_phrase_prefix(self, phrase_prefix):
            """sets phrases"""

            self.phrase_prefix = phrase_prefix

        def set_phrase_suffix(self, phrase_suffix):
            """sets phrases"""

            self.phrase_suffix = phrase_suffix

        def set_phrases(self, phrases):
            """sets phrases"""

            self.phrases = self._clean_phrases(phrases)
            self.child_clauses = [phrase for phrase in self.phrases if isinstance(phrase, SourceConfigParser.SqlClause)]

        def remove_phrases(self):
            """removes phrases"""

            removed_phrases = self.phrases
            self.phrases = []
            self.child_clauses = []

            return removed_phrases

        def set_sql_keyword(self, sql_keyword):
            """sets sql_keyword"""

            self.sql_keyword = sql_keyword

        def modify_indents(self, indents):
            """modifies indents"""

            self.indents += indents

        def modify_phrase_indents(self, indents):
            """modifies indents"""

            self.phrase_indents += indents

        def append_to_phrase_prefix(self, phrase_prefix):
            """adds to indents"""

            self.phrase_prefix += phrase_prefix

        def append_to_phrase_suffix(self, phrase_suffix):
            """adds to indents"""

            self.phrase_suffix += phrase_suffix

        def get_stringified_phrases(self, outer_indent_string):
            """returns phrases with suffixes and prefixes"""

            return [self.stringify_phrase(phrase, outer_indent_string) for phrase in self.phrases]

        def get_sql_clause_elements(self):
            """returns sql clause as list object"""
            raise NotImplementedError
            #return [self.sql_keyword] + self.get_stringified_phrases()

        def get_indent_string(self):
            """returns indent string"""

            return self.INDENT_CHAR*self.indents

        def get_phrase_indent_string(self):
            """returns phrase_indent string"""

            return self.INDENT_CHAR*self.phrase_indents

        def get_sql_clause(self, outer_indent_string=''):
            """returns sql clause as a string"""

            #if no phrases, return empty string
            if not self.phrases:
                return ''

            indent_string = outer_indent_string +  self.get_indent_string()

            #get phrases string
            phrases_string = [indent_string + phrase for phrase in self.get_stringified_phrases(outer_indent_string)]
            phrases_string = self.phrase_separator.join(phrases_string)

            #create clause elements with inden string added
            clause_elements = list()

            if self.header:
                clause_elements.append(indent_string + self.header)
            if self.sql_keyword:
                clause_elements.append(indent_string + self.sql_keyword)

            clause_elements.append(phrases_string)

            if self.footer:
                clause_elements.append(indent_string + self.footer)

            return '\n'.join(clause_elements)

        def stringify_phrase(self, phrase, outer_indent_string=''):
            """returns string version of phrase (which can be a str object of SqlClause object)"""

            if isinstance(phrase, SourceConfigParser.SqlClause):
                outer_indent_string += self.get_indent_string() + self.get_phrase_indent_string()

                #get sql_phrase and strip off leading outer_indent_string
                sql_phrase = phrase.get_sql_clause(outer_indent_string=outer_indent_string)[len(outer_indent_string):]

                if self.fixify_clauses:
                    sql_phrase = self._add_phrase_fixes(sql_phrase)

            else:
                sql_phrase = self._add_phrase_fixes(phrase)

            return self.get_phrase_indent_string() + sql_phrase

        def _add_phrase_fixes(self, phrase):
            """helper function to add phrase suffix and prefix"""

            return self.phrase_prefix + phrase + self.phrase_suffix

    @staticmethod
    def join_phrases(phrases, joiner=None, prefix=None):
        """returns clauses joined with AND"""

        if joiner:
            pass
        else:
            joiner = ' AND '

        if prefix:
            pass
        else:
            prefix = ''

        return joiner.join([prefix + phrase for phrase in phrases if phrase])

    @classmethod
    def _create_joins_array(cls, joins):
        """
        returns joins segments as elements in list
        input: list of dictionaries
        """

        joins_array = list()
        for join in joins:

            join_type = join[cls.JOIN_TYPE_ATTRIBUTE]
            join_table = join[cls.JOIN_TABLE_ATTRIBUTE]
            join_on = join[cls.JOIN_ON_ATTRIBUTE]

            join_clause = cls.SqlClause(sql_keyword=join_type + ' ' + 'JOIN', phrases=[join_table])
            on_clause = cls.SqlClause(sql_keyword='ON', phrases=join_on, phrase_separator=' AND\n')

            joins_array.append(join_clause)
            joins_array.append(on_clause)

        return joins_array

    @classmethod
    def _create_joins_clause(cls, joins):
        """
        creates the joins segment of SQL statement.
        Input: list of dictionaries
        """

        joins_array = cls._create_joins_array(joins)

        return '\n'.join(join.get_sql_clause() for join in joins_array)

    @classmethod
    def _create_group_by_clause(cls, groups):
        """
        creates the groupy by clause of SQL statement
        groups is an iterable (list)
        """

        group_by_clause = 'GROUP BY' + ' ' + ', '.join(groups)

        return group_by_clause

    def get_source_joins_clause(self):
        """returns source joins clause"""

        try:
            joins = self.get_source_joins()
        except KeyError:
            joins_clause = ''
        else:
            joins_clause = self._create_joins_clause(joins)

        return joins_clause

    def get_source_group_by_clause(self):
        """returns source group by clause"""

        try:
            groups = self.get_source_groups()
        except KeyError:
            group_by_clause = ''
        else:
            group_by_clause = self._create_group_by_clause(groups)

        return group_by_clause

    @_check_attr_set('config')
    def get_source_where_phrases(self):
        """gets source group by's for current configuration"""

        return self.config[self.SOURCE_WHERE_CLAUSES_ATTRIBUTE]

    @_check_attr_set('config')
    def get_source_groups(self):
        """gets source group by's for current configuration"""

        return self.config[self.SOURCE_GROUP_BY_ATTRIBUTE]

    @_check_attr_set('config')
    def get_source_joins(self):
        """gets source sync field for current configuration"""

        return self.config[self.SOURCE_JOINS_ATTRIBUTE]

    @_check_attr_set('config')
    def get_source_sync_field(self):
        """gets source sync field for current configuration"""

        return self.config[self.SOURCE_SYNC_FIELD_ATTR]

    @_check_attr_set('config')
    def get_source_table_name(self):
        """gets source table for current configuration"""

        return self.config[self.SOURCE_TABLE_ATTR]

    @_check_attr_set('config')
    def get_source_match_fields(self):
        """gets source match fields for current configuration"""

        return self.config[self.SOURCE_MATCH_FIELDS_ATTR]

    @_check_attr_set('config')
    def get_source_multiple_tables(self):
        """gets source multiple tables for current configuration"""

        return self.config[self.SOURCE_MULTIPLE_TABLES_ATTR]

    @_check_attr_set('config')
    def get_source_delete_field(self):
        """gets source delete fields for current configuration"""

        return self.config[self.SOURCE_DELETE_ATTR]

    @_check_attr_set('config')
    def get_target_delete_field(self):
        """gets target delete field for current configuration"""

        field_mappings = self.get_field_mappings()
        return field_mappings[self.get_source_delete_field()][1]
