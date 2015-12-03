"""Sql Clause creates Sql statements"""

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

        return [phrase for phrase in phrases if phrase]

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
        self.child_clauses = [phrase for phrase in self.phrases if isinstance(phrase, SqlClause)]

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

        if isinstance(phrase, SqlClause):
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

