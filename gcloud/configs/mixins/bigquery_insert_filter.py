"""parses configuration and returns useful things"""
#pylint cant tell AddFiltersMixin is new class
#pylint: disable=super-on-old-class

from etl_framework.config_mixins.AddFiltersMixin import AddFiltersMixin

class BigqueryInsertFilterMixin(AddFiltersMixin):
    """parses configuration files"""

    def add_filters_from_module(self, filter_functions):
        """override add_filters method of config object"""

        super(BigqueryInsertFilterMixin, self).add_filters_from_module(filter_functions)

        self.set_bigquery_insert_filter(
            getattr(filter_functions,
                self.get_bigquery_insert_filter()
            )
        )

    @property
    def bigquery_insert_filter(self):
        """stuff"""

        return self.config.get('bigquery_insert_filter')

    @bigquery_insert_filter.setter
    def bigquery_insert_filter(self, insert_filter):
        """stuff"""

        self.config['bigquery_insert_filter'] = insert_filter

