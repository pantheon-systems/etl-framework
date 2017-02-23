"""parses configuration and returns useful things"""
#pylint cant tell AddFiltersMixin is new class
#pylint: disable=super-on-old-class

from etl_framework.config_mixins.AddFiltersMixin import AddFiltersMixin
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class BigqueryInsertFilterMixin(AddFiltersMixin):
    """parses configuration files"""

    BIGQUERY_INSERT_FILTER_ATTR = 'bigquery_insert_filter'

    def add_filters_from_module(self, filter_functions):
        """override add_filters method of config object"""

        super(BigqueryInsertFilterMixin, self).add_filters_from_module(filter_functions)

        self.set_bigquery_insert_filter(
            getattr(filter_functions,
                self.get_bigquery_insert_filter()
            )
        )

    @check_config_attr_default_none
    def get_bigquery_insert_filter(self):
        """stuff"""

        return self.config[self.BIGQUERY_INSERT_FILTER_ATTR]

    @check_config_attr_default_none
    def set_bigquery_insert_filter(self, insert_filter):
        """stuff"""

        self.config[self.BIGQUERY_INSERT_FILTER_ATTR] = insert_filter

