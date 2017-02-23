"""parses configuration and returns useful things"""
#pylint cant tell AddFiltersMixin is new class
#pylint: disable=super-on-old-class

from etl_framework.config_mixins.AddFiltersMixin import AddFiltersMixin
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class BigqueryTablePartitionMixin(AddFiltersMixin):
    """parses configuration files"""

    BIGQUERY_TABLE_PARTITION_CHOOSER_ATTR = 'bigquery_table_partition_chooser'

    def add_filters_from_module(self, filter_functions):
        """override add_filters method of config object"""

        super(BigqueryTablePartitionMixin, self).add_filters_from_module(filter_functions)

        self.set_bigquery_tbigquery_table_partition_chooser(
            getattr(filter_functions,
                self.get_bigquery_table_partition_chooser()
            )
        )

    @check_config_attr_default_none
    def get_bigquery_table_partition_chooser(self):
        """stuff"""

        return self.config[self.BIGQUERY_TABLE_PARTITION_CHOOSER_ATTR]

    @check_config_attr_default_none
    def set_bigquery_tbigquery_table_partition_chooser(self, partition_chooser):
        """stuff"""

        self.config[self.BIGQUERY_TABLE_PARTITION_CHOOSER_ATTR] = partition_chooser

