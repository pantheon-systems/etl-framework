"""parses configuration and returns useful things"""
#pylint cant tell AddFiltersMixin is new class
#pylint: disable=super-on-old-class

from etl_framework.config_mixins.AddFiltersMixin import AddFiltersMixin

class BigqueryTablePartitionMixin(AddFiltersMixin):
    """parses configuration files"""

    def add_filters_from_module(self, filter_functions):
        """override add_filters method of config object"""

        super(BigqueryTablePartitionMixin, self).add_filters_from_module(filter_functions)

        self.set_bigquery_tbigquery_table_partition_chooser(
            getattr(filter_functions,
                self.get_bigquery_table_partition_chooser()
            )
        )

    @property
    def bigquery_table_partition_chooser(self):
        """stuff"""

        return self.config.get('bigquery_table_partition_chooser')

    @bigquery_table_partition_chooser.setter
    def bigquery_tbigquery_table_partition_chooser(self, partition_chooser):
        """stuff"""

        self.config['bigquery_table_partition_chooser'] = partition_chooser

