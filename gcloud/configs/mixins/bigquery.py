"""parses configuration and returns useful things"""

class BigqueryMixin(object):
    """parses configuration files"""

    @property
    def bigquery_dataset_id(self):
        """stuff"""

        return self.config.get('bigquery_dataset_id')

    @property
    def bigquery_table_id(self):
        """stuff"""

        return self.config.get('bigquery_table_id')
