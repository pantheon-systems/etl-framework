"""parses configuration and returns useful things"""

from etl_framework.BaseConfig import BaseConfig

from gcloud.configs.mixins.gcloud import GcloudMixin
from gcloud.configs.mixins.bigquery import BigqueryMixin

class BigquerySchemaConfig(
    BaseConfig,
    BigqueryMixin,
    GcloudMixin,
):

    @property
    def bigquery_schema(self):
        """stuff"""

        return self.config['bigquery_schema']

    @property
    def bigquery_table_expiration_time(self):
        """stuff"""

        return self.config.get('bigquery_table_time_partitioning_expiration')

    @property
    def bigquery_table_time_partitioning(self):
        """stuff"""

        return self.config.get('bigquery_table_time_partitioning')

    @property
    def bigquery_table_time_partitioning_expiration(self):
        """stuff"""

        return self.config.get('bigquery_table_expiration_time')
