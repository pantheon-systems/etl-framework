"""parses configuration and returns useful things"""

#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class BigquerySchemaMixin(object):
    """parses configuration files"""

    EXPIRATION_TIME_ATTRIBUTE = 'bigquery_table_expiration_time'
    TIME_PARTITIONING_ATTRIBUTE = 'bigquery_table_time_partitioning'
    TIME_PARTITIONING_EXPIRATION_ATTRIBUTE = 'bigquery_table_time_partitioning_expiration'
    SCHEMA_ATTRIBUTE = 'bigquery_schema'

    def get_bigquery_schema(self):
        """stuff"""

        return self.config[self.SCHEMA_ATTRIBUTE]

    @check_config_attr_default_none
    def get_bigquery_table_expiration_time(self):
        """stuff"""

        return self.config[self.EXPIRATION_TIME_ATTRIBUTE]

    @check_config_attr_default_none
    def get_bigquery_table_time_partitioning(self):
        """stuff"""

        return self.config[self.TIME_PARTITIONING_ATTRIBUTE]

    @check_config_attr_default_none
    def get_bigquery_table_time_partitioning_expiration(self):
        """stuff"""

        return self.config[self.TIME_PARTITIONING_EXPIRATION_ATTRIBUTE]
