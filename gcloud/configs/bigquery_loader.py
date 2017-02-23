"""parses configuration and returns useful things"""

from etl_framework.LoaderConfig import LoaderConfig
from etl_framework.config_mixins.BufferMixin import BufferMixin

from gcloud.configs.mixins.bigquery import BigqueryMixin
from gcloud.configs.mixins.gcloud import GcloudMixin
from gcloud.configs.mixins.bigquery_insert_filter import BigqueryInsertFilterMixin
from gcloud.configs.mixins.bigquery_table_partition import \
    BigqueryTablePartitionMixin

class BigqueryLoaderConfig(
    LoaderConfig,
    BufferMixin,
    BigqueryMixin,
    BigqueryInsertFilterMixin,
    BigqueryTablePartitionMixin,
    GcloudMixin,
):
    """parses configuration files"""
