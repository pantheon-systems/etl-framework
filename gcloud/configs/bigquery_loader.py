"""parses configuration and returns useful things"""

from etl_framework.LoaderConfig import LoaderConfig
from etl_framework.config_mixins.BufferMixin import BufferMixin
from etl_framework.gcloud.configs.mixins.BigqueryMixin import BigqueryMixin
from etl_framework.gcloud.configs.mixins.GcloudMixin import GcloudMixin
from etl_framework.gcloud.configs.mixins.BigqueryInsertFilterMixin \
	import BigqueryInsertFilterMixin
from etl_framework.gcloud.configs.mixins.BigqueryTablePartitionMixin \
	import BigqueryTablePartitionMixin

class BigqueryLoaderConfig(
    LoaderConfig,
    BufferMixin,
    BigqueryMixin,
    BigqueryInsertFilterMixin,
    BigqueryTablePartitionMixin,
    GcloudMixin,
):
    """parses configuration files"""
