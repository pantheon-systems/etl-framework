"""parses configuration and returns useful things"""

from etl_framework.BaseConfig import BaseConfig

from etl_framework.gcloud.configs.mixins.GcloudMixin import GcloudMixin
from etl_framework.gcloud.configs.mixins.BigqueryMixin import BigqueryMixin
from etl_framework.gcloud.configs.mixins.BigquerySchemaMixin import BigquerySchemaMixin

class BigquerySchemaConfig(
    BaseConfig,
    BigqueryMixin,
    GcloudMixin,
    BigquerySchemaMixin,
):
    """parses configuration files"""
