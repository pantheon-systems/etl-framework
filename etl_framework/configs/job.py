"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.BaseConfig import BaseConfig
from etl_framework.config_mixins.JobMixin import JobMixin

class JobConfig(BaseConfig,
    JobMixin
):
    """parses configuration files"""

