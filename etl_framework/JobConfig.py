"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from BaseConfig import BaseConfig
from config_mixins.JobMixin import JobMixin

class JobConfig(BaseConfig,
    JobMixin
):
    """parses configuration files"""

